"""
cookie_store.py — Persistent Yandex/Metrika cookie cache.

Goals:
  - Keep first-party Metrika cookies keyed by the final landing host.
  - Keep Yandex-global cookies in a separate pool reused across sites.
  - Replay cookies at the right stage of the visit flow.
  - Stay safe under multi-process workers by using a lock file + atomic writes.
"""
from __future__ import annotations

import json
import os
import random
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback below
    fcntl = None

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX uses fcntl
    msvcrt = None

_STORE_PATH = Path(__file__).parent / ".ym_cookie_store.json"
_LOCK_PATH = _STORE_PATH.with_suffix(_STORE_PATH.suffix + ".lock")
_STORE_VERSION = 2

RETURNING_VISITOR_RATIO = 0.38
_MAX_GLOBAL_SESSIONS = 20
_MAX_SITE_SESSIONS_PER_HOST = 20
_MAX_TARGET_MAPPINGS = 500

_SITE_COOKIE_PREFIX = "_ym_"
_GLOBAL_COOKIE_NAMES = {"yabs-sid", "yandexuid", "_yasc", "bh"}
_COOKIE_IDENTITY_PRIORITY = {
    "site": ("_ym_uid", "_ym_d"),
    "global": ("yandexuid", "yabs-sid", "bh"),
}


def _empty_store() -> dict:
    return {
        "version": _STORE_VERSION,
        "yandex_global_sessions": [],
        "site_sessions": {},
        "target_mappings": {},
    }


def _normalize_host(value: str) -> str:
    value = (value or "").strip().lower()
    if not value:
        return ""
    if "://" in value:
        value = urlparse(value).netloc
    if "@" in value:
        value = value.rsplit("@", 1)[-1]
    value = value.split(":", 1)[0].strip(".")
    if value.startswith("www."):
        value = value[4:]
    return value


def _normalize_cookie_record(cookie: dict) -> dict | None:
    if not isinstance(cookie, dict):
        return None

    name = str(cookie.get("name", "")).strip()
    value = str(cookie.get("value", ""))
    domain = str(cookie.get("domain", "")).strip().lower()
    path = str(cookie.get("path", "/") or "/")
    if not name or not domain:
        return None

    result = {
        "name": name,
        "value": value,
        "domain": domain,
        "path": path,
        "secure": bool(cookie.get("secure", False)),
        "httpOnly": bool(cookie.get("httpOnly", False)),
    }

    expires = cookie.get("expires")
    if isinstance(expires, (int, float)) and expires > 0:
        result["expires"] = expires

    same_site = cookie.get("sameSite")
    if same_site in {"Strict", "Lax", "None"}:
        result["sameSite"] = same_site

    return result


def _is_site_cookie(name: str) -> bool:
    return name.startswith(_SITE_COOKIE_PREFIX)


def _is_global_cookie(name: str) -> bool:
    return name in _GLOBAL_COOKIE_NAMES


def _domain_matches_host(cookie_domain: str, host: str) -> bool:
    cookie_host = _normalize_host(cookie_domain)
    host = _normalize_host(host)
    if not cookie_host or not host:
        return False
    return host == cookie_host or host.endswith(f".{cookie_host}")


def _normalize_session(session: list[dict]) -> list[dict]:
    if not isinstance(session, list):
        return []
    cookies = []
    for cookie in session:
        normalized = _normalize_cookie_record(cookie)
        if normalized:
            cookies.append(normalized)
    return cookies


def _session_fingerprint(cookies: list[dict], bucket_type: str) -> str:
    for name in _COOKIE_IDENTITY_PRIORITY[bucket_type]:
        for cookie in cookies:
            if cookie.get("name") == name:
                return f"{name}:{cookie.get('domain', '')}:{cookie.get('value', '')}"

    stable = sorted(
        (cookie.get("name", ""), cookie.get("domain", ""), cookie.get("value", ""))
        for cookie in cookies
    )
    return json.dumps(stable, ensure_ascii=False, separators=(",", ":"))


def _append_session(bucket: list[list[dict]], cookies: list[dict], bucket_type: str, max_size: int) -> None:
    if not cookies:
        return

    fingerprint = _session_fingerprint(cookies, bucket_type)
    for existing in bucket:
        if _session_fingerprint(existing, bucket_type) == fingerprint:
            return

    bucket.append(cookies)
    if len(bucket) > max_size:
        del bucket[:-max_size]


def _normalize_v2_store(raw: dict) -> dict:
    store = _empty_store()

    global_sessions = raw.get("yandex_global_sessions", [])
    if isinstance(global_sessions, list):
        for session in global_sessions:
            normalized = _normalize_session(session)
            if normalized:
                _append_session(
                    store["yandex_global_sessions"],
                    normalized,
                    "global",
                    _MAX_GLOBAL_SESSIONS,
                )

    site_sessions = raw.get("site_sessions", {})
    if isinstance(site_sessions, dict):
        for host, sessions in site_sessions.items():
            normalized_host = _normalize_host(host)
            if not normalized_host or not isinstance(sessions, list):
                continue
            bucket = store["site_sessions"].setdefault(normalized_host, [])
            for session in sessions:
                normalized = _normalize_session(session)
                if normalized:
                    _append_session(
                        bucket,
                        normalized,
                        "site",
                        _MAX_SITE_SESSIONS_PER_HOST,
                    )

    target_mappings = raw.get("target_mappings", {})
    if isinstance(target_mappings, dict):
        for target_url, host in target_mappings.items():
            normalized_host = _normalize_host(host)
            if isinstance(target_url, str) and target_url and normalized_host:
                store["target_mappings"][target_url] = normalized_host

    return store


def _migrate_legacy_store(raw: dict) -> dict:
    store = _empty_store()
    if not isinstance(raw, dict):
        return store

    for _, sessions in raw.items():
        if not isinstance(sessions, list):
            continue

        for session in sessions:
            normalized_session = _normalize_session(session)
            if not normalized_session:
                continue

            global_cookies: list[dict] = []
            site_groups: dict[str, list[dict]] = {}

            for cookie in normalized_session:
                name = cookie.get("name", "")
                if _is_site_cookie(name):
                    host = _normalize_host(cookie.get("domain", ""))
                    if host:
                        site_groups.setdefault(host, []).append(cookie)
                elif _is_global_cookie(name):
                    global_cookies.append(cookie)

            if global_cookies:
                _append_session(
                    store["yandex_global_sessions"],
                    global_cookies,
                    "global",
                    _MAX_GLOBAL_SESSIONS,
                )

            for host, cookies in site_groups.items():
                bucket = store["site_sessions"].setdefault(host, [])
                _append_session(
                    bucket,
                    cookies,
                    "site",
                    _MAX_SITE_SESSIONS_PER_HOST,
                )

    return store


def _normalize_store(raw: dict) -> dict:
    if isinstance(raw, dict) and raw.get("version") == _STORE_VERSION:
        return _normalize_v2_store(raw)
    return _migrate_legacy_store(raw)


def _read_store_unlocked() -> dict:
    try:
        if not _STORE_PATH.exists():
            return _empty_store()
        return _normalize_store(json.loads(_STORE_PATH.read_text(encoding="utf-8")))
    except Exception:
        return _empty_store()


def _write_store_unlocked(data: dict) -> None:
    payload = json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
    tmp_path = _STORE_PATH.with_name(f"{_STORE_PATH.name}.tmp.{os.getpid()}")
    try:
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, _STORE_PATH)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _acquire_lock(lock_file) -> None:
    if fcntl is not None:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        return

    if msvcrt is not None:  # pragma: no cover - Windows fallback
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)


def _release_lock(lock_file) -> None:
    if fcntl is not None:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        return

    if msvcrt is not None:  # pragma: no cover - Windows fallback
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)


@contextmanager
def _store_lock() -> Iterator[None]:
    _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK_PATH.open("a+b") as lock_file:
        _acquire_lock(lock_file)
        try:
            yield
        finally:
            _release_lock(lock_file)


def _read_store() -> dict:
    with _store_lock():
        return _read_store_unlocked()


def _update_store(mutator) -> None:
    with _store_lock():
        data = _read_store_unlocked()
        mutator(data)
        _write_store_unlocked(data)


def _pick_session(sessions: list[list[dict]]) -> list[dict]:
    if not sessions:
        return []
    return list(random.choice(sessions))


def _extract_cookie_sets(raw_cookies: list[dict], final_host: str) -> tuple[list[dict], list[dict]]:
    site_cookies: list[dict] = []
    global_cookies: list[dict] = []

    for cookie in raw_cookies:
        normalized = _normalize_cookie_record(cookie)
        if not normalized:
            continue

        name = normalized["name"]
        if _is_site_cookie(name):
            if _domain_matches_host(normalized.get("domain", ""), final_host):
                site_cookies.append(normalized)
        elif _is_global_cookie(name):
            global_cookies.append(normalized)

    return site_cookies, global_cookies


def get_cookie_replay_plan(target_url: str) -> dict:
    """
    Build one cookie replay plan for a visit.

    The random "returning visitor" decision is made once here so the global and
    site-level cookies stay internally consistent for that visit.
    """
    plan = {
        "is_returning": False,
        "global_cookies": [],
        "site_cookies": [],
        "predicted_final_host": None,
    }

    if random.random() > RETURNING_VISITOR_RATIO:
        return plan

    data = _read_store()
    target_host = _normalize_host(target_url)
    predicted_final_host = data["target_mappings"].get(target_url)

    if not predicted_final_host and target_host in data["site_sessions"]:
        predicted_final_host = target_host

    global_cookies = _pick_session(data["yandex_global_sessions"])
    site_cookies = _pick_session(data["site_sessions"].get(predicted_final_host, []))

    if not global_cookies and not site_cookies:
        return plan

    plan["is_returning"] = True
    plan["global_cookies"] = global_cookies
    plan["site_cookies"] = site_cookies
    plan["predicted_final_host"] = predicted_final_host
    return plan


def save_cookies_from_browser(target_url: str, final_url: str, raw_cookies: list[dict]) -> None:
    """
    Persist cookie material from a completed visit.

    - First-party `_ym_*` cookies are keyed by the final landing host.
    - Yandex-global cookies are stored in a shared cross-site pool.
    - The exact target URL is mapped to the observed final host so future visits
      to the same short-link can pre-inject site cookies before navigation.
    """
    final_host = _normalize_host(final_url)
    if not final_host:
        return

    site_cookies, global_cookies = _extract_cookie_sets(raw_cookies, final_host)

    def _mutate(data: dict) -> None:
        if global_cookies:
            _append_session(
                data["yandex_global_sessions"],
                global_cookies,
                "global",
                _MAX_GLOBAL_SESSIONS,
            )

        if site_cookies:
            bucket = data["site_sessions"].setdefault(final_host, [])
            _append_session(
                bucket,
                site_cookies,
                "site",
                _MAX_SITE_SESSIONS_PER_HOST,
            )

        if target_url:
            data["target_mappings"][target_url] = final_host
            while len(data["target_mappings"]) > _MAX_TARGET_MAPPINGS:
                oldest_key = next(iter(data["target_mappings"]))
                del data["target_mappings"][oldest_key]

    _update_store(_mutate)
