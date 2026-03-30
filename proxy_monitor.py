"""
Monitor sticky proxy rotation in 5 parallel sessions.

Every cycle the script:
1. Creates N sticky proxy sessions.
2. Checks each session through an external IP endpoint.
3. Reports whether the session username or exit IP has appeared before.
4. Sleeps until the next cycle.

Usage:
    python3 proxy_monitor.py
    python3 proxy_monitor.py --interval 60 --sessions 5
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import quote

from tokens import PASSWORD, PROXY_HOST, PROXY_PORT, USERNAME


DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_SESSION_COUNT = 5
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_CHECK_URL = "https://api.ipify.org?format=json"


@dataclass(frozen=True)
class ProxySession:
    slot: int
    session_id: str
    username: str
    proxy_url: str


@dataclass
class ProxyCheckResult:
    slot: int
    session_id: str
    username: str
    ok: bool
    status_code: Optional[int]
    exit_ip: Optional[str]
    elapsed: float
    error: Optional[str] = None


def color(text: str, code: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_banner(message: str) -> None:
    width = min(100, shutil.get_terminal_size((100, 20)).columns)
    line = "=" * width
    print(color(line, "36"))
    print(color(message.center(width), "1;36"))
    print(color(line, "36"))


def make_sticky_session(slot: int) -> ProxySession:
    session_id = uuid.uuid4().hex[:8]
    username = f"{USERNAME}_session-{session_id}"
    encoded_username = quote(username, safe="")
    encoded_password = quote(PASSWORD, safe="")
    proxy_url = f"http://{encoded_username}:{encoded_password}@{PROXY_HOST}:{PROXY_PORT}"
    return ProxySession(
        slot=slot,
        session_id=session_id,
        username=username,
        proxy_url=proxy_url,
    )


def extract_ip(payload: str) -> Optional[str]:
    try:
        data = json.loads(payload)
        if isinstance(data, dict):
            for key in ("ip", "origin", "query"):
                value = data.get(key)
                if value:
                    return str(value).split(",")[0].strip()
    except json.JSONDecodeError:
        pass

    payload = payload.strip()
    if payload:
        return payload.split(",")[0].strip()
    return None


def check_proxy(session: ProxySession, check_url: str, timeout_seconds: int) -> ProxyCheckResult:
    started = time.perf_counter()
    proxy_handler = urllib.request.ProxyHandler(
        {
            "http": session.proxy_url,
            "https": session.proxy_url,
        }
    )
    opener = urllib.request.build_opener(proxy_handler)
    request = urllib.request.Request(
        check_url,
        headers={"User-Agent": "ProxySessionMonitor/1.0"},
    )

    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8", errors="replace")
            status_code = getattr(response, "status", None) or response.getcode()
            exit_ip = extract_ip(payload)
            elapsed = time.perf_counter() - started

            if status_code and 200 <= status_code < 300 and exit_ip:
                return ProxyCheckResult(
                    slot=session.slot,
                    session_id=session.session_id,
                    username=session.username,
                    ok=True,
                    status_code=status_code,
                    exit_ip=exit_ip,
                    elapsed=elapsed,
                )

            error = f"bad_response:{status_code}"
            if not exit_ip:
                error += ":missing_ip"
            return ProxyCheckResult(
                slot=session.slot,
                session_id=session.session_id,
                username=session.username,
                ok=False,
                status_code=status_code,
                exit_ip=exit_ip,
                elapsed=elapsed,
                error=error,
            )
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        return ProxyCheckResult(
            slot=session.slot,
            session_id=session.session_id,
            username=session.username,
            ok=False,
            status_code=exc.code,
            exit_ip=extract_ip(payload),
            elapsed=time.perf_counter() - started,
            error=f"http_error:{exc.code}",
        )
    except Exception as exc:
        return ProxyCheckResult(
            slot=session.slot,
            session_id=session.session_id,
            username=session.username,
            ok=False,
            status_code=None,
            exit_ip=None,
            elapsed=time.perf_counter() - started,
            error=str(exc)[:180],
        )


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def fmt_row(row: list[str]) -> str:
        return " | ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))

    separator = "-+-".join("-" * width for width in widths)
    print(fmt_row(headers))
    print(separator)
    for row in rows:
        print(fmt_row(row))


def render_cycle(
    cycle_number: int,
    results: list[ProxyCheckResult],
    seen_usernames: set[str],
    seen_exit_ips: set[str],
) -> None:
    print()
    print(color(f"[{now_str()}] Cycle {cycle_number}", "1;34"))

    headers = ["Slot", "Session", "Exit IP", "HTTP", "Time", "History", "Result"]
    rows: list[list[str]] = []

    batch_usernames: set[str] = set()
    batch_exit_ips: set[str] = set()
    ok_count = 0
    reused_ip_count = 0

    for result in results:
        session_reused_before = result.username in seen_usernames
        exit_ip_reused_before = bool(result.exit_ip and result.exit_ip in seen_exit_ips)
        batch_username_duplicate = result.username in batch_usernames
        batch_ip_duplicate = bool(result.exit_ip and result.exit_ip in batch_exit_ips)

        history_flags: list[str] = []
        if session_reused_before:
            history_flags.append("session_seen")
        if exit_ip_reused_before:
            history_flags.append("ip_seen")
        if batch_username_duplicate:
            history_flags.append("batch_session_dup")
        if batch_ip_duplicate:
            history_flags.append("batch_ip_dup")
        if not history_flags:
            history_flags.append("new")

        if result.username:
            batch_usernames.add(result.username)
        if result.exit_ip:
            batch_exit_ips.add(result.exit_ip)

        if result.ok:
            ok_count += 1
            result_label = color("OK", "1;32")
        else:
            result_label = color(f"FAIL {result.error or ''}".strip(), "1;31")

        if exit_ip_reused_before or batch_ip_duplicate:
            reused_ip_count += 1

        rows.append(
            [
                str(result.slot),
                result.session_id,
                result.exit_ip or "-",
                str(result.status_code or "-"),
                f"{result.elapsed:.2f}s",
                ", ".join(history_flags),
                result_label,
            ]
        )

    print_table(headers, rows)
    print(
        f"Summary: ok={ok_count}/{len(results)} | "
        f"reused_ip={reused_ip_count} | "
        f"known_sessions={len(seen_usernames)} | "
        f"known_ips={len(seen_exit_ips)}"
    )


def monitor_proxies(
    interval_seconds: int,
    session_count: int,
    timeout_seconds: int,
    check_url: str,
) -> None:
    seen_usernames: set[str] = set()
    seen_exit_ips: set[str] = set()
    cycle_number = 0

    print_banner("PROXY SESSION MONITOR")
    print(
        f"[{now_str()}] host={PROXY_HOST}:{PROXY_PORT} | "
        f"sessions={session_count} | interval={interval_seconds}s | url={check_url}"
    )

    while True:
        cycle_number += 1
        cycle_started = time.perf_counter()
        sessions = [make_sticky_session(slot=index + 1) for index in range(session_count)]

        results: list[ProxyCheckResult] = []
        with ThreadPoolExecutor(max_workers=session_count) as executor:
            futures = [
                executor.submit(check_proxy, session, check_url, timeout_seconds)
                for session in sessions
            ]
            for future in as_completed(futures):
                results.append(future.result())

        results.sort(key=lambda item: item.slot)
        render_cycle(cycle_number, results, seen_usernames, seen_exit_ips)

        for result in results:
            seen_usernames.add(result.username)
            if result.exit_ip:
                seen_exit_ips.add(result.exit_ip)

        elapsed = time.perf_counter() - cycle_started
        sleep_seconds = max(0.0, interval_seconds - elapsed)
        print(f"[{now_str()}] Cycle runtime: {elapsed:.2f}s | next check in {sleep_seconds:.2f}s")
        time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor proxy rotation and detect repeats.")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS, help="Seconds between cycles.")
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSION_COUNT, help="Parallel sessions per cycle.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout in seconds.")
    parser.add_argument("--url", default=DEFAULT_CHECK_URL, help="Endpoint used to verify exit IP.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        monitor_proxies(
            interval_seconds=max(1, args.interval),
            session_count=max(1, args.sessions),
            timeout_seconds=max(1, args.timeout),
            check_url=args.url,
        )
    except KeyboardInterrupt:
        print(color("\nStopped by user.", "1;33"))


if __name__ == "__main__":
    main()
