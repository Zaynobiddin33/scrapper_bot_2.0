"""
Reliable Yandex Metrika hit verification via the CDP network layer.

Why this module exists: the previous detection injected a JS interceptor
(`window._ymHitDetected`) through the WebDriver connection and read it with
`sb.execute_script`. In SeleniumBase UC/CDP mode that interceptor does NOT
survive `activate_cdp_mode()` (it evaluates to `undefined`), and
`performance.getEntriesByType('resource')` misses the `/watch` beacon because
Metrika sends it via Image/sendBeacon. Result: 0 hits ever detected, even on
sites where the beacon genuinely fires.

This module instead subscribes to CDP `Network.requestWillBeSent` events and
watches for real requests to `mc.yandex.*/watch` — the actual Metrika hit,
captured at the network layer regardless of JS transport or page context.
Verified empirically: on apteka.uz the engine's `sb.open()` flow produces
`watch>=2` captured here.
"""

try:
    import mycdp  # SeleniumBase's vendored CDP event module
except Exception:  # pragma: no cover
    mycdp = None

import time

_WATCH_MARK = "/watch"
_MC = "mc.yandex"


class HitCapture:
    """
    Captures Metrika /watch beacons for one visit via CDP network events.

    Usage:
        cap = HitCapture(sb)
        cap.start()               # after activate_cdp_mode(prewarm)
        ...
        cap.reset()               # right before opening the target
        sb.open(target)
        ...
        if cap.wait_for_hit(timeout): ...   # or check cap.watch_count
    """

    def __init__(self, sb):
        self.sb = sb
        self._all = []      # every mc.yandex request URL
        self._watch = []    # only /watch beacons (the actual hits)
        self.active = False

    def _handler(self, evt):
        # Runs in the CDP event loop thread. list.append/len are GIL-atomic,
        # so no lock is needed for the simple counters we read on the main thread.
        try:
            req = getattr(evt, "request", None)
            url = getattr(req, "url", "") or ""
            if _MC in url:
                self._all.append(url)
                if _WATCH_MARK in url:
                    self._watch.append(url)
        except Exception:
            pass

    def start(self) -> bool:
        """Register the network handler. Safe no-op if CDP module unavailable."""
        if mycdp is None:
            return False
        try:
            self.sb.cdp.add_handler(mycdp.network.RequestWillBeSent, self._handler)
            try:
                self.sb.cdp.page.send(mycdp.network.enable())
            except Exception:
                pass  # domain is often already enabled by CDP mode
            self.active = True
            return True
        except Exception:
            self.active = False
            return False

    def reset(self):
        """Clear captured hits (call right before opening the target so the
        prewarm page's own Metrika traffic is not counted)."""
        self._all.clear()
        self._watch.clear()

    @property
    def watch_count(self) -> int:
        return len(self._watch)

    @property
    def mc_count(self) -> int:
        return len(self._all)

    def last_watch_url(self):
        return self._watch[-1] if self._watch else None

    def wait_for_hit(self, timeout: float = 25.0, stop_flag=None,
                     interval: float = 0.3) -> bool:
        """Poll until a /watch beacon is captured or timeout elapses."""
        end = time.time() + max(0.0, timeout)
        while time.time() < end:
            if self._watch:
                return True
            if stop_flag is not None:
                try:
                    if stop_flag():
                        break
                except Exception:
                    pass
            time.sleep(interval)
        return len(self._watch) > 0


# --- Target classification: which destinations can NEVER produce a Metrika hit ---
# Yandex Direct ads frequently point to Telegram bots/channels or app stores.
# Those destinations do not run a Metrika counter, so a hit is impossible and
# visiting them for a "Metrika hit" is wasted. Detected by final landing host.
NO_METRIKA_HOST_SUFFIXES = (
    "t.me",
    "telegram.me",
    "telegram.org",
    "apps.apple.com",
    "itunes.apple.com",
    "play.google.com",
    "onelink.me",
    "app.link",
    "appsflyer.com",
    "adj.st",
)


def host_has_no_metrika(host: str) -> bool:
    """True if the host is a known counter-less destination (Telegram/app store)."""
    if not host:
        return False
    h = host.lower().split(":", 1)[0]
    if h.startswith("www."):
        h = h[4:]
    for suf in NO_METRIKA_HOST_SUFFIXES:
        if h == suf or h.endswith("." + suf):
            return True
    return False
