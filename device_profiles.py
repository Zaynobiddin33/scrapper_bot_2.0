"""
Device Profiles - SeleniumBase version
Real device fingerprints for Yandex Metrika.
Provides consistent, realistic device profiles instead of random variations.
"""
import random
from typing import Dict, Any, List


# Real device profiles based on actual device specifications
DEVICE_PROFILES: List[Dict[str, Any]] = [
    # ===== WINDOWS CHROME (60% of traffic) =====
    {
        "name": "win10_chrome_intel_i5",
        "weight": 20,
        "platform": "Win32",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "navigator": {
            "appVersion": "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "platform": "Win32",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 8,
            "deviceMemory": 8,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Google Inc. (Intel)",
            "renderer": "ANGLE (Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)",
        },
        "viewport": {"width": 1366, "height": 768},
        "screen": {"width": 1366, "height": 768, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
    {
        "name": "win10_chrome_intel_i7",
        "weight": 15,
        "platform": "Win32",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "navigator": {
            "appVersion": "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "platform": "Win32",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 12,
            "deviceMemory": 16,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Google Inc. (Intel)",
            "renderer": "ANGLE (Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0)",
        },
        "viewport": {"width": 1440, "height": 900},
        "screen": {"width": 1440, "height": 900, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
    {
        "name": "win10_chrome_nvidia",
        "weight": 12,
        "platform": "Win32",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "navigator": {
            "appVersion": "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "platform": "Win32",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 16,
            "deviceMemory": 16,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Google Inc. (NVIDIA)",
            "renderer": "ANGLE (NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0)",
        },
        "viewport": {"width": 1920, "height": 1080},
        "screen": {"width": 1920, "height": 1080, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
    # ===== WINDOWS EDGE =====
    {
        "name": "win10_edge",
        "weight": 8,
        "platform": "Win32",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "navigator": {
            "appVersion": "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "platform": "Win32",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 8,
            "deviceMemory": 8,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Google Inc. (Intel)",
            "renderer": "ANGLE (Intel(R) HD Graphics 630 Direct3D11 vs_5_0 ps_5_0)",
        },
        "viewport": {"width": 1280, "height": 720},
        "screen": {"width": 1280, "height": 720, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
    # ===== WINDOWS FIREFOX =====
    {
        "name": "win10_firefox",
        "weight": 7,
        "platform": "Win32",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "navigator": {
            "appVersion": "5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
            "platform": "Win32",
            "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 8,
            "deviceMemory": 8,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Mozilla",
            "renderer": "ANGLE (Intel, Intel(R) UHD Graphics 620)",
        },
        "viewport": {"width": 1366, "height": 768},
        "screen": {"width": 1366, "height": 768, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
    # ===== MAC SAFARI =====
    {
        "name": "mac_safari_intel",
        "weight": 10,
        "platform": "MacIntel",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
        "navigator": {
            "appVersion": "5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
            "platform": "MacIntel",
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 8,
            "deviceMemory": 8,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Apple Inc.",
            "renderer": "Apple GPU",
        },
        "viewport": {"width": 1440, "height": 900},
        "screen": {"width": 1440, "height": 900, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
    # ===== MAC CHROME =====
    {
        "name": "mac_chrome_m1",
        "weight": 8,
        "platform": "MacIntel",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "navigator": {
            "appVersion": "5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "platform": "MacIntel",
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "language": "uz-UZ",
            "languages": ["uz-UZ", "uz", "ru-RU", "ru", "en-US", "en"],
            "hardwareConcurrency": 8,
            "deviceMemory": 8,
            "maxTouchPoints": 0,
            "connection": {"effectiveType": "4g", "rtt": 50, "downlink": 10, "saveData": False},
        },
        "webgl": {
            "vendor": "Apple Inc.",
            "renderer": "Apple M1",
        },
        "viewport": {"width": 1280, "height": 800},
        "screen": {"width": 1280, "height": 800, "colorDepth": 24, "pixelDepth": 24},
        "timezone": "Asia/Tashkent",
    },
]


def pick_profile() -> Dict[str, Any]:
    """
    Pick a device profile using weighted random selection.
    
    Returns:
        Device profile dictionary with all properties
    """
    # Extract weights (default to 10 if not specified)
    weights = [p.get("weight", 10) for p in DEVICE_PROFILES]
    
    # Weighted random selection
    selected = random.choices(DEVICE_PROFILES, weights=weights, k=1)[0]
    
    # Return copy to avoid mutation
    import copy
    return copy.deepcopy(selected)


def get_device_memory() -> int:
    """Get a realistic device memory value based on profile distribution."""
    return random.choices(
        [4, 6, 8, 16],
        weights=[5, 15, 60, 20],
        k=1
    )[0]


def get_hardware_concurrency() -> int:
    """Get a realistic hardware concurrency value based on profile distribution."""
    return random.choices(
        [4, 6, 8, 12, 16],
        weights=[10, 10, 50, 20, 10],
        k=1
    )[0]


def get_viewport() -> Dict[str, int]:
    """Get viewport dimensions based on profile distribution."""
    return {"width": 1366, "height": 768}


def get_language() -> str:
    """Get language based on regional distribution."""
    languages = ["uz-UZ", "ru-RU", "en-US"]
    return random.choices(languages, weights=[60, 25, 15], k=1)[0]


def get_timezone() -> str:
    """Get timezone - Uzbekistan region."""
    return "Asia/Tashkent"


# Helper to create a complete device specification
def create_device_spec() -> Dict[str, Any]:
    """
    Create a complete device specification for browser fingerprinting.
    
    Returns:
        Complete device specification dictionary
    """
    profile = pick_profile()
    
    spec = {
        "name": profile["name"],
        "platform": profile["platform"],
        "user_agent": profile["user_agent"],
        "viewport": profile["viewport"],
        "screen": profile["screen"],
        "timezone": profile["timezone"],
        "language": get_language(),
        "webgl": {
            "vendor": profile["webgl"]["vendor"],
            "renderer": profile["webgl"]["renderer"],
        },
        "hardware": {
            "concurrency": get_hardware_concurrency(),
            "memory": get_device_memory(),
        },
    }
    
    return spec
