"""Random device fingerprint pool for the MTProto handshake.

Each account in the pool should present a distinct, plausible device profile.
Profiles are kept recent on purpose — a 2023 phone on an EOL OS running a
2-year-old Telegram version is a spoofing tell. The three pools here (Android
devices, iOS devices, app versions) are curated so any random combination
looks like a real user who updated their app sometime in the last ~6 months.

Browse / query directly:
    python -c "from src.devices import ANDROID_DEVICES; print(ANDROID_DEVICES)"

Generate one:
    from src.devices import random_device
    random_device()                       # weighted random across platforms
    random_device(platform="ios")         # force iOS
    random_device(lang_code="cs",         # localise per account
                  system_lang_code="cs-CZ")
"""

from __future__ import annotations

import random
from typing import Literal

Platform = Literal["android", "ios"]

# (device_model, system_version). OS version is pinned per device so we don't
# emit combinations like "Pixel 9 on Android 12" that would never ship.
ANDROID_DEVICES: list[tuple[str, str]] = [
    ("Samsung Galaxy S24", "Android 14"),
    ("Samsung Galaxy S24+", "Android 14"),
    ("Samsung Galaxy S24 Ultra", "Android 15"),
    ("Samsung Galaxy S25", "Android 15"),
    ("Samsung Galaxy S25 Ultra", "Android 15"),
    ("Samsung Galaxy A55", "Android 14"),
    ("Samsung Galaxy Z Fold 6", "Android 15"),
    ("Samsung Galaxy Z Flip 6", "Android 15"),
    ("Google Pixel 8", "Android 14"),
    ("Google Pixel 8 Pro", "Android 15"),
    ("Google Pixel 9", "Android 15"),
    ("Google Pixel 9 Pro", "Android 15"),
    ("Google Pixel 9 Pro XL", "Android 15"),
    ("Google Pixel 9a", "Android 15"),
    ("OnePlus 12", "Android 14"),
    ("OnePlus 13", "Android 15"),
    ("Xiaomi 14", "Android 14"),
    ("Xiaomi 14 Pro", "Android 14"),
    ("Xiaomi 15", "Android 15"),
    ("Nothing Phone (2a)", "Android 14"),
    ("Nothing Phone (3a)", "Android 15"),
    ("Motorola Edge 50 Pro", "Android 14"),
    ("Motorola Edge 50 Ultra", "Android 14"),
]

IOS_DEVICES: list[tuple[str, str]] = [
    ("iPhone 15", "iOS 17.6"),
    ("iPhone 15 Plus", "iOS 18.0"),
    ("iPhone 15 Pro", "iOS 18.1"),
    ("iPhone 15 Pro Max", "iOS 18.2"),
    ("iPhone 16", "iOS 18.1"),
    ("iPhone 16 Plus", "iOS 18.2"),
    ("iPhone 16 Pro", "iOS 18.2"),
    ("iPhone 16 Pro Max", "iOS 18.3"),
    ("iPhone 16e", "iOS 18.3"),
]

# Telegram mobile app versions shipped across late-2025 / early-2026. Both
# platforms ship on roughly the same cadence, so one shared pool is fine.
APP_VERSIONS: list[str] = [
    "11.4.2",
    "11.5.0",
    "11.5.3",
    "11.6.1",
    "11.6.4",
    "11.7.0",
    "11.7.4",
    "11.8.0",
    "11.8.2",
]


def random_device(
    platform: Platform | None = None,
    lang_code: str = "en",
    system_lang_code: str = "en-US",
) -> dict:
    """Return a fresh device dict ready to drop into `AccountRecord.device`.

    When `platform` is None, picks Android ~70% of the time and iOS ~30% —
    roughly the real Telegram install mix.
    """
    if platform is None:
        platform = random.choices(["android", "ios"], weights=[0.7, 0.3])[0]
    if platform == "android":
        model, system = random.choice(ANDROID_DEVICES)
    else:
        model, system = random.choice(IOS_DEVICES)
    return {
        "device_model": model,
        "system_version": system,
        "app_version": random.choice(APP_VERSIONS),
        "lang_code": lang_code,
        "system_lang_code": system_lang_code,
    }
