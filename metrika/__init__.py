"""
Yandex Metrika Integration Module

This module provides tools for interacting with Yandex Metrika:
- Hit verification (ensure visits are counted)
- Fingerprint generation (realistic browser profiles)
- Tab visibility simulation (avoid bot detection)
- Stealth patches (anti-detection)
- Human actions (realistic mouse/scroll)

Usage:
    from metrika import validate_metrika_hit, MetrikaHitValidator
"""

# Only import what's needed - skip playwright-dependent files
from .yandex_metrika_hits import (
    MetrikaHitValidator,
    validate_metrika_hit,
    validate_metrika_hit_from_network,
    is_hit_valid,
)
from .fingerprint import Fingerprint, FingerprintGenerator, generate_fingerprint, generate_multiple
from .stealth import get_stealth_script
from .human_actions import HumanMouse, HumanScroller, HumanBehaviorSimulator
from .logger import BotLogger, VisitLog, get_logger

__all__ = [
    # Hit verification - DIRECT (100% accurate, no playwright needed)
    'MetrikaHitValidator',
    'validate_metrika_hit',
    'validate_metrika_hit_from_network',
    'is_hit_valid',
    
    # Fingerprint generation
    'Fingerprint',
    'FingerprintGenerator',
    'generate_fingerprint',
    'generate_multiple',
    
    # Stealth
    'get_stealth_script',
    
    # Human actions
    'HumanMouse',
    'HumanScroller', 
    'HumanBehaviorSimulator',
    
    # Logging
    'BotLogger',
    'VisitLog',
    'get_logger',
]

# Module version
__version__ = '1.2.0'
