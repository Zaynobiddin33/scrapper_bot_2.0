"""
Tab Visibility Simulation Module

Yandex Metrika tracks tab visibility to detect bots. Real users:
- Keep tabs visible most of the time
- Occasionally switch away (blur) briefly
- Return to tab (focus)

This module simulates realistic tab visibility patterns.
"""
import asyncio
import random
from playwright.async_api import Page


class TabVisibilitySimulator:
    """
    Simulates realistic tab visibility patterns.
    
    Patterns observed in real users:
    - 70%: Tab stays visible entire session (focused reading)
    - 20%: One brief blur (1-3s) mid-session
    - 10%: Multiple blurs (checking other tabs)
    """
    
    def __init__(self, page: Page):
        self.page = page
        self._is_visible = True
        self._blur_count = 0
    
    async def simulate(self, duration: int):
        """
        Simulate tab visibility for the duration of the visit.
        
        Args:
            duration: Total visit duration in seconds
        """
        # Decide visibility pattern
        pattern = random.choices(
            ['focused', 'single_blur', 'multiple_blurs'],
            weights=[0.7, 0.2, 0.1],
            k=1
        )[0]
        
        if pattern == 'focused':
            # Tab stays visible entire time
            await self._stay_visible(duration)
        
        elif pattern == 'single_blur':
            # One brief blur mid-session
            await self._single_blur(duration)
        
        else:  # multiple_blurs
            # Several short blurs
            await self._multiple_blurs(duration)
    
    async def _stay_visible(self, duration: int):
        """Keep tab visible entire session"""
        # Just wait, tab stays visible
        await asyncio.sleep(duration)
    
    async def _single_blur(self, duration: int):
        """One blur event mid-session"""
        # Wait until 40-60% through session
        blur_time = duration * random.uniform(0.4, 0.6)
        await asyncio.sleep(blur_time)
        
        # Blur for 1-3 seconds
        await self._blur(random.uniform(1, 3))
        
        # Wait rest of session
        remaining = duration - blur_time - 3
        if remaining > 0:
            await asyncio.sleep(remaining)
    
    async def _multiple_blurs(self, duration: int):
        """Multiple short blur events"""
        num_blurs = random.randint(2, 4)
        blur_interval = duration / (num_blurs + 1)
        
        for i in range(num_blurs):
            # Wait until next blur point
            await asyncio.sleep(blur_interval * random.uniform(0.8, 1.2))
            
            # Brief blur (0.5-2s)
            await self._blur(random.uniform(0.5, 2))
        
        # Wait rest of session
        await asyncio.sleep(max(0, duration - (blur_interval * num_blurs)))
    
    async def _blur(self, duration: float):
        """Simulate tab being blurred (switched away)"""
        self._is_visible = False
        self._blur_count += 1
        
        try:
            # Override document.hidden
            await self.page.evaluate("""
                () => {
                    Object.defineProperty(document, 'hidden', {
                        get: () => true,
                        configurable: true
                    });
                    Object.defineProperty(document, 'visibilityState', {
                        get: () => 'hidden',
                        configurable: true
                    });
                    document.dispatchEvent(new Event('visibilitychange'));
                }
            """)
            
            await asyncio.sleep(duration)
            
        finally:
            # Restore visibility
            await self._focus()
    
    async def _focus(self):
        """Simulate tab being focused (switched back)"""
        self._is_visible = True
        
        try:
            await self.page.evaluate("""
                () => {
                    Object.defineProperty(document, 'hidden', {
                        get: () => false,
                        configurable: true
                    });
                    Object.defineProperty(document, 'visibilityState', {
                        get: () => 'visible',
                        configurable: true
                    });
                    window.focus();
                    document.hasFocus = () => true;
                    document.dispatchEvent(new Event('visibilitychange'));
                }
            """)
        except Exception:
            pass
    
    @property
    def stats(self) -> dict:
        """Get visibility statistics"""
        return {
            'blur_count': self._blur_count,
            'final_state': 'visible' if self._is_visible else 'hidden',
        }


async def simulate_tab_visibility(page: Page, duration: int):
    """
    Convenience function to simulate tab visibility.
    
    Args:
        page: Playwright page object
        duration: Visit duration in seconds
    """
    simulator = TabVisibilitySimulator(page)
    await simulator.simulate(duration)
    return simulator.stats


async def ensure_visible(page: Page):
    """Ensure tab is visible (call before critical Metrika events)"""
    try:
        await page.evaluate("""
            () => {
                Object.defineProperty(document, 'hidden', {
                    get: () => false,
                    configurable: true
                });
                Object.defineProperty(document, 'visibilityState', {
                    get: () => 'visible',
                    configurable: true
                });
                window.focus();
                document.hasFocus = () => true;
            }
        """)
    except Exception:
        pass
