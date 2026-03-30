"""
Human-like Action Simulator
Generates realistic mouse movements, scrolling, and interactions.
"""
import asyncio
import random
import math
from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class Point:
    x: float
    y: float


class HumanMouse:
    """
    Simulates human-like mouse movements with:
    - Bezier curves for natural paths
    - Variable speed (acceleration/deceleration)
    - Occasional overshoots and corrections
    - Random pauses (reading/thinking)
    """
    
    def __init__(self, page):
        self.page = page
        self.current_pos = Point(0, 0)
        self.viewport_width = 1920
        self.viewport_height = 1080
    
    async def update_viewport(self):
        """Get current viewport dimensions"""
        try:
            size = await self.page.evaluate("() => ({width: window.innerWidth, height: window.innerHeight})")
            self.viewport_width = size.get('width', 1920)
            self.viewport_height = size.get('height', 1080)
        except:
            pass
    
    def _bezier_curve(self, p0: Point, p1: Point, p2: Point, p3: Point, t: float) -> Point:
        """Calculate point on cubic Bezier curve at time t (0-1)"""
        u = 1 - t
        tt = t * t
        uu = u * u
        uuu = uu * u
        ttt = tt * t
        
        x = uuu * p0.x + 3 * uu * t * p1.x + 3 * u * tt * p2.x + ttt * p3.x
        y = uuu * p0.y + 3 * uu * t * p1.y + 3 * u * tt * p3.y + ttt * p3.y
        
        return Point(x, y)
    
    def _generate_control_points(self, start: Point, end: Point) -> Tuple[Point, Point]:
        """Generate control points for Bezier curve"""
        distance = math.sqrt((end.x - start.x)**2 + (end.y - start.y)**2)
        
        # Random offset based on distance
        offset = distance * 0.3
        
        # First control point (closer to start)
        cp1 = Point(
            start.x + (end.x - start.x) * 0.2 + random.uniform(-offset, offset),
            start.y + (end.y - start.y) * 0.2 + random.uniform(-offset, offset)
        )
        
        # Second control point (closer to end)
        cp2 = Point(
            start.x + (end.x - start.x) * 0.8 + random.uniform(-offset, offset),
            start.y + (end.y - start.y) * 0.8 + random.uniform(-offset, offset)
        )
        
        return cp1, cp2
    
    async def move_to(self, target: Point, duration: Optional[float] = None, 
                     overshoot: bool = False) -> None:
        """
        Move mouse to target position with human-like motion.
        
        Args:
            target: Target coordinates
            duration: Movement duration in seconds (None = auto-calculate)
            overshoot: Whether to overshoot and correct
        """
        await self.update_viewport()
        
        start = self.current_pos
        
        # Calculate duration based on distance
        distance = math.sqrt((target.x - start.x)**2 + (target.y - start.y)**2)
        if duration is None:
            # Humans move faster for short distances, slower for long
            # Speed: ~500-1500 pixels per second
            speed = random.uniform(500, 1500)
            duration = distance / speed
            duration = max(0.1, min(duration, 2.0))  # Clamp between 0.1-2s
        
        # Generate Bezier curve points
        cp1, cp2 = self._generate_control_points(start, target)
        
        # Number of steps (more steps = smoother)
        steps = max(10, int(distance / 10))
        
        # Movement with ease-in-out
        for i in range(steps + 1):
            t = i / steps
            # Ease-in-out function
            t = t * t * (3 - 2 * t)
            
            pos = self._bezier_curve(start, cp1, cp2, target, t)
            
            # Add small jitter (hand tremor)
            jitter_x = random.gauss(0, 0.5)
            jitter_y = random.gauss(0, 0.5)
            
            # Clamp to viewport
            x = max(0, min(self.viewport_width, pos.x + jitter_x))
            y = max(0, min(self.viewport_height, pos.y + jitter_y))
            
            await self.page.mouse.move(x, y)
            await asyncio.sleep(duration / steps)
        
        # Overshoot and correct (human behavior)
        if overshoot and random.random() < 0.3:
            overshoot_x = target.x + random.uniform(-10, 10)
            overshoot_y = target.y + random.uniform(-10, 10)
            await self.page.mouse.move(overshoot_x, overshoot_y)
            await asyncio.sleep(random.uniform(0.05, 0.15))
            await self.page.mouse.move(target.x, target.y)
        
        self.current_pos = target
        
        # Random pause after movement ("reading" the element)
        if random.random() < 0.4:
            await asyncio.sleep(random.uniform(0.1, 0.4))
    
    async def move_near(self, target: Point, radius: float = 50) -> None:
        """Move to a random position near target"""
        angle = random.uniform(0, 2 * math.pi)
        distance = random.uniform(0, radius)
        near_target = Point(
            target.x + distance * math.cos(angle),
            target.y + distance * math.sin(angle)
        )
        await self.move_to(near_target)
    
    async def click(self, target: Optional[Point] = None, 
                   button: str = "left", double: bool = False) -> None:
        """Human-like click (move then click with small offset)"""
        if target:
            # Move near target first
            await self.move_near(target, radius=20)
            # Then move to actual target
            await self.move_to(target, duration=random.uniform(0.1, 0.3))
        
        # Small random delay before click
        await asyncio.sleep(random.uniform(0.05, 0.15))
        
        # Perform click
        if double:
            await self.page.mouse.dblclick()
        else:
            await self.page.mouse.click()
        
        # Move slightly after click (natural)
        if random.random() < 0.5:
            new_pos = Point(
                self.current_pos.x + random.uniform(-5, 5),
                self.current_pos.y + random.uniform(-5, 5)
            )
            await self.move_to(new_pos, duration=0.1)
    
    async def random_movement(self, count: int = 5) -> None:
        """Random movements across the page"""
        await self.update_viewport()
        
        for _ in range(count):
            target = Point(
                random.uniform(50, self.viewport_width - 50),
                random.uniform(50, self.viewport_height - 50)
            )
            await self.move_to(target)
            
            # Random pause
            if random.random() < 0.3:
                await asyncio.sleep(random.uniform(0.5, 1.5))


class HumanScroller:
    """Human-like scrolling behavior"""
    
    def __init__(self, page):
        self.page = page
    
    async def scroll_down(self, amount: Optional[int] = None) -> None:
        """Scroll down with momentum"""
        if amount is None:
            amount = random.randint(200, 800)
        
        # Break into smaller chunks (humans scroll in bursts)
        chunks = random.randint(2, 5)
        chunk_size = amount // chunks
        
        for _ in range(chunks):
            await self.page.mouse.wheel(0, chunk_size)
            await asyncio.sleep(random.uniform(0.1, 0.3))
    
    async def scroll_up(self, amount: Optional[int] = None) -> None:
        """Scroll up with momentum"""
        if amount is None:
            amount = random.randint(100, 400)
        
        chunks = random.randint(1, 3)
        chunk_size = amount // chunks
        
        for _ in range(chunks):
            await self.page.mouse.wheel(0, -chunk_size)
            await asyncio.sleep(random.uniform(0.1, 0.2))
    
    async def scroll_to_element(self, selector: str) -> None:
        """Scroll to an element naturally"""
        try:
            await self.page.evaluate(f"""
                const el = document.querySelector('{selector}');
                if (el) {{
                    el.scrollIntoView({{behavior: 'smooth', block: 'center'}});
                }}
            """)
            await asyncio.sleep(random.uniform(0.5, 1.0))
        except:
            pass
    
    async def random_scroll_session(self, duration: float = 20.0) -> int:
        """
        Simulate a realistic scrolling session.
        Returns total pixels scrolled.
        """
        total_scrolled = 0
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < duration:
            action = random.choices(
                ["scroll_down", "scroll_up", "pause", "read"],
                weights=[40, 20, 25, 15]
            )[0]
            
            if action == "scroll_down":
                amount = random.randint(200, 600)
                await self.scroll_down(amount)
                total_scrolled += amount
                
            elif action == "scroll_up":
                amount = random.randint(100, 300)
                await self.scroll_up(amount)
                total_scrolled -= amount
                
            elif action == "pause":
                # Short pause
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
            else:  # read
                # Longer pause ("reading")
                await asyncio.sleep(random.uniform(2, 5))
        
        return abs(total_scrolled)


class HumanBehaviorSimulator:
    """Complete human behavior simulation"""
    
    def __init__(self, page):
        self.page = page
        self.mouse = HumanMouse(page)
        self.scroller = HumanScroller(page)
    
    async def simulate_visit(self, duration: float = 25.0) -> dict:
        """
        Simulate a complete human visit.
        
        Returns:
            dict with stats: actions, scroll_px, duration
        """
        actions = 0
        scroll_px = 0
        start_time = asyncio.get_event_loop().time()
        end_time = start_time + duration
        
        # Initial wait (page "reading")
        await asyncio.sleep(random.uniform(1, 3))
        
        while asyncio.get_event_loop().time() < end_time:
            remaining = end_time - asyncio.get_event_loop().time()
            
            if remaining < 2:
                break
            
            # Choose action
            action = random.choices(
                ["move", "scroll", "read", "random_move", "pause"],
                weights=[25, 30, 20, 15, 10]
            )[0]
            
            try:
                if action == "move":
                    # Move to random position
                    await self.mouse.update_viewport()
                    target = Point(
                        random.uniform(100, self.mouse.viewport_width - 100),
                        random.uniform(100, self.mouse.viewport_height - 100)
                    )
                    await self.mouse.move_to(target, overshoot=True)
                    actions += 1
                    
                elif action == "scroll":
                    # Scroll
                    if random.random() < 0.7:
                        amount = random.randint(200, 600)
                        await self.scroller.scroll_down(amount)
                        scroll_px += amount
                    else:
                        amount = random.randint(50, 300)
                        await self.scroller.scroll_up(amount)
                    actions += 1
                    
                elif action == "read":
                    # Pause to "read"
                    await asyncio.sleep(random.uniform(1, 4))
                    
                elif action == "random_move":
                    # Random mouse movements
                    await self.mouse.random_movement(count=random.randint(2, 5))
                    actions += 1
                    
                else:  # pause
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    
            except Exception:
                await asyncio.sleep(0.5)
        
        actual_duration = asyncio.get_event_loop().time() - start_time
        
        return {
            "actions": actions,
            "scroll_px": scroll_px,
            "duration": actual_duration
        }
