"""
Yandex Metrika Hit Verification Module

This module intercepts and validates Metrika tracking requests to ensure
visits are actually counted by Yandex servers.

Key validation points:
1. /watch/{counter_id} request fires
2. Response status = 200 OK
3. Response JSON contains hittoken
4. Set-Cookie: bh= header present
"""
import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from playwright.async_api import Page, Response


@dataclass
class HitResult:
    """Result of hit verification"""
    success: bool = False
    watch_request_fired: bool = False
    response_status: int = 0
    hittoken: Optional[str] = None
    hidv2: Optional[str] = None
    bh_cookie: Optional[str] = None
    counter_id: Optional[int] = None
    page_url: Optional[str] = None
    error: Optional[str] = None
    response_json: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def hit_verified(self) -> bool:
        """Hit was confirmed counted by Yandex"""
        return (
            self.success and
            self.watch_request_fired and
            self.response_status == 200 and
            self.hittoken is not None
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'hit_verified': self.hit_verified,
            'watch_request_fired': self.watch_request_fired,
            'response_status': self.response_status,
            'hittoken_present': self.hittoken is not None,
            'bh_cookie_present': self.bh_cookie is not None,
            'counter_id': self.counter_id,
            'error': self.error,
        }


class HitVerifier:
    """
    Intercepts and validates Yandex Metrika tracking requests.
    
    Usage:
        verifier = HitVerifier(counter_id=94115023)
        verifier.attach(page)
        await page.goto(url)
        result = await verifier.wait_for_hit(timeout=30)
    """
    
    def __init__(self, counter_id, require_hittoken: bool = True):
        """
        Initialize hit verifier.
        
        Args:
            counter_id: Counter ID (int) or 'auto' to detect from request URL
            require_hittoken: Whether to require hittoken in response
        """
        if counter_id == 'auto':
            self.counter_id = None  # Will match any /watch/{id} pattern
        else:
            self.counter_id = int(counter_id) if counter_id else None
        self.require_hittoken = require_hittoken
        self._result = HitResult(counter_id=counter_id)
        self._hit_event = asyncio.Event()
        self._response_handler = None
    
    def attach(self, page: Page):
        """Attach verifier to a page's response events"""
        self._result = HitResult(counter_id=self.counter_id)
        self._hit_event = asyncio.Event()
        
        # Pattern to match: /watch/{counter_id} or /watch/{counter_id}/{hit_num}
        # If counter_id is None (auto), match any /watch/ followed by digits
        if self.counter_id:
            watch_pattern = re.compile(rf'mc\.yandex\.ru/watch/{self.counter_id}(?:/\d+)?')
        else:
            watch_pattern = re.compile(r'mc\.yandex\.ru/watch/\d+(?:/\d+)?')
        
        async def on_response(response: Response):
            url = response.url
            if not watch_pattern.search(url):
                return
            
            # This is a Metrika watch request
            self._result.watch_request_fired = True
            self._result.page_url = url
            
            try:
                self._result.response_status = response.status
                
                # Check for bh cookie in Set-Cookie header
                headers = response.headers
                set_cookie = headers.get('set-cookie', '')
                if 'bh=' in set_cookie:
                    # Extract bh cookie value
                    bh_match = re.search(r'bh=([^;]+)', set_cookie)
                    if bh_match:
                        self._result.bh_cookie = bh_match.group(1)
                
                # Try to parse response JSON
                try:
                    content_type = headers.get('content-type', '')
                    if 'application/json' in content_type:
                        text = await response.text()
                        self._result.response_json = json.loads(text)
                        
                        # Extract hittoken
                        settings = self._result.response_json.get('settings', {})
                        self._result.hittoken = settings.get('hittoken')
                        self._result.hidv2 = settings.get('hidv2')
                        
                except Exception as e:
                    # Response might not be JSON (e.g., redirect)
                    pass
                
            except Exception as e:
                self._result.error = f"Error processing response: {e}"
            
            # Signal that hit was received
            self._hit_event.set()
        
        self._response_handler = on_response
        page.on('response', on_response)
    
    async def wait_for_hit(self, timeout: float = 30.0) -> HitResult:
        """
        Wait for Metrika hit to be sent and verified.
        
        Args:
            timeout: Maximum seconds to wait for hit
        
        Returns:
            HitResult with verification details
        """
        try:
            await asyncio.wait_for(self._hit_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self._result.error = f"Timeout waiting for hit (>{timeout}s)"
            return self._result
        
        # Validate the hit
        if not self._result.watch_request_fired:
            self._result.error = "Watch request did not fire"
            return self._result
        
        if self._result.response_status != 200:
            self._result.error = f"Non-200 response: {self._result.response_status}"
            return self._result
        
        if self.require_hittoken and not self._result.hittoken:
            self._result.error = "No hittoken in response"
            return self._result
        
        # All validations passed
        self._result.success = True
        return self._result
    
    def detach(self, page: Page):
        """Remove event listener from page"""
        if self._response_handler:
            page.remove_listener('response', self._response_handler)
            self._response_handler = None


async def verify_metrika_hit(
    page: Page,
    counter_id: int,
    timeout: float = 30.0,
    require_hittoken: bool = True
) -> HitResult:
    """
    Convenience function to verify a Metrika hit.
    
    Usage:
        result = await verify_metrika_hit(page, counter_id=94115023)
        if result.hit_verified:
            print("Visit counted!")
    """
    verifier = HitVerifier(counter_id=counter_id, require_hittoken=require_hittoken)
    verifier.attach(page)
    
    result = await verifier.wait_for_hit(timeout=timeout)
    verifier.detach(page)
    
    return result
