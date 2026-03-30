"""
Navigation Pipeline - SeleniumBase version
Simulates realistic user navigation behavior.
Implements search → click flow to match Referer header expectations.
"""
import time
import random
import re
from typing import Optional, Dict, Any


class NavigationPipeline:
    """
    Simulates realistic user navigation behavior.
    Implements search → click flow that matches Yandex Metrika expectations.
    Uses SeleniumBase SB class for navigation.
    """
    
    def __init__(self, sb, storage=None):
        """
        Initialize navigation pipeline.
        
        Args:
            sb: SeleniumBase SB instance
            storage: Optional session storage manager
        """
        self.sb = sb
        self.storage = storage
        self.results = []
    
    def search_then_click(self, query: str, target_domain: str, wait_for_results: float = 2.0) -> bool:
        """
        Navigate via Yandex search: search → click → target.
        This simulates natural user behavior and sets correct Referer.
        
        Args:
            query: Search query (domain name without TLD)
            target_domain: Domain to click on (e.g., 'booking.centrum-air.com')
            wait_for_results: Time to wait for search results
            
        Returns:
            True if navigation successful, False otherwise
        """
        try:
            print(f"[Navigation] Searching for '{query}'...")
            
            # Step 1: Navigate to Yandex search
            self.sb.open("https://yandex.uz/")
            time.sleep(random.uniform(0.5, 1.5))
            
            # Verify we're on search page
            current_url = self.sb.get_current_url()
            if "yandex.uz" not in current_url:
                print(f"[Navigation] Failed to reach Yandex search: {current_url}")
                return False
            
            # Step 2: Fill search box
            search_selectors = [
                'input[name="text"]',
                'input[type="text"]',
                'input#text',
                'input.search-input',
            ]
            
            search_found = False
            for selector in search_selectors:
                try:
                    if self.sb.is_element_present(selector, timeout=5):
                        self.sb.type(selector, query, timeout=3)
                        search_found = True
                        break
                except:
                    continue
            
            if not search_found:
                print("[Navigation] Could not find search input")
                return False
            
            time.sleep(random.uniform(0.3, 0.6))
            
            # Step 3: Submit search
            submit_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button.search-button',
            ]
            
            for selector in submit_selectors:
                try:
                    self.sb.click(selector, timeout=3)
                    break
                except:
                    continue
            
            # Wait for search results
            time.sleep(wait_for_results)
            self.sb.wait_for_ready_state_complete()
            
            # Step 4: Click on our domain result
            # Try multiple patterns to find our domain link
            domain_clean = target_domain.replace("www.", "").replace(".com", "").replace(".uz", "")
            
            # Build selector for our domain
            link_selector = f"a[href*='{domain_clean}'], a[href*='{target_domain}']"
            
            links = self.sb.find_elements(link_selector, timeout=5)
            if not links:
                print("[Navigation] Could not find domain link")
                return False
            
            # Click random link from first 5 results
            link = random.choice(links[:min(5, len(links))])
            link.click()
            
            # Wait for page load
            time.sleep(random.uniform(1.0, 2.0))
            self.sb.wait_for_ready_state_complete()
            
            # Record navigation
            if self.storage:
                self.storage.add_navigation({
                    "type": "search_click",
                    "from": "yandex_search",
                    "to": target_domain,
                })
            
            print(f"[Navigation] Successfully navigated via search → click")
            return True
            
        except Exception as e:
            print(f"[Navigation] Error during search flow: {e}")
            return False
    
    def direct_visit(self, url: str) -> bool:
        """
        Direct visit fallback (if search navigation fails).
        Still sets Referer header from Yandex search.
        
        Args:
            url: Direct URL to visit
            
        Returns:
            True if visit successful, False otherwise
        """
        try:
            print(f"[Navigation] Direct visit to {url[:50]}...")
            
            self.sb.open(url)
            time.sleep(random.uniform(0.8, 1.5))
            self.sb.wait_for_ready_state_complete()
            
            # Verify page loaded
            current_url = self.sb.get_current_url()
            title = self.sb.get_page_title()
            
            if "404" in title.lower() or "blocked" in current_url.lower():
                print(f"[Navigation] Page blocked or 404: {current_url}")
                return False
            
            if not current_url or current_url == "about:blank":
                print("[Navigation] Failed to load page")
                return False
            
            # Record navigation
            if self.storage:
                self.storage.add_navigation({
                    "type": "direct",
                    "url": url,
                })
            
            print(f"[Navigation] Direct visit successful")
            return True
            
        except Exception as e:
            print(f"[Navigation] Direct visit error: {e}")
            return False
    
    def explore_site(self, target_domain: str, min_pages: int = 2, max_pages: int = 5) -> int:
        """
        Explore site by navigating to multiple pages (simulating engaged user).
        
        Args:
            target_domain: Domain being visited
            min_pages: Minimum pages to visit
            max_pages: Maximum pages to visit
            
        Returns:
            Number of pages visited
        """
        pages_visited = 1
        num_pages = random.randint(min_pages, max_pages)
        
        # Generate plausible page paths based on domain
        domain_clean = target_domain.replace("www.", "").replace(".com", "").replace(".uz", "")
        
        page_paths = [
            f"/{domain_clean}",
            f"/about",
            f"/contact",
            f"/services",
            f"/products",
            f"/blog",
            f"/faq",
            f"/terms",
            f"/privacy",
        ]
        
        for i in range(num_pages - 1):
            if i >= len(page_paths):
                break
            
            # Pick random path
            path = random.choice(page_paths[:min(len(page_paths), 5)])
            full_url = f"https://{target_domain}{path}"
            
            try:
                print(f"[Navigation] Visiting page {i+2}: {path}")
                
                self.sb.open(full_url)
                time.sleep(random.uniform(2.0, 5.0))
                self.sb.wait_for_ready_state_complete()
                
                pages_visited += 1
                
                # Record navigation
                if self.storage:
                    self.storage.add_navigation({
                        "type": "internal",
                        "from": f"https://{target_domain}",
                        "to": full_url,
                    })
                    
            except Exception as e:
                print(f"[Navigation] Internal navigation error: {e}")
                continue
        
        print(f"[Navigation] Explored {pages_visited} pages")
        return pages_visited


def navigate_realistically(sb, target_url: str, storage=None) -> Dict[str, Any]:
    """
    Convenience function for realistic navigation.
    
    Args:
        sb: SeleniumBase SB instance
        target_url: Target URL to visit
        storage: Optional session storage
        
    Returns:
        Navigation result dictionary
    """
    pipeline = NavigationPipeline(sb, storage)
    
    # Extract domain and query for search
    from urllib.parse import urlparse
    parsed = urlparse(target_url)
    domain = parsed.netloc.replace("www.", "")
    query = domain.replace(".com", "").replace(".uz", "")
    
    result = {
        "success": False,
        "method": "unknown",
        "pages_visited": 1,
    }
    
    # Try search → click first
    success = pipeline.search_then_click(query, domain)
    
    if success:
        result["success"] = True
        result["method"] = "search_click"
        
        # Explore site after navigation
        pages = pipeline.explore_site(domain)
        result["pages_visited"] = pages
    else:
        # Fallback to direct visit
        success = pipeline.direct_visit(target_url)
        result["success"] = success
        result["method"] = "direct"
    
    return result
