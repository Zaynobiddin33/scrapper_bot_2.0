#!/usr/bin/env python3
"""
Yandex Metrika Diagnostic Test - UNIVERSAL VERSION
Works with ANY website that has Yandex Metrika installed.

Usage:
    python3 test_metrika.py [URL] [COUNTER_ID]

Examples:
    python3 test_metrika.py https://avo.uz/uz 94115023
    python3 test_metrika.py https://example.com auto
    python3 test_metrika.py  # Uses defaults
"""
import asyncio
import sys
import os

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from urllib.parse import urlparse
from playwright.async_api import async_playwright
from metrika import HitVerifier, generate_fingerprint, simulate_tab_visibility, ensure_visible, get_logger


# Default configuration
DEFAULT_URL = "https://avo.uz/uz"
DEFAULT_COUNTER_ID = "auto"  # Auto-detect from page

# Get from tokens.py
try:
    from tokens import PROXY_HOST, PROXY_PORT, USERNAME, PASSWORD
except ImportError:
    PROXY_HOST = "proxy.smartproxy.net"
    PROXY_PORT = "3120"
    USERNAME = "smart-ek2xtlnzj90u_area-UZ"
    PASSWORD = "tfAfyxSYLtEItTa9"


def new_proxy():
    """Generate proxy with unique session"""
    import uuid
    sid = uuid.uuid4().hex[:12]
    return {
        "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
        "username": f"{USERNAME}_session-{sid}",
        "password": PASSWORD,
    }


def get_referer_for_domain(domain: str) -> str:
    """Generate appropriate referer based on domain TLD"""
    tld = domain.split('.')[-1].lower() if '.' in domain else 'com'
    
    referers = {
        'uz': f"https://yandex.uz/search/?text={domain}",
        'ru': f"https://yandex.ru/search/?text={domain}",
        'kz': f"https://yandex.kz/search/?text={domain}",
        'by': f"https://yandex.by/search/?text={domain}",
        'ua': f"https://yandex.ua/search/?text={domain}",
        'com': f"https://www.google.com/search?q={domain}",
        'org': f"https://www.google.com/search?q={domain}",
        'net': f"https://www.google.com/search?q={domain}",
    }
    
    return referers.get(tld, f"https://www.google.com/search?q={domain}")


STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
if (!window.chrome) window.chrome = {};
if (!window.chrome.runtime) {
    window.chrome.runtime = {
        connect: function(){},
        sendMessage: function(){},
        onMessage: {addListener: function(){}, removeListener: function(){}},
        onConnect: {addListener: function(){}, removeListener: function(){}}
    };
}
Object.defineProperty(navigator, 'languages', {
    get: () => ['uz-UZ', 'uz', 'ru-RU', 'ru', 'en-US', 'en']
});
const origGetTimezoneOffset = Date.prototype.getTimezoneOffset;
Date.prototype.getTimezoneOffset = function() { return -300; };
"""


async def test_single_visit(url: str, counter_id: str):
    """Run a single test visit and report results"""
    domain = urlparse(url).netloc.replace("www.", "")
    
    print("=" * 70)
    print("YANDEX METRIKA DIAGNOSTIC TEST (UNIVERSAL)")
    print("=" * 70)
    print(f"Target URL: {url}")
    print(f"Domain: {domain}")
    print(f"Counter ID: {counter_id if counter_id != 'auto' else 'AUTO-DETECT'}")
    print("-" * 70)
    
    # Generate fingerprint
    print("\n[1/6] Generating fingerprint...")
    fp = generate_fingerprint()
    print(f"  User-Agent: {fp.user_agent[:50]}...")
    print(f"  Viewport: {fp.viewport['width']}x{fp.viewport['height']}")
    print(f"  Hardware: {fp.hw_concurrency} cores, {fp.device_memory}GB RAM")
    print(f"  Language: {fp.language}, Timezone: {fp.timezone}")
    
    # Generate proxy
    print("\n[2/6] Configuring proxy...")
    proxy = new_proxy()
    print(f"  Server: {proxy['server']}")
    print(f"  Session ID: {proxy['username'][-12:]}")
    
    # Get referer
    referer = get_referer_for_domain(domain)
    print(f"  Referer: {referer}")
    
    async with async_playwright() as pw:
        print("\n[3/6] Launching browser...")
        browser = await pw.chromium.launch(
            headless=False,  # Headed mode for realism
            proxy=proxy,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                f"--window-size={fp.viewport['width']},{fp.viewport['height']}",
            ],
        )
        print("  Browser launched successfully")
        
        print("\n[4/6] Creating context and page...")
        context = await browser.new_context(
            viewport=fp.viewport,
            user_agent=fp.user_agent,
            locale=fp.language,
            timezone_id=fp.timezone,
            extra_http_headers={
                "Referer": referer,
                "Accept-Language": f"{fp.language}, en-US;q=0.9, en;q=0.5",
            },
        )
        page = await context.new_page()
        await page.add_init_script(STEALTH_SCRIPT)
        await page.add_init_script(f"""
            Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => {fp.hw_concurrency}}});
            Object.defineProperty(navigator, 'deviceMemory', {{get: () => {fp.device_memory}}});
            Object.defineProperty(navigator, 'platform', {{get: () => '{fp.platform}'}});
            Object.defineProperty(navigator, 'language', {{get: () => '{fp.language}'}});
            const gp = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(p) {{
                if (p === 37445) return '{fp.webgl_vendor}';
                if (p === 37446) return '{fp.webgl_renderer}';
                return gp.call(this, p);
            }};
        """)
        print("  Context and page created")
        
        # Attach hit verifier
        print("\n[5/6] Attaching hit verifier...")
        cid = counter_id if counter_id != 'auto' else None
        verifier = HitVerifier(counter_id=cid, require_hittoken=True)
        verifier.attach(page)
        print(f"  Hit verifier attached (counter_id={counter_id})")
        
        # Navigate
        print(f"\n[6/6] Navigating to {url}...")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=40000)
            await page.wait_for_load_state("load", timeout=20000)
            print("  Page loaded successfully")
        except Exception as e:
            print(f"  Navigation warning: {e}")
        
        # Wait for hit
        print("\n" + "=" * 70)
        print("WAITING FOR METRIKA HIT...")
        print("=" * 70)
        
        result = await verifier.wait_for_hit(timeout=45)
        verifier.detach(page)
        
        # Report results
        print("\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)
        
        print(f"\nWatch Request Fired: {'✅ YES' if result.watch_request_fired else '❌ NO'}")
        print(f"Response Status: {result.response_status}")
        print(f"Hit Verified: {'✅ YES' if result.hit_verified else '❌ NO'}")
        
        if result.hittoken:
            print(f"Hittoken Received: ✅ YES")
            print(f"  Token: {result.hittoken}")
        else:
            print(f"Hittoken Received: ❌ NO")
        
        if result.hidv2:
            print(f"HIDv2 Received: ✅ YES")
            print(f"  HID: {result.hidv2}")
        
        if result.bh_cookie:
            print(f"BH Cookie Set: ✅ YES")
            print(f"  Cookie: {result.bh_cookie[:50]}...")
        else:
            print(f"BH Cookie Set: ❌ NO")
        
        if result.error:
            print(f"\nError: {result.error}")
        
        # Final verdict
        print("\n" + "=" * 70)
        if result.hit_verified:
            print("✅ SUCCESS: Visit was counted by Yandex Metrika!")
            print("=" * 70)
            print("\nNext steps:")
            print(f"1. Check Metrika dashboard for counter (see URL in logs above)")
            print("2. Look for new visit in the next 10-30 minutes")
            print("3. If visible, your universal setup is working!")
        else:
            print("❌ FAILURE: Visit was NOT counted")
            print("=" * 70)
            print("\nTroubleshooting:")
            print("1. Check if proxy is working")
            print("2. Verify the site has Yandex Metrika installed")
            print("3. Check if counter ID is correct")
            print("4. Try different proxy provider")
            print("5. Check Metrika filters in dashboard")
        
        # Cleanup
        await browser.close()
        
        return result.hit_verified


if __name__ == "__main__":
    # Parse arguments
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    counter_id = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_COUNTER_ID
    
    success = asyncio.run(test_single_visit(url, counter_id))
    sys.exit(0 if success else 1)
