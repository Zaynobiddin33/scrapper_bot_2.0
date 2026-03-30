"""
Session Manager - SeleniumBase version
Manages session state across multiple page loads for Yandex Metrika.
"""
import os
import json
import uuid
import shutil
from typing import Dict, Any, Optional, List


class SessionManager:
    """
    Manages session state across multiple page loads for Yandex Metrika.
    Ensures continuity by tracking hittoken, hidv2, browser-info.pv across pages.
    """
    
    def __init__(self, visit_id: int, profile_dir: str):
        """
        Initialize session manager for a visit.
        
        Args:
            visit_id: Unique visit identifier
            profile_dir: Directory to store session state (SeleniumBase profile)
        """
        self.visit_id = visit_id
        self.session_id = uuid.uuid4().hex[:12]
        self.storage_dir = os.path.join(profile_dir, f"session_{self.session_id}")
        
        # Create storage directory
        os.makedirs(self.storage_dir, exist_ok=True)
        
        # State persistence path
        self.state_path = os.path.join(self.storage_dir, "state.json")
        
        # Initialize state
        self.state = {
            "visit_id": visit_id,
            "session_id": self.session_id,
            "profile_dir": profile_dir,
            "storage": {},
            "hits": [],
            "browser_info": {
                "pv": 0,  # Page view counter
                "vf": None,  # Visitor fingerprint
                "fu": None,  # First visit timestamp
            },
            "metrika_data": {
                "hittoken": None,
                "hidv2": None,
                "bh_cookie": None,
                "last_hit_time": None,
            },
            "navigation_history": [],
            "created_at": None,
            "completed_at": None,
        }
        
        # Load existing state if present (restart scenario)
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, 'r') as f:
                    loaded = json.load(f)
                    self.state.update(loaded)
            except Exception as e:
                print(f"[SessionManager] Error loading state: {e}")
                # Start fresh if corrupted
                self._save()
        else:
            self._save()
        
        # Track if session has been validated
        self.validated = False
        self.validation_errors = []
    
    def _save(self):
        """Save current state to disk."""
        try:
            with open(self.state_path, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception as e:
            print(f"[SessionManager] Error saving state: {e}")
    
    def start(self):
        """Mark session as started."""
        if not self.state["created_at"]:
            self.state["created_at"] = str(uuid.uuid4())[:8]
            self._save()
    
    def add_hit(self, hit: Dict[str, Any]) -> bool:
        """
        Record a Metrika hit for continuity tracking.
        
        Args:
            hit: Hit data including URL, response, headers
            
        Returns:
            True if hit recorded successfully
        """
        hit_id = str(uuid.uuid4())[:8]
        hit_record = {
            "id": hit_id,
            "url": hit.get("url", ""),
            "status": hit.get("status", 0),
            "response_json": hit.get("response_json", {}),
            "response_headers": hit.get("response_headers", {}),
            "timestamp": str(uuid.uuid4())[:8],
            "validated": hit.get("validated", False),
            "errors": hit.get("errors", []),
        }
        
        # Extract key Metrika data
        settings = hit.get("response_json", {}).get("settings", {})
        
        # Update browser-info
        browser_info = settings.get("browser-info", "")
        if browser_info:
            self.state["browser_info"]["vf"] = browser_info
        
        # Update Metrika data
        if settings.get("hittoken"):
            self.state["metrika_data"]["hittoken"] = settings["hittoken"]
        
        if settings.get("hidv2"):
            self.state["metrika_data"]["hidv2"] = settings["hidv2"]
        
        # Extract bh cookie
        set_cookie = hit.get("response_headers", {}).get("set-cookie", "")
        if "bh=" in set_cookie:
            import re
            match = re.search(r'bh=([^;]+)', set_cookie)
            if match:
                self.state["metrika_data"]["bh_cookie"] = match.group(1)
        
        # Extract pv counter
        if "pv:" in browser_info:
            import re
            match = re.search(r'pv:(\d+)', browser_info)
            if match:
                self.state["browser_info"]["pv"] = int(match.group(1))
        
        # Record hit
        self.state["hits"].append(hit_record)
        self._save()
        
        return True
    
    def get_last_hit(self) -> Optional[Dict[str, Any]]:
        """Get the last recorded hit for continuity check."""
        if self.state["hits"]:
            return self.state["hits"][-1]
        return None
    
    def get_hittoken(self) -> Optional[str]:
        """Get current hittoken for continuity verification."""
        return self.state["metrika_data"]["hittoken"]
    
    def get_hidv2(self) -> Optional[str]:
        """Get current hidv2 for visitor ID persistence."""
        return self.state["metrika_data"]["hidv2"]
    
    def get_bh_cookie(self) -> Optional[str]:
        """Get bh cookie for session tracking."""
        return self.state["metrika_data"]["bh_cookie"]
    
    def get_pv(self) -> int:
        """Get current page view counter."""
        return self.state["browser_info"]["pv"]
    
    def increment_pv(self) -> int:
        """Increment page view counter and return new value."""
        self.state["browser_info"]["pv"] = self.state["browser_info"]["pv"] + 1
        self._save()
        return self.state["browser_info"]["pv"]
    
    def add_navigation(self, navigation: Dict[str, Any]):
        """Record a navigation event."""
        nav_record = {
            "type": navigation.get("type", "unknown"),
            "from": navigation.get("from", ""),
            "to": navigation.get("to", ""),
            "timestamp": str(uuid.uuid4())[:8],
        }
        self.state["navigation_history"].append(nav_record)
        self._save()
    
    def validate_hit(self, hit: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a hit against session continuity.
        
        Args:
            hit: Hit data to validate
            
        Returns:
            Validation result with details
        """
        result = {
            "valid": True,
            "errors": [],
            "warnings": [],
        }
        
        # Get previous hit for continuity check
        last_hit = self.get_last_hit()
        
        # Check hittoken consistency
        if last_hit:
            last_hittoken = last_hit["response_json"].get("settings", {}).get("hittoken")
            current_hittoken = hit.get("response_json", {}).get("settings", {}).get("hittoken")
            
            if last_hittoken and current_hittoken:
                if last_hittoken != current_hittoken:
                    result["errors"].append("hittoken_changed")
                    result["valid"] = False
        
        # Check hidv2 consistency
        if last_hit:
            last_hidv2 = last_hit["response_json"].get("settings", {}).get("hidv2")
            current_hidv2 = hit.get("response_json", {}).get("settings", {}).get("hidv2")
            
            if last_hidv2 and current_hidv2:
                if last_hidv2 != current_hidv2:
                    result["warnings"].append("hidv2_changed")
        
        # Check bh cookie consistency
        if last_hit:
            last_bh = last_hit["response_headers"].get("set-cookie", "").find("bh=")
            current_bh = hit.get("response_headers", {}).get("set-cookie", "").find("bh=")
            
            if last_bh >= 0 and current_bh >= 0:
                # Both have bh cookie, this is good
                pass
            elif current_bh < 0:
                result["errors"].append("missing_bh_cookie")
                result["valid"] = False
        
        # Check browser-info for pv
        browser_info = hit.get("response_json", {}).get("settings", {}).get("browser-info", "")
        if "pv:" not in browser_info:
            result["errors"].append("missing_pv_in_browser_info")
            result["valid"] = False
        
        # Record validation
        if result["valid"]:
            self.state["browser_info"]["pv"] = self.state["browser_info"]["pv"] + 1
            self._save()
        
        return result
    
    def cleanup(self) -> bool:
        """
        Cleanup session data after completion.
        
        Returns:
            True if cleanup successful
        """
        try:
            self.state["completed_at"] = str(uuid.uuid4())[:8]
            self._save()
            
            # Keep state file for debugging
            return True
        except Exception as e:
            print(f"[SessionManager] Error during cleanup: {e}")
            return False
    
    def get_state(self) -> Dict[str, Any]:
        """Get current session state (for debugging)."""
        return self.state.copy()
    
    def is_validated(self) -> bool:
        """Check if session has been validated."""
        return self.validated
    
    def mark_validated(self):
        """Mark session as validated."""
        self.validated = True
    
    def add_error(self, error: str):
        """Record an error during validation."""
        self.validation_errors.append(error)


def create_session_manager(visit_id: int, profile_dir: str) -> SessionManager:
    """Create a new session manager instance."""
    return SessionManager(visit_id, profile_dir)
