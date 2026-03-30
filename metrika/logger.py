"""
Structured Logging Module for Yandex Metrika Bot

Provides organized, readable logs with color coding and structured output.
"""
import logging
import sys
from datetime import datetime
from typing import Optional
from dataclasses import dataclass


# Color codes for terminal output
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    # Standard colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    
    # Backgrounds
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'


@dataclass
class VisitLog:
    """Structured visit log entry"""
    worker_id: int
    url: str
    domain: str
    counter_id: Optional[str]
    status: str  # 'success', 'failed', 'retry'
    hit_verified: bool
    hittoken: Optional[str]
    duration: float
    actions: int
    scroll_px: int
    error: Optional[str] = None
    attempt: int = 1
    total_attempts: int = 1


class BotLogger:
    """
    Custom logger with structured output and color coding.
    """
    
    def __init__(self, name: str = "MetrikaBot"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        self.logger.handlers = []
        
        # Console handler with custom formatter
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        
        # Custom formatter
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Track stats
        self.stats = {
            'total_visits': 0,
            'successful': 0,
            'failed': 0,
            'hits_verified': 0,
            'retries': 0,
        }
    
    def _colorize(self, text: str, color: str) -> str:
        """Add color to text"""
        return f"{color}{text}{Colors.RESET}"
    
    def banner(self, text: str):
        """Display a banner"""
        width = 70
        print()
        print(self._colorize("=" * width, Colors.CYAN))
        print(self._colorize(text.center(width), Colors.BOLD + Colors.CYAN))
        print(self._colorize("=" * width, Colors.CYAN))
        print()
    
    def section(self, text: str):
        """Display a section header"""
        print()
        print(self._colorize(f"▶ {text}", Colors.BOLD + Colors.BLUE))
        print(self._colorize("─" * 70, Colors.DIM))
    
    def worker_start(self, worker_id: int, total_workers: int):
        """Log worker start"""
        msg = f"Worker {worker_id}/{total_workers} started"
        self.logger.info(self._colorize(msg, Colors.CYAN))
    
    def worker_stop(self, worker_id: int):
        """Log worker stop"""
        msg = f"Worker {worker_id} stopped"
        self.logger.info(self._colorize(msg, Colors.YELLOW))
    
    def visit_start(self, worker_id: int, url: str, attempt: int = 1, total_attempts: int = 1):
        """Log visit start"""
        domain = url.split('/')[2] if '://' in url else url
        attempt_str = f" (attempt {attempt}/{total_attempts})" if total_attempts > 1 else ""
        msg = f"[W{worker_id}] Starting visit to {domain}{attempt_str}"
        self.logger.info(msg)
    
    def visit_end(self, log_entry: VisitLog):
        """Log visit completion with structured output"""
        self.stats['total_visits'] += 1
        
        w = log_entry.worker_id
        url = log_entry.url[:50] + "..." if len(log_entry.url) > 50 else log_entry.url
        
        # Build status line
        if log_entry.status == 'success':
            self.stats['successful'] += 1
            status_icon = self._colorize("✓", Colors.GREEN + Colors.BOLD)
            status_text = self._colorize("SUCCESS", Colors.GREEN)
        else:
            self.stats['failed'] += 1
            status_icon = self._colorize("✗", Colors.RED + Colors.BOLD)
            status_text = self._colorize("FAILED", Colors.RED)
        
        # Hit verification
        if log_entry.hit_verified:
            self.stats['hits_verified'] += 1
            hit_status = self._colorize("✓ HIT VERIFIED", Colors.GREEN)
        else:
            hit_status = self._colorize("✗ No hit", Colors.YELLOW)
        
        # Print structured log
        print()
        print(f"  {status_icon} [{self._colorize(f'W{w}', Colors.BOLD)}] {status_text}")
        print(f"    URL: {self._colorize(url, Colors.DIM)}")
        print(f"    Duration: {log_entry.duration:.1f}s | Actions: {log_entry.actions} | Scroll: {log_entry.scroll_px}px")
        print(f"    Hit: {hit_status}")
        
        if log_entry.hittoken:
            token_short = log_entry.hittoken[:25] + "..."
            print(f"    Token: {self._colorize(token_short, Colors.DIM)}")
        
        if log_entry.error:
            print(f"    {self._colorize('Error:', Colors.RED)} {log_entry.error}")
        
        if log_entry.attempt < log_entry.total_attempts and log_entry.status == 'success':
            print(f"    {self._colorize('→ Succeeded after retry', Colors.GREEN)}")
    
    def retry(self, worker_id: int, delay: float, attempt: int, max_attempts: int):
        """Log retry"""
        self.stats['retries'] += 1
        msg = f"[W{worker_id}] Retrying in {delay:.1f}s ({attempt}/{max_attempts})"
        self.logger.warning(self._colorize(msg, Colors.YELLOW))
    
    def proxy_error(self, worker_id: int, error: str):
        """Log proxy error"""
        msg = f"[W{worker_id}] Proxy error: {error}"
        self.logger.error(self._colorize(msg, Colors.RED))
    
    def navigation_status(self, worker_id: int, status: int, url: str):
        """Log navigation status"""
        if status == 200:
            color = Colors.GREEN
        elif status in [301, 302, 304]:
            color = Colors.YELLOW
        else:
            color = Colors.RED
        msg = f"[W{worker_id}] HTTP {status} - {url[:40]}"
        self.logger.info(self._colorize(msg, color))
    
    def bot_detected(self, worker_id: int, url: str, status: int):
        """Log potential bot detection"""
        msg = f"[W{worker_id}] Possible bot detection (HTTP {status}): {url[:40]}"
        self.logger.warning(self._colorize(msg, Colors.BG_YELLOW + Colors.BOLD))
    
    def counter_detected(self, worker_id: int, counter_id):
        """Log counter detection"""
        cid = counter_id if counter_id else "auto-detect"
        msg = f"[W{worker_id}] Using counter ID: {cid}"
        self.logger.info(self._colorize(msg, Colors.BLUE))
    
    def stats_summary(self):
        """Print statistics summary"""
        print()
        print(self._colorize("=" * 70, Colors.CYAN))
        print(self._colorize("SESSION STATISTICS", Colors.BOLD + Colors.CYAN))
        print(self._colorize("=" * 70, Colors.CYAN))
        
        total = self.stats['total_visits']
        success = self.stats['successful']
        failed = self.stats['failed']
        hits = self.stats['hits_verified']
        retries = self.stats['retries']
        
        success_rate = (success / total * 100) if total > 0 else 0
        hit_rate = (hits / success * 100) if success > 0 else 0
        
        print(f"  Total Visits:     {total}")
        print(f"  Successful:       {self._colorize(str(success), Colors.GREEN)} ({success_rate:.1f}%)")
        print(f"  Failed:           {self._colorize(str(failed), Colors.RED if failed > 0 else Colors.DIM)}")
        print(f"  Hits Verified:    {self._colorize(str(hits), Colors.GREEN)} ({hit_rate:.1f}% of successful)")
        print(f"  Retries:          {retries}")
        print(self._colorize("=" * 70, Colors.CYAN))
        print()
    
    def progress(self, completed: int, total: int):
        """Log progress"""
        pct = (completed / total * 100) if total > 0 else 0
        bar_width = 30
        filled = int(bar_width * completed / total)
        bar = "█" * filled + "░" * (bar_width - filled)
        msg = f"Progress: [{bar}] {completed}/{total} ({pct:.1f}%)"
        self.logger.info(self._colorize(msg, Colors.CYAN))
    
    def info(self, msg: str):
        """Log info message"""
        self.logger.info(msg)
    
    def warning(self, msg: str):
        """Log warning message"""
        self.logger.warning(self._colorize(msg, Colors.YELLOW))
    
    def error(self, msg: str):
        """Log error message"""
        self.logger.error(self._colorize(msg, Colors.RED))
    
    def debug(self, msg: str):
        """Log debug message"""
        self.logger.debug(self._colorize(msg, Colors.DIM))


# Global logger instance
bot_logger = BotLogger()

# Convenience functions
def get_logger() -> BotLogger:
    """Get the global logger instance"""
    return bot_logger
