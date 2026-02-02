# Database Configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PORT = 3306
DB_PASSWORD = '123456'  # PLEASE CHANGE THIS TO YOUR ACTUAL PASSWORD
DB_NAME = 'nmpa_data'
TABLE_NAME = 'medical_device_enterprises'

# Scraper Configuration
BASE_URL = "https://www.nmpa.gov.cn/datasearch/search-result.html"
HEADLESS = False  # Set to True for production/background run
MAX_PAGES = 500   # Increased limit for full scraping
DELAY_RANGE = (2, 4) # Fast scraping!
BLOCKED_COOLDOWN = 70 # Legacy static value (will be superseded by Auto-Adaptive logic)

# Smart Adaptive Rate Limiter Settings
RL_BASE_WAIT = 2.5    # Initial base wait (Super Fast!)
RL_MIN_WAIT = 2.2     # Fastest allowed base wait (Raised to prevent 'Death Trap' blocks)
RL_MAX_WAIT = 120     # Slowest allowed base wait
RL_PENALTY_ADD = 20   # Seconds to add when blocked
RL_RECOVERY_STEP = 2  # Seconds to recover (speed up) every 10 successes
