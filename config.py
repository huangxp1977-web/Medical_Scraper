# Database Configuration
USE_SSH = True  # Set to True if using the remote server via SSH Tunnel
SSH_HOST = '47.100.80.210'
SSH_PORT = 22
SSH_USER = 'root'
SSH_PASSWORD = 'Hxp-770907' # User's SSH Password

DB_HOST = '127.0.0.1' # Tunnel will map remote 3306 to local
DB_USER = 'root'
DB_PORT = 3306
DB_PASSWORD = 'aJxD1W5FUBhW2u'  # Server DB Password
DB_NAME = 'crmeb'
TABLE_NAME = 'medical_device_enterprises'

# Scraper Configuration
# ... (rest of the file)

# Scraper Configuration
BASE_URL = "https://www.nmpa.gov.cn/datasearch/search-result.html"
HEADLESS = False  # Set to True for production/background run
MAX_PAGES = 1000   # Increased limit to 1000 pages (10,000 records) per keyword
DELAY_RANGE = (2, 4) # Fast scraping!
BLOCKED_COOLDOWN = 70 # Legacy static value (will be superseded by Auto-Adaptive logic)

# Smart Adaptive Rate Limiter Settings
RL_BASE_WAIT = 2.2    # 最快速率 (Block前多抓数据)
RL_MIN_WAIT = 2.2     # 不限速
RL_MAX_WAIT = 120     # Slowest allowed base wait
RL_PENALTY_ADD = 20   # Seconds to add when blocked
RL_RECOVERY_STEP = 2  # Seconds to recover (speed up) every 10 successes

# 🧪 EXPERIMENTAL: Aggressive Recovery Mode
# Set to True to enable fast recovery after block (3 consecutive wins -> instant reset)
# Set to False for gradual recovery (default, current behavior)
USE_AGGRESSIVE_RECOVERY = True  # ← 激进模式实验中
