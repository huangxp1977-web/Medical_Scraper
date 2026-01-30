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
BLOCKED_COOLDOWN = 70 # Wait time if blocked (seconds) - Increased as per user request
