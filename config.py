# Database Configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PORT = 3306
DB_PASSWORD = '123456'  # PLEASE CHANGE THIS TO YOUR ACTUAL PASSWORD
DB_NAME = 'nmpa_data'
TABLE_NAME = 'medical_device_enterprises'

# Scraper Configuration
BASE_URL = "https://www.nmpa.gov.cn/datasearch/#category=ylqx"
HEADLESS = False  # Set to True for production/background run
MAX_PAGES = 5     # Default limit for testing
DELAY_RANGE = (3, 6) # Random delay between requests
