import sys
import time
import io
from storage import Storage
from scraper import NMPAScraper
from config import MAX_PAGES

# Force UTF-8 for Windows Console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def main():
    print("=== NMPA Medical Device Enterprise Scraper ===")
    
    # 1. Init Database
    try:
        db = Storage()
        db.init_db()
    except Exception as e:
        print(f"[Error] Database connection failed: {e}")
        print("Please check config.py and ensure MySQL is running.")
        return

    # 2. Init Scraper
    # Fetch existing records for deduplication check
    existing_records = db.get_existing_records()
    print(f"[Storage] Loaded {len(existing_records)} existing records for deduplication.")
    
    scraper = NMPAScraper(existing_records=existing_records)
    
    try:
        scraper.start()
        
        
        # 3. Keyword Loop
        KEYWORDS = ["医美"] # Target Keyword: "Medical Beauty"
        print(f"[Configuration] Target Keywords: {KEYWORDS}")

        total_saved_all = 0
        
        for kw in KEYWORDS:
            print(f"\n>>> Starting search for keyword: {kw}")
            # scraper.search is a generator yielding batches
            kw_saved = 0
            
            # Note: We pass the explicit keyword here
            for batch_data in scraper.search(keyword=kw, max_pages=MAX_PAGES):
                if batch_data:
                    count = db.save_batch(batch_data)
                    kw_saved += count
                    total_saved_all += count
            
            print(f">>> Finished keyword '{kw}'. Saved: {kw_saved} records.")
            time.sleep(2) # Break between keywords
                
        print(f"\n[Success] Grand Total records saved: {total_saved_all}")
        
        if total_saved_all == 0:
            print("\n[Warning] No data found for ANY keywords. Check network or anti-scraping blocks.")
            
    except KeyboardInterrupt:
        print("\n[Stopped] User interrupted.")

    except Exception as e:
        print(f"\n[Error] An unexpected error occurred: {e}")
    finally:
        print("\n=== Debug Pause ===")
        # input("Press Enter to close the browser and exit...")
        scraper.close()
        db.close()
        print("Done.")

if __name__ == "__main__":
    main()
