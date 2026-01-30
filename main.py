import sys
import time
from storage import Storage
from scraper import NMPAScraper
from config import MAX_PAGES

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
        
        # 3. Run Loop
        print(f"Starting crawl for {MAX_PAGES} pages...")
        # scraper.search is a generator yielding batches
        total_saved = 0
        for batch_data in scraper.search(max_pages=MAX_PAGES):
            if batch_data:
                count = db.save_batch(batch_data)
                total_saved += count
                
        print(f"\n[Success] Total records saved: {total_saved}")
        
        if total_saved == 0:
            print("\n[Warning] No data found. Check network or anti-scraping blocks.")
            
    except KeyboardInterrupt:
        print("\n[Stopped] User interrupted.")
    except Exception as e:
        print(f"\n[Error] An unexpected error occurred: {e}")
    finally:
        print("\n=== Debug Pause ===")
        input("Press Enter to close the browser and exit...")
        scraper.close()
        db.close()
        print("Done.")

if __name__ == "__main__":
    main()
