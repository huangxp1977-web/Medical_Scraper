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
    
    # 2.1 Self-Repair Check
    # Find records that are "Broken" (Name exists but Rep/License is empty)
    broken_names = db.get_empty_records()
    print(f"[Self-Repair] Found {len(broken_names)} incomplete records to re-scrape.")
    
    # CRITICAL: If we want to re-scrape them, we must REMOVE them from the 'existing_records' set passed to scraper.
    # Otherwise, scraper will see them in memory and skip them again!
    if broken_names:
        broken_set = set(broken_names)
        # Filter out broken items from the Deduplication Set
        existing_records = {rec for rec in existing_records if rec[1] not in broken_set}
        print(f"[Self-Repair] Removed {len(broken_names)} items from Dedupe Memory to force re-scrape.")

    scraper = NMPAScraper(existing_records=existing_records)
    
    try:
        scraper.start()
        
        # --- PHASE 1: SELF-REPAIR ---
        repair_count = 0 # Initialize repair_count here
        if broken_names:
            print(f"\n=== Phase 1: Self-Repair ({len(broken_names)} items) ===")
            for name in broken_names:
                print(f">>> Repairing: {name}")
                # Search by exact name, just 1 page needed
                for batch_data in scraper.search(keyword=name, max_pages=1):
                    if batch_data:
                        count = db.save_batch(batch_data)
                        repair_count += count
                time.sleep(1.5) # Gentle pace for repair
            print(f"=== Phase 1 Complete. Repaired {repair_count} records. ===\n")

        # --- PHASE 2: BATCH SEARCH ---
        KEYWORDS = ["福建", "福州", "上海", "郑州", "河南"]
        print(f"[Configuration] Phase 2 Target Keywords: {KEYWORDS}")

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
        
        if total_saved_all == 0 and repair_count == 0:
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
