import sys
import time
import io
import json
import os
from database.storage import Storage
from engine.scraper import NMPAScraper
from engine.process_lock import ProcessLock
from config import MAX_PAGES

# Force UTF-8 for Windows Console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

CHECKPOINT_FILE = "resources/scraper_checkpoint.json"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"completed": []}
    return {"completed": []}

def save_checkpoint(checkpoint):
    try:
        os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[Warning] Failed to save checkpoint: {e}")

def main():
    # Acquire process lock to prevent multiple instances
    lock = ProcessLock()
    if not lock.acquire():
        return
    
    try:
        print("=== NMPA Medical Device Enterprise Scraper ===")
        
        # 1. Init Database
        try:
            db = Storage()
            db.init_db()
        except Exception as e:
            print(f"[Error] Database connection failed: {e}")
            print("Please check config.py and ensure server/SSH is accessible.")
            return
        
        # 2. Init Checkpoint
        checkpoint = load_checkpoint()
        completed_kw = set(checkpoint.get("completed", []))
        pending_queue = checkpoint.get("pending", []) # Discovered but not yet searched
        print(f"[Checkpoint] Loaded {len(completed_kw)} completed / {len(pending_queue)} pending.")

        # 3. Init Scraper
        existing_records = db.get_existing_records()
        print(f"[Storage] Loaded {len(existing_records)} existing records for deduplication.")
        
        # 3.1 Self-Repair Check
        broken_names = db.get_empty_records()
        if broken_names:
            print(f"[Self-Repair] Found {len(broken_names)} incomplete records to re-scrape.")
            broken_set = set(broken_names)
            existing_records = {rec for rec in existing_records if rec[1] not in broken_set}

        scraper = NMPAScraper(existing_records=existing_records)
        
        try:
            scraper.start()
            
            # --- PHASE 1: SELF-REPAIR ---
            if broken_names:
                print(f"\n=== Phase 1: Self-Repair ({len(broken_names)} items) ===")
                print(f"[Self-Repair] Repairing incomplete records (empty fields + truncated names '...')")
                repair_count = 0
                for name in broken_names:
                    print(f">>> Repairing: {name}")
                    # IMPORTANT: Delete old record first (for truncated names)
                    if name.endswith('...'):
                        deleted = db.delete_by_name(name)
                        if deleted:
                            print(f"[Repair] Deleted old truncated record: {name}")
                    
                    # Search and repair ALL records with this name (may span multiple pages)
                    # Skip dedupe to allow processing all same-name records with different license numbers
                    for batch_data, _ in scraper.search(keyword=name, max_pages=10, skip_dedupe=True):
                        if batch_data:
                            count = db.save_batch(batch_data)
                            repair_count += count
                    time.sleep(1.5)
                print(f"=== Phase 1 Complete. Repaired {repair_count} records. ===\n")

            # --- PHASE 2: BATCH SEARCH & RECURSION ---
            # 🧪 Experiment 1: Gradual recovery (test_keywords.json - 山东城市)
            # 🧪 Experiment 2: Aggressive recovery (test_keywords_aggressive.json - 江苏浙江城市)
            # 🏭 Production: city_targets.json (2800+ full coverage)
            target_file = "resources/city_targets.json"  # 🏭 正式生产模式
            # target_file = "resources/test_keywords.json"  # 渐进模式实验
            # target_file = "resources/city_targets.json"  # 生产环境
            try:
                with open(target_file, 'r', encoding='utf-8') as f:
                    raw_targets = json.load(f)
                
                # Start with static list (excluding completed)
                ALL_STATIC = []
                for item in raw_targets:
                    if isinstance(item, dict): ALL_STATIC.extend(item.get('keywords', []))
                    elif isinstance(item, str): ALL_STATIC.append(item)
                
                # Track static keywords for filtering warnings
                static_keywords = set(ALL_STATIC)
                
                # Queue = Pending (Discovered) + Static (Not yet reached)
                initial_queue = pending_queue + [k for k in ALL_STATIC if k not in completed_kw and k not in set(pending_queue)]
                print(f"[Configuration] Initial Queue Size: {len(initial_queue)}")
            except FileNotFoundError:
                print(f"[Error] Cannot find '{target_file}'!")
                print("Please ensure the keyword file exists. You can generate it using 'tools/generate_省市关键词.py'")
                return
            except Exception as e:
                print(f"[Error] Failed to load keywords: {e}")
                return

            total_saved_all = 0
            queue = initial_queue
            
            while queue:
                kw = queue.pop(0)
                if kw in completed_kw: continue
                
                print(f"\n>>> Starting search for: {kw} (Queue size: {len(queue)})")
                kw_saved = 0
                discovered_this_round = set()
                pages_processed = 0
                
                # Smart Search: Fetch data and discover new keywords
                for batch_data, new_prefixes in scraper.search(keyword=kw, max_pages=MAX_PAGES):
                    pages_processed += 1
                    if batch_data:
                        count = db.save_batch(batch_data)
                        kw_saved += count
                        total_saved_all += count
                    
                    # Harvest new prefixes
                    for p in new_prefixes:
                        if p not in completed_kw and p != kw:
                            discovered_this_round.add(p)
                
                print(f">>> Finished '{kw}'. Saved: {kw_saved} records. Digged {pages_processed} pages.")
                
                # ⚠️ 1000-PAGE LIMIT WARNING (Only for dynamically discovered keywords)
                # Static keywords (province/city names) are EXPECTED to exceed 1000 pages
                if pages_processed >= MAX_PAGES and kw not in static_keywords:
                    warning_msg = f"\n{'='*80}\n⚠️  WARNING: 1000-PAGE LIMIT HIT!\n"
                    warning_msg += f"   Keyword: '{kw}' (Dynamically Discovered)\n"
                    warning_msg += f"   Pages: {pages_processed} (reached maximum)\n"
                    warning_msg += f"   Records: {kw_saved}\n"
                    warning_msg += f"\n   This refined keyword STILL has MORE data beyond 1000 pages.\n"
                    warning_msg += f"   Suggested Actions:\n"
                    warning_msg += f"   1. Manually add year-based keywords: '{kw}2023', '{kw}2024', etc.\n"
                    warning_msg += f"   2. Or contact admin to enable auto-slicing feature.\n"
                    warning_msg += f"{'='*80}\n"
                    print(warning_msg)
                    
                    # Log to file for review
                    try:
                        os.makedirs("logs", exist_ok=True)
                        with open("logs/1000page_warnings.txt", "a", encoding="utf-8") as f:
                            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {kw} - {pages_processed} pages, {kw_saved} records\n")
                    except:
                        pass
                
                # If we discovered new keywords, add them to the FRONT of the queue (Recursive approach)
                if discovered_this_round:
                    new_list = list(discovered_this_round)
                    print(f"[Discovery] Found {len(new_list)} new prefix keywords: {new_list[:5]}...")
                    # Add to queue (unique)
                    for np in reversed(new_list): # Reversed to keep discovered order when using pop(0)
                        if np not in queue: queue.insert(0, np)

                # Save Milestone
                completed_kw.add(kw)
                checkpoint["completed"] = list(completed_kw)
                checkpoint["pending"] = queue # Save current queue state
                checkpoint["last_finished"] = kw
                checkpoint["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
                save_checkpoint(checkpoint)
                
                time.sleep(2)
                    
            print(f"\n[Success] Grand Total records saved this session: {total_saved_all}")
            
        except KeyboardInterrupt:
            print("\n[Stopped] User interrupted.")
        except Exception as e:
            print(f"\n[Error] An unexpected error occurred: {e}")
        finally:
            scraper.close()
            db.close()
            print("Done.")
    
    finally:
        lock.release()

if __name__ == "__main__":
    main()
