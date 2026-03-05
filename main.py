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
        completed_list = checkpoint.get("completed", [])
        completed_kw = set(completed_list)
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
                
                # Queue = Current (if any) + Pending (Discovered) + Static (Not yet reached)
                # 🔧 如果有正在处理的关键词（中断恢复），放到队首
                current_kw = checkpoint.get("current")
                initial_queue = pending_queue + [k for k in ALL_STATIC if k not in completed_kw and k not in set(pending_queue)]
                
                # 🔧 FIX: 如果current存在，先从队列中移除它，然后放到队首
                if current_kw and current_kw not in completed_kw:
                    # 从队列中移除（如果存在）
                    if current_kw in initial_queue:
                        initial_queue.remove(current_kw)
                    # 放到队首
                    initial_queue.insert(0, current_kw)
                    print(f"[Checkpoint] Resuming interrupted keyword: '{current_kw}'")
                    
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
                
                # 🔧 保存当前正在处理的关键词，防止中断丢失
                checkpoint["current"] = kw
                checkpoint["pending"] = queue
                checkpoint["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
                save_checkpoint(checkpoint)
                
                print(f"\n>>> Starting search for: {kw} (Queue size: {len(queue)})")
                kw_saved = 0
                discovered_this_round = set()
                pages_processed = 0
                need_year_split = False  # 🔧 年份拆分标志
                
                # Smart Search: Fetch data and discover new keywords
                for batch_data, new_prefixes in scraper.search(keyword=kw, max_pages=MAX_PAGES):
                    pages_processed += 1
                    if batch_data:
                        count = db.save_batch(batch_data)
                        kw_saved += count
                        total_saved_all += count
                    
                    # Harvest new prefixes
                    for p in new_prefixes:
                        if p != kw and p not in queue and p not in completed_kw:
                            discovered_this_round.add(p)
                            queue.insert(0, p)
                            print(f"[📍 Discovery] New keyword: '{p}'")
                    
                    # 🔧 第一页后检查：动态关键词是否需要按年份拆分
                    if pages_processed == 1 and kw not in static_keywords:
                        site_total = getattr(scraper, 'last_total_pages', 0)
                        if site_total > 1000:
                            print(f"\n[🔀 AutoSplit] '{kw}' has {site_total} pages (>1000). Switching to year-split mode...")
                            need_year_split = True
                            break
                    
                    # 🔧 每10页保存一次checkpoint，防止中断丢失
                    if pages_processed % 10 == 0:
                        checkpoint["pending"] = queue
                        checkpoint["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
                        save_checkpoint(checkpoint)
                
                # ════════════════════════════════════════════════════════════════
                # 🔀 年份拆分模式（动态关键词专用）
                # ════════════════════════════════════════════════════════════════
                if need_year_split:
                    import datetime
                    current_year = datetime.datetime.now().year
                    site_total = getattr(scraper, 'last_total_pages', 0)
                    site_records = getattr(scraper, 'last_total_records', 0)
                    print(f"[🔀 AutoSplit] Splitting '{kw}' into year-based sub-tasks (2014~{current_year})...")
                    print(f"[🔀 AutoSplit] Site reports: {site_records} records, {site_total} pages")
                    
                    def _run_sub_search(sub_kw, label):
                        """执行子任务搜索，返回 (saved_count, page_count)"""
                        nonlocal total_saved_all, kw_saved
                        sub_saved = 0
                        sub_pages = 0
                        for batch_data, new_prefixes in scraper.search(keyword=sub_kw, max_pages=MAX_PAGES):
                            sub_pages += 1
                            if batch_data:
                                count = db.save_batch(batch_data)
                                sub_saved += count
                                kw_saved += count
                                total_saved_all += count
                            for p in new_prefixes:
                                if p != kw and p not in queue and p not in completed_kw:
                                    discovered_this_round.add(p)
                                    queue.insert(0, p)
                                    print(f"[📍 Discovery] New keyword: '{p}'")
                            if sub_pages % 10 == 0:
                                checkpoint["pending"] = queue
                                checkpoint["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
                                save_checkpoint(checkpoint)
                        print(f"  >>> [{label}] Done. Saved: {sub_saved} records, {sub_pages} pages.")
                        return sub_saved, sub_pages
                    
                    for year in range(2014, current_year + 1):
                        year_kw = f"{kw}{year}"
                        print(f"\n  >>> [Year {year}] Searching: {year_kw}")
                        
                        # 先爬第一页，检测是否需要二级拆分
                        year_saved = 0
                        year_pages = 0
                        need_digit_split = False
                        
                        for batch_data, new_prefixes in scraper.search(keyword=year_kw, max_pages=MAX_PAGES):
                            year_pages += 1
                            if batch_data:
                                count = db.save_batch(batch_data)
                                year_saved += count
                                kw_saved += count
                                total_saved_all += count
                            for p in new_prefixes:
                                if p != kw and p not in queue and p not in completed_kw:
                                    discovered_this_round.add(p)
                                    queue.insert(0, p)
                                    print(f"[📍 Discovery] New keyword: '{p}'")
                            
                            # 第一页后检查：年份子任务是否也超1000页
                            if year_pages == 1:
                                year_total = getattr(scraper, 'last_total_pages', 0)
                                if year_total > 1000:
                                    print(f"  [🔀 AutoSplit L2] '{year_kw}' has {year_total} pages! Splitting by digit 0~9...")
                                    need_digit_split = True
                                    break
                            
                            if year_pages % 10 == 0:
                                checkpoint["pending"] = queue
                                checkpoint["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
                                save_checkpoint(checkpoint)
                        
                        # 二级拆分：追加数字 0~9
                        if need_digit_split:
                            for digit in range(10):
                                digit_kw = f"{year_kw}{digit}"
                                print(f"\n    >>> [Year {year}, Digit {digit}] Searching: {digit_kw}")
                                d_saved, d_pages = _run_sub_search(digit_kw, f"Year {year}, Digit {digit}")
                                year_saved += d_saved
                                year_pages += d_pages
                        
                        pages_processed += year_pages
                        print(f"  >>> [Year {year}] Total: {year_saved} records, {year_pages} pages.{' (digit-split)' if need_digit_split else ''}")
                    
                    print(f"\n[🔀 AutoSplit] Completed all years for '{kw}'. Total saved: {kw_saved}, Total pages: {pages_processed}")
                
                else:
                    # ════════════════════════════════════════════════════════════
                    # 正常模式（未拆分）
                    # ════════════════════════════════════════════════════════════
                    print(f">>> Finished '{kw}'. Saved: {kw_saved} records. Digged {pages_processed} pages.")
                    
                    # ⚠️ 1000-PAGE LIMIT WARNING（仅静态关键词才记录，动态已自动拆分）
                    if pages_processed >= MAX_PAGES and kw in static_keywords:
                        warning_msg = f"\n{'='*80}\n⚠️  WARNING: 1000-PAGE LIMIT HIT!\n"
                        warning_msg += f"   Keyword: '{kw}' (静态)\n"
                        warning_msg += f"   Pages: {pages_processed} (reached maximum)\n"
                        warning_msg += f"   Records: {kw_saved}\n"
                        warning_msg += f"\n   ⚠️ 该关键词可能存在漏采！超出1000页的记录无法获取。\n"
                        warning_msg += f"   Suggested Actions:\n"
                        warning_msg += f"   1. Manually add year-based keywords: '{kw}2023', '{kw}2024', etc.\n"
                        warning_msg += f"{'='*80}\n"
                        print(warning_msg)
                        
                        # Log to JSONL（仅静态关键词）
                        try:
                            os.makedirs("logs", exist_ok=True)
                            import json as _json
                            site_total_records = getattr(scraper, 'last_total_records', 0)
                            site_total_pages = getattr(scraper, 'last_total_pages', 0)
                            from collections import OrderedDict
                            log_entry = OrderedDict([
                                ("keyword", kw),
                                ("type", "静态"),
                                ("total_records", site_total_records if site_total_records > 0 else kw_saved),
                                ("total_pages", site_total_pages if site_total_pages > 0 else pages_processed),
                                ("scraped_pages", pages_processed),
                                ("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                                ("overflow", True)
                            ])
                            with open("logs/overflow_keywords.jsonl", "a", encoding="utf-8") as f:
                                f.write(_json.dumps(log_entry, ensure_ascii=False) + "\n")
                        except:
                            pass
                
                # If we discovered new keywords, add them to the FRONT of the queue
                if discovered_this_round:
                    new_list = list(discovered_this_round)
                    print(f"[Discovery] Found {len(new_list)} new prefix keywords: {new_list[:5]}...")
                    for np in reversed(new_list):
                        if np not in queue and np not in completed_kw: queue.insert(0, np)

                # 🔧 完成度验证：代码执行到这里 = 正常完成（或年份拆分全部完成）
                if kw not in completed_kw:
                    completed_kw.add(kw)
                    completed_list.append(kw)
                checkpoint["completed"] = completed_list
                checkpoint["current"] = None
                print(f"[✅ Completed] '{kw}' marked as done ({pages_processed} pages{', year-split' if need_year_split else ''})")
                
                checkpoint["pending"] = queue
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
