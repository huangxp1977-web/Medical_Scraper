import time
import random
import json
import os
from playwright.sync_api import sync_playwright
import config
from config import BASE_URL, HEADLESS, DELAY_RANGE
from engine.rate_limiter import SmartRateLimiter

class NMPAScraper:
    def __init__(self, existing_records=None):
        self.browser = None
        self.context = None
        self.page = None
        self.intercepted_data = []
        self.playwright = None
        # Set of (licenseNum, entName) already in DB to avoid dupes
        # Set of (licenseNum, entName) already in DB to avoid dupes
        if existing_records:
            self.existing_records = existing_records 
            # Split into fast lookups
            self.existing_licenses = {rec[0] for rec in existing_records if rec[0]}
            self.existing_names = {rec[1] for rec in existing_records if rec[1]}
        else:
            self.existing_records = set()
            self.existing_licenses = set()
            self.existing_names = set()
        
        # Initialize the Brain (choose limiter based on config)
        if config.USE_AGGRESSIVE_RECOVERY:
            # 🧪 Experimental Mode: Use aggressive recovery limiter
            from engine.rate_limiter_experimental import SmartRateLimiter as ExperimentalLimiter
            self.limiter = ExperimentalLimiter(
                default_base=config.RL_BASE_WAIT,
                min_base=config.RL_MIN_WAIT,
                max_base=config.RL_MAX_WAIT,
                penalty_add=config.RL_PENALTY_ADD,
                recovery_step=config.RL_RECOVERY_STEP,
                aggressive_recovery=True
            )
            print("[🧪 EXPERIMENTAL MODE] Using Aggressive Recovery Limiter")
        else:
            self.limiter = SmartRateLimiter(
                default_base=config.RL_BASE_WAIT,
                min_base=config.RL_MIN_WAIT,
                max_base=config.RL_MAX_WAIT,
                penalty_add=config.RL_PENALTY_ADD,
                recovery_step=config.RL_RECOVERY_STEP
            )

    def start(self):
        self.playwright = sync_playwright().start()
        
        print("[Scraper] Connecting to YOUR manually opened Chrome (Port 9222)...")
        try:
            # Connect to the Chrome instance launched by the user
            self.browser = self.playwright.chromium.connect_over_cdp("http://localhost:9222")
            self.context = self.browser.contexts[0]
            
            # Use the active page
            if self.context.pages:
                self.page = self.context.pages[0]
            else:
                self.page = self.context.new_page()
                
            print("[Scraper] Connected successfully! Logic will now run on your open window.")
            
        except Exception as e:
            print(f"[Error] Could not connect to Chrome on port 9222.")
            print("Please ensure you launched Chrome with this EXACT command:")
            print(r'"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\chrome_debug" --no-first-run --no-default-browser-check --disable-extensions --disable-popup-blocking')
            print(f"Details: {e}")
            raise e

    def handle_response(self, response):
        """Intercept network responses to find data JSONs."""
        try:
            if 'application/json' in response.headers.get('content-type', ''):
                if '/datasearch/' in response.url:
                    try:
                        data = response.json()
                        if isinstance(data, dict) or isinstance(data, list):
                             self.intercepted_data.append(data)
                    except:
                        pass
        except Exception:
            pass

    def search(self, keyword="上海", max_pages=5, skip_dedupe=False):
        """
        Main search entry point with tab-syncing and fallback search logic.
        skip_dedupe: If True, skip duplicate checking (used during repair phase to process all same-name records)
        """
        try:
            # 1. SCAN FOR EXISTING RESULT TAB (User-first approach)
            print("[Scraper] Scanning open tabs for 'search-result.html'...")
            existing_target = None
            for p in self.context.pages:
                if "search-result.html" in p.url:
                    existing_target = p
                    break
            
            if existing_target:
                self.page = existing_target
                self.page.bring_to_front()
                print(f"[Scraper] Found existing result tab: {self.page.url}")
            else:
                # 2. FALLBACK: Normal Search Flow
                print(f"[Scraper] No result tab found. Starting from {BASE_URL}...")
                self.page.on("response", self.handle_response)
                
                try:
                    self.page.goto(BASE_URL, timeout=60000)
                    self.page.wait_for_load_state("networkidle")
                except Exception as e_nav:
                    print(f"[Scraper] Navigation issue: {e_nav}. Trying reload...")
                    self.page.reload()
                
            self._close_overlays()
            time.sleep(1)
            # Select Category (User Request: Auto-Select)
            category_target = "医疗器械经营企业（备案）"
            try:
                # 1. First, check if the category tag is ALREADY active (Best visual confirmation)
                # The screenshot shows the tag below the search bar even if the dropdown says "请选择"
                tag_exists = self.page.locator(f".el-tag:has-text('{category_target}')").count() > 0
                
                if tag_exists:
                        print(f"[Scraper] Found active category tag '{category_target}'. Skipping selection.")
                else:
                    print(f"[Scraper] Tag missing. Checking dropdown value...")
                    # 2. Check current value via JS (targeting the 'Select' input we identified as Input #0)
                    current_cat = self.page.evaluate("""() => {
                        const i = document.querySelector('input[placeholder="请选择"]'); 
                        return i ? i.value : '';
                    }""")
                    
                    if category_target not in current_cat:
                        print(f"[Scraper] Category mismatch. actively selecting '{category_target}'...")
                        self.page.click('input[placeholder="请选择"]', timeout=3000)
                        time.sleep(1)
                        # Wait and Click the option
                        # Use locator with visible=True to avoid clicking hidden dropdowns
                        self.page.locator(f".el-select-dropdown__item:has-text('{category_target}')").filter(has=self.page.locator(":visible")).first.click(timeout=5000)
                        time.sleep(1)
                    else:
                        print(f"[Scraper] Category dropdown already set to '{current_cat}'. Skipping.")
            except Exception as e_cat:
                print(f"[Warning] Category selection failed: {e_cat}")

            # Fill Keyword (Strict Validation Loop)
            try:
                print(f"[Scraper] Inputting keyword '{keyword}'...")
                search_success = False
                
                for attempt in range(3):
                    # JS Strategy: Find input, wipe it, set it, dispatch events
                    # This bypasses any "element not interactable" or focus issues
                    js_success = self.page.evaluate(f"""(kw) => {{
                        // Try multiple ways to find the MAIN search input
                        const inputs = Array.from(document.querySelectorAll('input'));
                        // Filter for visible text inputs that look like the main search bar AND ARE NOT READONLY
                        const target = inputs.find(i => {{
                            const style = window.getComputedStyle(i);
                            return style.display !== 'none' && 
                                    style.visibility !== 'hidden' && 
                                    i.type === 'text' && 
                                    !i.readOnly && // CRITICAL FIX: Ignore the 'Select' dropdown
                                    (i.placeholder.includes('企业名称') || i.className.includes('el-input__inner')) &&
                                    i.clientWidth > 100; 
                        }});
                        
                        if (target) {{
                            target.focus();
                            target.value = ''; // Wipe
                            target.value = kw; // Set
                            
                            // CJK Event Sequence for Element UI / Vue
                            target.dispatchEvent(new Event('compositionstart', {{ bubbles: true }}));
                            target.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            target.dispatchEvent(new Event('compositionend', {{ bubbles: true }}));
                            target.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            
                            return true;
                        }}
                        return false;
                    }}""", keyword)
                    
                    if js_success:
                        print(f"[Scraper] JS injected '{keyword}'. Verifying...")
                        time.sleep(1)
                        # Verify value
                        actual_val = self.page.evaluate("""() => {
                            // Specific verification: Only verify the input with '企业名称' placeholder to avoid reading the dropdown
                            const i = document.querySelector('input[placeholder*="企业名称"]');
                            return i ? i.value : '';
                        }""")
                        
                        if actual_val == keyword:
                            print(f"[Scraper] Verified input is '{keyword}'. Pressing Enter.")
                            self.page.keyboard.press("Enter")
                            search_success = True
                            break
                        else:
                            print(f"[Scraper] Mismatch after JS! Expected '{keyword}', got '{actual_val}'.")
                    else:
                        print(f"[Scraper] JS could not find input box (Attempt {attempt+1}). Retrying...")
                        time.sleep(1)

                if not search_success:
                    print(f"[Error] Failed to set search keyword to '{keyword}'. Abort.")
                    try: self.page.screenshot(path="logs/debug_search_fail.png")
                    except: pass
                    return [] 
                
                time.sleep(1)
                
                # Click Search Button (Magnifier) as backup
                try:
                    # Search for the button next to the input
                    self.page.evaluate("""() => {
                        const btns = Array.from(document.querySelectorAll('button'));
                        const searchBtn = btns.find(b => b.innerText.includes('查询') || b.querySelector('i.el-icon-search'));
                        if (searchBtn) searchBtn.click();
                    }""")
                except: pass
                
                print("[Scraper] Search trigger sequence completed.")
            except Exception as e_int:
                print(f"[Scraper] Interaction sequence failed: {e_int}")

            # Check for "No Data" immediately
            print("[Scraper] Verifying search results...")
            try:
                self.page.wait_for_selector("tr, .el-table__row, .no-data, text='暂无数据'", timeout=8000)
                if self.page.locator("text='暂无数据内容'").count() > 0 or self.page.locator("text='暂无数据'").count() > 0:
                     print("[Scraper] Search returned NO DATA. Stopping.")
                     return []
            except:
                pass

            self.page.wait_for_load_state("domcontentloaded")
            time.sleep(2) # Settle time

        except Exception as e:
            print(f"[Scraper] search() method failed: {e}")
            return []

        # Yielding results loop (Smart Page Counting)
        effective_pages = 0
        total_attempts = 0 # Safety breaker
        self.current_discovered = set()
        
        while effective_pages < max_pages:
            total_attempts += 1
            if total_attempts > max_pages * 5: # Prevent infinite loops if site is huge but all dups
                print("[Scraper] Max safety attempts reached. Stopping.")
                break
                
            print(f"[Scraper] Processing Page {total_attempts} (Effective: {effective_pages}/{max_pages})...")
            time.sleep(2)
            self._close_overlays()

            # Wait for data table
            try:
                self.page.wait_for_selector("tr, .el-table__row", timeout=15000)
            except:
                print("[Scraper] Table wait timeout. Check if page is empty.")

            current_batch = self._scrape_with_details(skip_dedupe=skip_dedupe)
            
            # Logic: If batch has items, it counts as a page.
            # We yield both the data AND any NEW prefixes found during this page
            if current_batch or self.current_discovered:
                yield (current_batch, list(self.current_discovered))
                # Clear for next yield to avoid redundant notification, 
                # though the caller manages the global set.
                self.current_discovered = set() 
                
                if current_batch:
                    effective_pages += 1
                    print(f"[Scraper] Page yielded new data. Counted as valid page.")
            else:
                print(f"[Scraper] Page yielded NO new data and no new prefixes. NOT counting.")
            
            if not self.go_to_next_page():
                print("[Scraper] No more pages.")
                break

    def _close_overlays(self):
        """Attempt to close known overlays/popups."""
        try:
            close_btns = [
                self.page.locator(".el-dialog__headerbtn").first,
                self.page.locator(".close-btn").first,
                self.page.locator("button[aria-label='Close']").first,
                self.page.get_by_role("button", name="关闭").first
            ]
            for btn in close_btns:
                if btn.count() > 0 and btn.is_visible():
                    btn.click(force=True)
                    time.sleep(0.5)
        except: pass

    def _scrape_with_details(self, skip_dedupe=False):
        """Find rows, open detail tabs using hardware-emulated clicks, scrape, close."""
        items = []
        try:
            # 1. WAIT FOR DATA (Crucial: AJAX might be slow)
            print("[Scraper] Waiting for table data to render...")
            try:
                # Wait for EITHER a Details button OR just table rows (fallback)
                try:
                    self.page.wait_for_selector("text='详情'", timeout=10000)
                except:
                    # If no 'Details' text found, maybe just wait for rows (Global Search might behave differently)
                    self.page.wait_for_selector(".el-table__row", timeout=5000)
                
                time.sleep(1) # Extra settle time
            except:
                print("[Scraper] Wait timeout. Page might be empty or loading very slowly.")
                return []

            # 2. Find all potential rows
            rows = self.page.locator("tr").all()
            if not rows:
                rows = self.page.locator(".el-table__row").all()
            
            # Filter out header rows that look like data but aren't
            rows = [r for r in rows if "企业名称" not in r.inner_text()]
            
            print(f"[Debug] Found {len(rows)} potential table rows on this page.")
            
            for i, row in enumerate(rows):
                
                # Scroll to row to ensure elements are lazy-loaded/visible
                try: row.scroll_into_view_if_needed()
                except: pass

                # Capture Base Info
                base_info = {}
                try:
                    cols = row.locator("td").all()
                    if len(cols) >= 3:
                        lic_text = cols[1].inner_text().strip()
                        base_info['licenseNum'] = lic_text
                        base_info['entName'] = cols[2].inner_text().strip()
                        
                        # DYNAMIC KEYWORD HARVESTING (User Request)
                        # Extract the regulator identifier (e.g. 京朝食药监械经营备案)
                        # Harvesting happens for ALL rows, even duplicates, to build the full discovery map.
                        if lic_text:
                            # Use regex to find the first sequence of digits (usually the year) and take everything before it
                            prefix_match = re.search(r'^(\D+)', lic_text)
                            if prefix_match:
                                prefix = prefix_match.group(1).strip()
                                # Common noise removal: stop at '2', '1' or '号' if greedy
                                prefix = re.split(r'20\d\d|20[012]\d', prefix)[0].strip()
                                if len(prefix) > 1:
                                    self.current_discovered.add(prefix)
                except: pass

                # Duplication Check (Loose Coupling with Truncation Support)
                # Skip check during repair phase to allow re-scraping same-named records
                is_duplicate = False
                if not skip_dedupe:
                    curr_lic = base_info.get('licenseNum', '').strip()
                    curr_name = base_info.get('entName', '').strip()

                    # Check by license number (exact match)
                    if curr_lic and curr_lic in self.existing_licenses:
                        is_duplicate = True
                    
                    # Check by name (support truncated names with "...")
                    if not is_duplicate and curr_name:
                        # If list page name is truncated (ends with "..."), match by prefix
                        if curr_name.endswith('...'):
                            name_prefix = curr_name[:-3].strip()  # Remove "..."
                            # Check if any existing name starts with this prefix
                            for existing_name in self.existing_names:
                                if existing_name.startswith(name_prefix):
                                    is_duplicate = True
                                    print(f"[Dedupe] Truncated match: '{curr_name}' → '{existing_name}'")
                                    break
                        else:
                            # Exact name match
                            if curr_name in self.existing_names:
                                is_duplicate = True

                if is_duplicate:
                     print(f"[Scraper] Skipping: {curr_name} (Already in DB)")
                     continue
                
                # Find detail button (Strategy: Text -> Class -> Last Column)
                btn = row.locator("button, a, .el-button, span").filter(has_text="详情").first
                
                # FALLBACK 1: Try positional (The button is always in the last column)
                if btn.count() == 0:
                     cols = row.locator("td").all()
                     if cols:
                         btn = cols[-1].locator("button, a, span, div").first
                         if btn.count() > 0:
                             print(f"[Scraper] Found button via Last Column Strategy.")

                # FALLBACK 2: Click Enterprise Name
                if btn.count() == 0:
                    print(f"[Scraper] Row {i}: No 'Details' button found. Trying Name Click.")
                    btn = row.locator("td").nth(2).locator("div, span, a").first 
                
                if btn.count() > 0:
                    detail_success = False
                    for attempt in range(3):
                        try:
                            initial_page_count = len(self.context.pages)
                            print(f"[Scraper] Row {i}: Opening '{base_info.get('entName', 'Unknown')}'...")
                            
                            detail_page = None
                            try: btn.scroll_into_view_if_needed(timeout=2000)
                            except: pass
                            
                            box = btn.bounding_box()
                            if box:
                                center_x = box['x'] + box['width'] / 2
                                center_y = box['y'] + box['height'] / 2
                                # Hardware Click Emulation
                                self.page.mouse.move(center_x - 5, center_y - 5)
                                time.sleep(random.uniform(0.2, 0.4))
                                self.page.mouse.move(center_x, center_y)
                                time.sleep(random.uniform(0.3, 0.6))
                                with self.context.expect_page(timeout=10000) as new_page_info:
                                    self.page.mouse.click(center_x, center_y)
                                detail_page = new_page_info.value
                            else:
                                btn.click()
                                time.sleep(2)
                            
                            if not detail_page and len(self.context.pages) > initial_page_count:
                                detail_page = self.context.pages[-1]

                            if detail_page:
                                try:
                                    detail_page.bring_to_front()
                                    detail_page.wait_for_load_state("domcontentloaded")
                                    
                                    # Content Polling
                                    data_ready = False
                                    for _ in range(20):
                                        try:
                                            if detail_page.locator("tr").count() > 5:
                                                has_val = detail_page.evaluate("""() => {
                                                    const divs = Array.from(document.querySelectorAll('td .cell div'));
                                                    const ignore = ["编号", "企业名称", "法定代表人", "企业负责人", "住所", "经营场所", "经营方式", "经营范围", "库房地址", "备案部门", "备案日期"];
                                                    return divs.some(d => d.innerText.trim().length > 1 && !ignore.includes(d.innerText.trim()));
                                                }""")
                                                if has_val: data_ready = True; break
                                        except: pass
                                        time.sleep(0.5)
                                    
                                    # Persistent Reload Strategy with Randomized Backoff (Smart Limiter)
                                    reload_attempts = 0
                                    while not data_ready and reload_attempts < 7:
                                        reload_attempts += 1
                                        
                                        # Tell the brain we failed
                                        if reload_attempts == 1: self.limiter.record_block()
                                        
                                        # Get adaptive wait time (Base + Increments)
                                        wait_time = self.limiter.get_backoff_wait(reload_attempts)
                                        
                                        print(f"[SmartLimiter] BLANK page! Penalty Base: {self.limiter.current_base:.1f}s. Waiting {wait_time:.2f}s (Attempt {reload_attempts}/7)...")
                                        time.sleep(wait_time) 
                                        
                                        try:
                                            detail_page.reload(timeout=60000)
                                            detail_page.wait_for_load_state("domcontentloaded")
                                            time.sleep(3)
                                            # Re-check data
                                            for _ in range(10):
                                                if detail_page.locator("tr").count() > 5:
                                                    data_ready = True
                                                    break
                                                time.sleep(0.5)
                                        except Exception as e_rel:
                                            print(f"[Warning] Reload attempt {reload_attempts} failed: {e_rel}")

                                    if not data_ready:
                                        print(f"[CRITICAL] Still BLANK after {reload_attempts} attempts (~10 mins).")
                                        self._log_failure(base_info, "Blank Page / IP Block")
                                        self.save_failure_artifacts(detail_page, f"Ban_{base_info.get('entName', 'Unknown')}")
                                        raise Exception("ABORT: Consistent blank pages detected. Please check IP status or website availability.")

                                    # Extraction
                                    detail_item = self._extract_detail_fields(detail_page)
                                    
                                    if not detail_item:
                                        print(f"[Scraper] Extraction failed (Incomplete data) for: {base_info.get('entName')}. Retrying or skipping...")
                                        continue # Go to next attempt for this row

                                    # Use DETAIL page data as authoritative (has full names, not truncated)
                                    # Only fall back to list page if detail is missing
                                    final_item = detail_item.copy()
                                    # Fill in any missing fields from list page
                                    for key, value in base_info.items():
                                        if key not in final_item or not final_item.get(key):
                                            final_item[key] = value
                                    
                                    # IMPORTANT: Always use detail page entName if available (it's complete)
                                    # List page names may be truncated with "..."
                                    if detail_item.get('entName'):
                                        final_item['entName'] = detail_item['entName']
                                    
                                    # Double Check: Ensure we have a REAL detail field
                                    if final_item.get('legalRep') or final_item.get('resPerson') or final_item.get('opMode'):
                                        items.append(final_item)
                                        # Update Dedupe Set immediately to prevent re-scraping in same session
                                        if final_item.get('licenseNum'):
                                            self.existing_licenses.add(final_item['licenseNum'].strip())
                                        if final_item.get('entName'):
                                            self.existing_names.add(final_item['entName'].strip())
                                        
                                        print(f"[Scraper] Captured: {final_item['entName']}")
                                        # Tell the brain we won
                                        self.limiter.record_success()
                                        detail_success = True
                                    else:
                                        print(f"[Warning] Record for {final_item['entName']} filtered out: No detail fields extracted.")
                                        self._log_failure(base_info, "Empty Detail Fields (Zero Payload)")
                                    
                                    # Adaptive Sleep
                                    sleep_time = self.limiter.get_delay()
                                    print(f"[SmartLimiter] Resting for {sleep_time:.2f}s...")
                                    time.sleep(sleep_time)
                                    break
                                    
                                finally:
                                    # CRITICAL: Always close detail page to prevent tab accumulation
                                    try:
                                        detail_page.close()
                                        print(f"[Scraper]Detail page closed.")
                                    except:
                                        pass 
                            else:
                                print(f"[Detail Retry {attempt+1}/3] No tab found.")
                        except Exception as e:
                            print(f"[Detail Retry {attempt+1}/3] Error: {e}")
                            time.sleep(2)
                    
                    if not detail_success and base_info.get('entName'):
                        print(f"[Scraper] Failed details for: {base_info['entName']}. Item will NOT be saved to avoid ghost records.")
                        self._log_failure(base_info, "Extraction Failed / Closed unexpectedly")
        except Exception as e:
            print(f"[Scraper] detail loop failure: {e}")
        return items

    def _extract_detail_fields(self, page):
        """Parse the table using strict Key-Value pairing with fuzzy match and retry logic."""
        item = {}
        # Retry up to 3 times if essential data is missing (Async Rendering)
        for attempt in range(3):
            try:
                # Refresh element list on each attempt
                rows = page.locator("tr").all()
                if not rows:
                    time.sleep(1)
                    continue

                item = {} # Reset
                key_map = {
                    "编号": "licenseNum", "企业名称": "entName", "法定代表人": "legalRep",
                    "企业负责人": "resPerson", "住所": "entAddress", "经营场所": "opAddress",
                    "经营方式": "opMode", "经营范围": "scope", "库房地址": "warehouseAddr",
                    "备案部门": "filingDept", "备案日期": "filingDate"
                }
                
                for row in rows:
                    cells = row.locator("td").all()
                    if len(cells) >= 2:
                        raw_label = cells[0].inner_text()
                        # Normalize label: Remove spaces, colons, newlines
                        compact_label = raw_label.replace(" ", "").replace("　", "").replace("：", "").replace(":", "").replace("\n", "").strip()
                        val = cells[1].inner_text().strip()
                        
                        if compact_label in key_map:
                            field_key = key_map[compact_label]
                            
                            # 🧹 数据清洗：过滤无效的法人/负责人值
                            if field_key in ("legalRep", "resPerson"):
                                import re
                                # 1. 去除括号前缀，如 "(负责人)陈泓" -> "陈泓"
                                val = re.sub(r'^[\(（][^)）]*[\)）]', '', val).strip()
                                # 2. 去除括号后缀，如 "罗焯(总公司)" -> "罗焯"
                                val = re.sub(r'[\(（][^)）]*[\)）]$', '', val).strip()
                                
                                # 3. 过滤无效值
                                invalid_values = {"无", "无此项", "无法人", "-", "/", "\\", "——", "—", "暂无", "无数据", "空", "null", "NULL", "N/A", "n/a"}
                                if val in invalid_values:
                                    val = ""
                                # 4. 只要包含*就不存
                                elif "*" in val:
                                    val = ""
                            
                            item[field_key] = val
                
                # Validation: Require at least Name, License AND identity data
                if item.get("entName") and item.get("licenseNum") and (item.get("legalRep") or item.get("resPerson")):
                    break 
                else:
                    print(f"[Parser] Attempt {attempt+1}: Identity data missing. Retrying...")
                    time.sleep(1.5)
            except Exception as e:
                print(f"[Parsing Error] Attempt {attempt+1}: {e}")
                time.sleep(1)
        
        # Final Verification
        if not item.get("legalRep") and not item.get("resPerson"):
            return None
            
        return item

    def save_failure_artifacts(self, page, name):
        """Forensic capture of failed pages."""
        try:
            debug_dir = os.path.join("logs", "debug_data")
            if not os.path.exists(debug_dir): os.makedirs(debug_dir)
            timestamp = int(time.time())
            safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '-', '_')]).strip() or "unknown"
            # HTML
            with open(os.path.join(debug_dir, f"{safe_name}_{timestamp}.html"), 'w', encoding='utf-8') as f:
                f.write(page.content())
            # PNG
            page.screenshot(path=os.path.join(debug_dir, f"{safe_name}_{timestamp}.png"))
        except: pass

    def go_to_next_page(self):
        try:
            print("[Scraper] Attempting to go to next page...")
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)
            
            # Element UI standard pagination "Next" button
            # We must ignore if it has 'disabled' attribute or class
            next_btn = self.page.locator("button.btn-next").first
            
            if next_btn.count() > 0:
                if next_btn.is_disabled() or "disabled" in next_btn.get_attribute("class"):
                    print("[Scraper] Next button is disabled. End of list.")
                    return False
                next_btn.click()
                print("[Scraper] Clicked 'Next' button.")
                return True
            
            # Fallback text search
            fallback_btn = self.page.locator("li.next, button:has-text('下一页')").first
            if fallback_btn.count() > 0 and fallback_btn.is_visible():
                 fallback_btn.click()
                 print("[Scraper] Clicked 'Next' (fallback).")
                 return True
                 
            print("[Scraper] No 'Next' button found.")
            return False
        except Exception as e: 
            print(f"[Scraper] Pagination error: {e}")
            return False

    def _log_failure(self, base_info, reason):
        """Append to logs/failed_details.jsonl for manual review."""
        try:
            log_file = os.path.join("logs", "failed_details.jsonl")
            entry = {
                "timestamp": int(time.time()),
                "iso_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "entName": base_info.get("entName", "Unknown"),
                "licenseNum": base_info.get("licenseNum", "Unknown"),
                "reason": reason
            }
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except: pass

    def close(self):
        try:
            if self.browser: self.browser.close()
            if self.playwright: self.playwright.stop()
        except: pass
