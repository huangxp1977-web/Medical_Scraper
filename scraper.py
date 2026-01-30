import time
import random
import json
from playwright.sync_api import sync_playwright
from config import BASE_URL, HEADLESS, DELAY_RANGE

class NMPAScraper:
    def __init__(self, existing_records=None):
        self.browser = None
        self.context = None
        self.page = None
        self.intercepted_data = []
        # Store existing records as a set of tuples (license_number, enterprise_name)
        self.existing_records = existing_records or set()

    def start(self):
        self.playwright = sync_playwright().start()
        
        print("[Scraper] Connecting to YOUR manually opened Chrome (Port 9222)...")
        try:
            # Connect to the Chrome instance launched by the user
            # This is the "God Mode" for bypassing anti-bot 
            self.browser = self.playwright.chromium.connect_over_cdp("http://localhost:9222")
            self.context = self.browser.contexts[0]
            
            # Use the active page
            if self.context.pages:
                self.page = self.context.pages[0]
            else:
                self.page = self.context.new_page()
                
            print("[Scraper] Connected successfully! logic will now run on your open window.")
            
        except Exception as e:
            print(f"[Error] Could not connect to Chrome on port 9222.")
            print("Please ensure you launched Chrome with this EXACT command:")
            print(r'"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\chrome_debug" --no-first-run --no-default-browser-check --disable-extensions --disable-popup-blocking')
            print(f"Details: {e}")
            raise e

    def handle_response(self, response):
        """Intercept network responses to find data JSONs."""
        try:
            # Debug: Log all JSON responses to find the right one
            if 'application/json' in response.headers.get('content-type', ''):
                url = response.url
                # Loose filter: Catch anything that looks like a search result
                if 'datasearch' in url or 'search' in url or 'query' in url or 'get' in url:
                    try:
                        data = response.json()
                        # Check if it looks like a list
                        if isinstance(data, dict) or isinstance(data, list):
                             self.intercepted_data.append(data)
                    except:
                        pass
        except Exception:
            pass

    def search(self, keyword="医疗", max_pages=5):
        try:
            print(f"[Scraper] Loading page {BASE_URL}...")
            self.page.on("response", self.handle_response)
            
            try:
                self.page.goto(BASE_URL, timeout=60000)
                self.page.wait_for_load_state("networkidle")
            except:
                print("[Scraper] Page load timed out or incomplete. Trying reload...")
                self.page.reload()
                self.page.wait_for_load_state("networkidle")
            
            # --- Interaction Steps based on User Feedback ---
            print("[Scraper] specialized check: closing any guide overlays...")
            self._close_overlays()
            time.sleep(1)
            
            print("[Scraper] Interacting with search bar...")
            time.sleep(1)
            
            # 1. Click the category DIRECTLY (User suggestion)
            # resolving Strict Mode by picking the visible one or the specific link class
            try:
                print("[Scraper] Clicking direct link '医疗器械经营企业（备案）'...")
                # The user pointed out a direct link below the search bar.
                # We use .first to avoid strict mode error if multiple exist (dropdown + list)
                # We also force a wait for it to be visible
                link = self.page.get_by_text("医疗器械经营企业（备案）").last # The list item is usually later in DOM than the dropdown
                # If .last doesn't work, we can try iterating or generic click
                if link.count() > 1:
                     print(f"[Debug] Found {link.count()} elements, clicking the last one (likely the list item)...")
                     link.last.click()
                else:
                     link.click()
                
                print("[Scraper] Category selected via direct link.")
                time.sleep(1)
                
                # 2. Input Search Keyword (REQUIRED)
                # 2. Input Search Keyword (REQUIRED)
                print(f"[Scraper] Inputting keyword '{keyword}'...")
                search_input = None
                try:
                    # Strategy A: Known placeholders
                    candidates = ["企业名称", "关键词", "输入", "名称", "注册证号"]
                    for ph in candidates:
                        loc = self.page.locator(f"input[placeholder*='{ph}']").first
                        if loc.is_visible():
                            try:
                                if loc.is_editable():
                                    search_input = loc
                                    print(f"[Debug] Found search input by placeholder: '{ph}'")
                                    break
                            except: pass

                    # Strategy B: Any editable textbox
                    if not search_input:
                        print("[Debug] Placeholder match failed. Checking all textboxes...")
                        textboxes = self.page.get_by_role("textbox").all()
                        for i, box in enumerate(textboxes):
                            if box.is_visible() and box.is_editable():
                                # Exclude the readonly one (often the category selector)
                                if not box.get_attribute("readonly"):
                                    search_input = box
                                    print(f"[Debug] Found generic editable input #{i}")
                                    break
                    
                    if search_input:
                        search_input.fill(keyword)
                    else:
                        print(f"[Scraper] WARNING: Could not auto-focus search bar. Please manually type '{keyword}'!")
                except Exception as e:
                    print(f"[Scraper] Input interaction failed: {e}")
                
                # 3. Click Search Button (SINGLE RELIABLE TRIGGER)
                print("[Scraper] Triggering Search (Click Strategy)...")
                
                # Snapshot current state
                initial_page_count = len(self.context.pages)
                search_triggered = False
                
                try:
                    # Find and click the button
                    search_btns = [
                        self.page.locator("button").filter(has_text="查询").first,
                        self.page.get_by_role("button", name="查询").first,
                        self.page.locator(".el-icon-search").locator("..").first 
                    ]
                    
                    btn_to_click = None
                    for btn in search_btns:
                        if btn.count() > 0 and btn.is_visible():
                            btn_to_click = btn
                            break
                    
                    if btn_to_click:
                        print(f"[Debug] Clicking search button: {btn_to_click}")
                        
                        # USE ROBUST WAY TO CATCH NEW PAGE
                        try:
                            with self.context.expect_page(timeout=10000) as new_page_info:
                                btn_to_click.click()
                            
                            # Sync API: new_page_info.value IS the page object
                            target_page = new_page_info.value
                            print(f"[Scraper] New window captured via expect_page: {target_page.url}")
                            search_triggered = True
                        except Exception as e_timeout:
                            print(f"[Warning] expect_page timed out: {e_timeout}. Checking manually...")
                            # Fallback: Check if page count increased anyway
                            btn_to_click.click() # Ensure it was clicked
                            time.sleep(3)
                    else:
                         print("[Error] No search button found.")
                            
                except Exception as e:
                    print(f"[Scraper] Search trigger failed: {e}") 
            except Exception as e:
                print(f"[Scraper] Interaction warning: {e}") 
            except Exception as e:
                print(f"[Scraper] Interaction warning: {e}")
                
            print("[Scraper] Waiting for results...")
            time.sleep(5)
            
            # --- Result Page Handling ---
            print("[Scraper] Checking for result page tab...")
            target_page = None
            
            # Wait a moment for tab URL to update
            time.sleep(2)
            
            # Iterate all pages to find the result one
            for _ in range(10): # retry loop for URL update
                for i, p in enumerate(self.context.pages):
                    try:
                        # print(f"[Debug] Tab {i}: {p.url}")
                        # Strict check: ONLY accept actual search result pages
                        if "search-result" in p.url:
                             target_page = p
                             print(f"[Scraper] Found Result Page by URL match: {p.url}")
                             break
                    except: pass
                
                if target_page: break
                time.sleep(1)
            
            # Fallback: If we know a new page opened (count > 1 likely means search+result), use the last one
            if not target_page and len(self.context.pages) > 1:
                print(f"[Scraper] Strict URL match failed. Defaulting to the latest tab (Tab {len(self.context.pages)-1}).")
                target_page = self.context.pages[-1]

            if target_page:
                self.page = target_page
                self.page.bring_to_front()
                try:
                    self.page.wait_for_load_state("domcontentloaded")
                    print(f"[Scraper] Switched context to: {self.page.url}")
                except: pass
            else:
                 print("[Warning] Could not identify result page. Staying on current page (Risk of 0 rows).")
            # ------------------------------------------------
            
        except Exception as e:
            print(f"[Scraper] Initial navigation failed: {e}")
            return []

        all_items = []
        
        for page_num in range(1, max_pages + 1):
            print(f"[Scraper] Processing Page {page_num}...")
            
            # Allow UI to settle (popup might animate in)
            time.sleep(2)
            
            # 0. Handle Potential Overlays (User reported a popup)
            self._close_overlays()

            # 1. Wait for data to load - Explicit Wait
            print("[Scraper] Waiting for table rows...")
            try:
                self.page.wait_for_selector("tr, .el-table__row", timeout=15000)
            except:
                print("[Scraper] Wait for rows timed out. Page might be empty or loading slowly.")

            # 2. Scrape Details (Deep Crawl)
            print("[Scraper] finding 'Details' buttons...")
            current_batch = self._scrape_with_details()
            
            if current_batch:
                all_items.extend(current_batch)
                print(f"[Scraper] Found {len(current_batch)} items on page {page_num}")
                yield current_batch
            else:
                print(f"[Scraper] Warning: No data found on page {page_num}.")
                self.page.screenshot(path=f"debug_page_{page_num}.png")
            
            # 3. Next Page
            if page_num < max_pages:
                if not self.go_to_next_page():
                    print("[Scraper] End of pages or could not click next.")
                    break
                
                sleep_time = random.uniform(*DELAY_RANGE)
                time.sleep(sleep_time)

        return all_items

    def _close_overlays(self):
        """Attempt to close known overlays/popups."""
        try:
            # Common close button selectors
            close_btns = [
                self.page.locator(".el-dialog__headerbtn"), # Element UI dialog close
                self.page.locator(".close-btn"),
                self.page.locator("button[aria-label='Close']"),
                self.page.get_by_text("关闭").first, # Most likely candidate
                self.page.get_by_text("我知道了").first,
                self.page.locator(".guide-close") # Guide overlays
            ]
            for btn in close_btns:
                if btn.count() > 0 and btn.is_visible():
                    print(f"[Scraper] Detecting overlay/popup ({btn}), closing it...")
                    btn.click(force=True) # Force click in case of overlay overlap
                    time.sleep(0.5)
        except:
            pass
            
    def _scrape_with_details(self):
        """Find rows, open detail tabs, scrape, close."""
        items = []
        try:
            # Locate rows - skip header
            rows = self.page.locator("tr").all()
            if not rows:
                rows = self.page.locator(".el-table__row").all()
            
            print(f"[Debug] Found {len(rows)} rows (including header).")
            
            # Iterate
            for i, row in enumerate(rows):
                if "企业名称" in row.inner_text(): continue # Skip header
                
                # 1. Capture Base Info from List Page (Reliable)
                base_info = {}
                try:
                    cols = row.locator("td").all()
                    if len(cols) >= 3:
                        base_info['licenseNum'] = cols[1].inner_text().strip()
                        base_info['entName'] = cols[2].inner_text().strip()
                except: pass

                # --- DUPLICATION CHECK ---
                # If we have base info, check if it's already in the DB
                if base_info.get('licenseNum') and base_info.get('entName'):
                     # Check tuple in set
                     if (base_info['licenseNum'], base_info['entName']) in self.existing_records:
                         print(f"[Scraper] Skipping existing record: {base_info['entName']}")
                         continue
                
                # 2. Find detail button
                btn = row.locator("button, a").filter(has_text="详情").first
                
                if btn.count() > 0 and btn.is_visible():
                    # Retry logic for detail page opening
                    detail_success = False
                    for attempt in range(3):
                        try:
                            # 1. Get current pages count
                            initial_pages = len(self.context.pages)
                            
                            # 2. Click the button (No expect_page context manager, handle manually)
                            # context manager sometimes conflicts with existing event loops
                            btn.click()
                            
                            # 3. Wait for new page count to increase
                            print("[Scraper] Waiting for detail page...")
                            detail_page = None
                            for _ in range(20): # Wait up to 10s
                                time.sleep(0.5)
                                if len(self.context.pages) > initial_pages:
                                    # Get the latest page
                                    detail_page = self.context.pages[-1]
                                    break
                            
                            if detail_page:
                                detail_page.wait_for_load_state("domcontentloaded")
                                # Increased wait for Vue/API rendering
                                time.sleep(2.5) 
                                
                                # Verify data presence
                                try:
                                    detail_page.wait_for_selector("td", timeout=3000)
                                except: pass

                                # Extract Details
                                detail_item = self._extract_detail_fields(detail_page)
                                
                                # Merge
                                final_item = detail_item.copy()
                                if base_info.get('entName'): final_item['entName'] = base_info['entName']
                                if base_info.get('licenseNum'): final_item['licenseNum'] = base_info['licenseNum']
                                
                                if final_item.get('entName'):
                                    items.append(final_item)
                                    print(f"[Scraper] + {final_item['entName']}")
                                
                                detail_page.close()
                                detail_success = True
                                break 
                            else:
                                print(f"[Detail Retry {attempt+1}/3] No new page opened.")
                        
                        except Exception as e:
                            print(f"[Detail Retry {attempt+1}/3] Failed: {e}")
                            # Close page if it opened but failed later
                            try:
                                if 'detail_page' in locals(): detail_page.close()
                            except: pass
                            time.sleep(2)
                    
                    if not detail_success:
                        print(f"[Detail Error] Row {i}: Failed into detail page after 3 attempts.")
                        # Fallback: Save base info

                        if base_info.get('entName'):
                            print(f"[Scraper] Saving base info only for Row {i}")
                            items.append(base_info)
                        
                        # Attempt to close stuck page
                        try:
                           if 'detail_page' in locals(): detail_page.close()
                        except: pass
                else:
                     # No button, but maybe have base info
                     if base_info.get('entName'):
                         items.append(base_info)
                    
        except Exception as e:
            print(f"[Scraper] Error in detail loop: {e}")
            
        return items

    def _extract_detail_fields(self, page):
        """Parse the detail page table using strict Key-Value pairing with fuzzy match."""
        item = {}
        try:
            # Map for Chinese keys to our DB keys
            # Keys here should be compacted (no whitespace)
            key_map = {
                "编号": "licenseNum",
                "企业名称": "entName",
                "法定代表人": "legalRep",
                "企业负责人": "resPerson",
                "住所": "entAddress",
                "经营场所": "opAddress",
                "经营方式": "opMode",
                "经营范围": "scope",
                "库房地址": "warehouseAddr",
                "备案部门": "filingDept",
                "备案日期": "filingDate"
            }
            
            # Strict strategy: Iterate Table Rows
            rows = page.locator("tr").all()
            
            # print(f"[Debug] Parsing details from {page.url}...")
            
            for row in rows:
                cells = row.locator("td").all()
                if len(cells) >= 2:
                    # Fuzzy normalization: remove spaces, colons, chinese colons
                    raw_label = cells[0].inner_text()
                    compact_label = raw_label.replace(" ", "").replace("　", "").replace("：", "").replace(":", "").strip()
                    val = cells[1].inner_text().strip()
                    
                    # Debug print to see what we are finding
                    # print(f"   Key: '{compact_label}' -> Val: '{val[:10]}...'")
                    
                    if compact_label in key_map:
                        item[key_map[compact_label]] = val
                        
            # Data validation check
            if not item.get("legalRep"):
                print(f"[Warning] Legal Rep missing for {item.get('entName', 'Unknown')}. Check key mapping.")

            item['url'] = page.url
            item['raw_json'] = json.dumps(item, ensure_ascii=False)
            
        except Exception as e:
            print(f"[Detail Parsing Error] {e}")
            
        return item


    def go_to_next_page(self):
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            next_buttons = [
                self.page.get_by_role("button", name="下一页"),
                self.page.get_by_text("下一页", exact=True),
                self.page.get_by_title("下一页"),
                self.page.locator(".layui-laypage-next"),
                self.page.locator(".btn-next"),
                self.page.locator("li.next"),
            ]
            for btn in next_buttons:
                if btn.count() > 0 and btn.is_visible():
                    btn.click()
                    return True
            return False
        except Exception:
            return False

    def close(self):
        try:
            if self.context: self.context.close()
            if self.browser: self.browser.close()
            if self.playwright: self.playwright.stop()
        except:
            pass
