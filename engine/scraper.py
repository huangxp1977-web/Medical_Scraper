import time
import random
import json
import os
import re
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
                
                # 🔧 对齐成功诊断脚本：使用 fill + 物理按键触发 Vue 同步
                input_locator = self.page.locator('input[placeholder*="企业名称"]')
                if input_locator.count() > 0:
                    input_locator.fill(keyword)
                    time.sleep(0.5)
                    self.page.keyboard.press("End")
                    self.page.keyboard.press("Space")
                    self.page.keyboard.press("Backspace")
                    time.sleep(0.5)
                    self.page.keyboard.press("Enter")
                    search_success = True
                else:
                    print(f"[Error] Could not find search input for '{keyword}'")
                    return []

                if not search_success:
                    print(f"[Error] Failed to set search keyword to '{keyword}'. Abort.")
                    try: self.page.screenshot(path="logs/debug_search_fail.png")
                    except: pass
                    return [] 
                
                
                print("[Scraper] Search trigger sequence completed.")
            except Exception as e_int:
                print(f"[Scraper] Interaction sequence failed: {e_int}")

            # 🔧 FIX: 不再用 wait_for_selector("tr") —— 已确认它会破坏 Vue 分页状态
            print("[Scraper] Verifying search results...")
            time.sleep(5)  # 等待搜索结果加载
            if self.page.locator("text='暂无数据内容'").count() > 0 or self.page.locator("text='暂无数据'").count() > 0:
                 print("[Scraper] Search returned NO DATA. Stopping.")
                 return []

        except Exception as e:
            print(f"[Scraper] search() method failed: {e}")
            return []

        # 🔧 读取网页上的总条数/总页数（用于超限记录）
        self._read_pagination_info()
        
        # Yielding results loop (Smart Page Counting)
        effective_pages = 0
        total_attempts = 0 # Safety breaker
        self.current_discovered = set()
        
        # 🔧 FIX: 网站最多只允许翻1000页（即使数据显示有更多页）
        SITE_PAGE_LIMIT = 1000
        
        while effective_pages < max_pages:
            total_attempts += 1
            
            # 🔧 FIX: 实际翻页数不能超过网站限制
            if total_attempts > SITE_PAGE_LIMIT:
                print(f"[Scraper] Reached site's {SITE_PAGE_LIMIT}-page limit. Stopping.")
                break
            
            if total_attempts > max_pages * 5: # Prevent infinite loops if site is huge but all dups
                print("[Scraper] Max safety attempts reached. Stopping.")
                break
                
            print(f"[Scraper] Processing Page {total_attempts} (Effective: {effective_pages}/{max_pages})...")
            time.sleep(2)
            self._close_overlays()

            # 🔧 FIX: 不再用 wait_for_selector("tr") —— 由 _scrape_with_details 内部的
            # wait_for_selector("详情"/".el-table__row") 来等待数据加载
            try:
                current_batch = self._scrape_with_details(skip_dedupe=skip_dedupe)
            except Exception as e:
                # 捕获熔断信号，执行自动清Cookie并在同页码满血复活
                if "ABORT:" in str(e):
                    print(f"\n🚨 [Anti-Ban] IP soft-blocked on attempt {total_attempts}. Executing MELTDOWN RECOVERY...")
                    self._recover_meltdown(keyword, total_attempts)
                    # 恢复后重置 limiter，避免再次立刻进入高处罚
                    try: self.limiter.current_base = self.limiter.default_base
                    except: pass
                    # 修正页码计数器，确保重试当前页不在打印时串号
                    total_attempts -= 1
                    continue # 重新执行对该页的抓取
                else:
                    raise e
            
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

    def _read_pagination_info(self):
        """读取分页栏的 '共 XX 条' 和总页数，存入实例属性供 main.py 使用"""
        import re as _re
        self.last_total_records = 0
        self.last_total_pages = 0
        try:
            # Element UI 分页组件通常有 .el-pagination__total 显示 '共 52969 条'
            total_el = self.page.locator(".el-pagination__total, span.el-pagination__total").first
            if total_el.count() > 0:
                total_text = total_el.inner_text()  # e.g. '共 52969 条'
                match = _re.search(r'(\d+)', total_text.replace(',', '').replace(' ', ''))
                if match:
                    self.last_total_records = int(match.group(1))
                    # 每页10条，计算总页数
                    self.last_total_pages = (self.last_total_records + 9) // 10
                    print(f"[Scraper] 📊 Total records on site: {self.last_total_records} ({self.last_total_pages} pages)")
        except Exception as e:
            print(f"[Scraper] Could not read pagination info: {e}")

    def _recover_meltdown(self, keyword, target_page):
        """
        Recover from a hard block:
        1. Clear cookies
        2. Close extra tabs
        3. Navigate to base URL, pick category, search again
        4. Jump/fast-forward to target_page
        """
        print(f"\n[🔥 MELTDOWN] Initiating 120s deep sleep and Cookie reset...")
        try: self.context.clear_cookies()
        except: pass
        
        # Deep sleep to cool down IP
        time.sleep(120)
        
        # Close extra tabs
        while len(self.context.pages) > 1:
            try: self.context.pages[-1].close()
            except: pass
        
        if len(self.context.pages) > 0:
            self.page = self.context.pages[0]
            self.page.bring_to_front()
        
        print(f"[🔥 MELTDOWN] Restarting search for '{keyword}'...")
        try:
            self.page.goto(BASE_URL, timeout=60000)
            self.page.wait_for_load_state("networkidle")
        except:
            self.page.reload()
            
        self._close_overlays()
        time.sleep(1)
        
        # Category
        category_target = "医疗器械经营企业（备案）"
        try:
            self.page.click('input[placeholder="请选择"]', timeout=3000)
            time.sleep(1)
            self.page.locator(f".el-select-dropdown__item:has-text('{category_target}')").filter(has=self.page.locator(":visible")).first.click(timeout=5000)
            time.sleep(1)
        except: pass
        
        # Search
        try:
            input_locator = self.page.locator('input[placeholder*="企业名称"]')
            if input_locator.count() > 0:
                input_locator.fill(keyword)
                time.sleep(0.5)
                self.page.keyboard.press("End")
                self.page.keyboard.press("Space")
                self.page.keyboard.press("Backspace")
                time.sleep(0.5)
                self.page.keyboard.press("Enter")
        except: pass
        
        print(f"[🔥 MELTDOWN] Waiting for initial results...")
        time.sleep(5)
        
        # Jump or Fast-Forward
        if target_page > 1:
            print(f"[🔥 MELTDOWN] Fast-forwarding back to page {target_page}...")
            
            # Attempt direct jump if jump input exists
            jump_input = self.page.locator("span.el-pagination__jump input").first
            if jump_input.count() > 0 and jump_input.is_visible():
                try:
                    jump_input.fill(str(target_page))
                    time.sleep(0.5)
                    jump_input.press("Enter")
                    print(f"[🔥 MELTDOWN] Triggered direct pagination jump to page {target_page}.")
                    time.sleep(3)
                    return
                except:
                    print(f"[🔥 MELTDOWN] Direct jump failed. Falling back to next clicking...")
                    
            # Fallback to next clicking
            for p in range(1, target_page):
                if p % 10 == 0:
                    print(f"[🔥 MELTDOWN] Fast-forward progress: {p}/{target_page}...")
                if not self.go_to_next_page():
                    break
                time.sleep(1.5)
        print(f"[🔥 MELTDOWN] Recovery complete. Resuming scraping.")

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
            # 🔧 FIX: 不再用 wait_for_selector —— 已确认会破坏 Vue 状态
            # 用 time.sleep 替代，等待数据渲染
            time.sleep(2)

            # 2. Find all potential rows
            rows = self.page.locator("tr").all()
            if not rows:
                rows = self.page.locator(".el-table__row").all()
            
            # Filter out header rows that look like data but aren't
            rows = [r for r in rows if "企业名称" not in r.inner_text()]
            
            print(f"[Debug] Found {len(rows)} potential table rows on this page.")
            
            for i, row in enumerate(rows):
                
                # 不再滚动（已确认会干扰 Vue 状态）

                # Capture Base Info
                base_info = {}
                try:
                    cols = row.locator("td").all()
                    if len(cols) >= 3:
                        # 0序号, 1编号, 2企业名称
                        lic_text = cols[1].inner_text().strip().replace(" ", "").replace("\t", "").replace("\n", "")
                        base_info['licenseNum'] = lic_text
                        
                        ent_name = cols[2].inner_text().strip().replace(" ", "").replace("\t", "").replace("\n", "")
                        base_info['entName'] = ent_name
                        
                        # DYNAMIC KEYWORD HARVESTING (User Request)
                        # Extract the regulator identifier (e.g. 京朝食药监械经营备案)
                        # Harvesting happens for ALL rows, even duplicates, to build the full discovery map.
                        if lic_text:
                            # 🔧 改进正则：匹配到括号或数字就停止
                            # 例如：银审服械备字〈2020〉 → 银审服械备字
                            prefix_match = re.search(r'^([^0-9()（）〈〉﹝﹞\[\]【】<>《》]+)', lic_text)
                            if prefix_match:
                                prefix = prefix_match.group(1).strip()
                                # 🔧 FIX: 去除所有空格 (例如 "粤江 食药监械经营备" -> "粤江食药监械经营备")
                                # 🔧 FIX: 去除偶尔出现的 "备案号：" 前缀 (爬虫有时会误把标签抓进来)
                                prefix = prefix.replace(" ", "").replace("\t", "").replace("\n", "") \
                                               .replace("备案号", "").replace("：", "").replace(":", "")
                                
                                # 去掉可能残留的年份部分
                                prefix = re.split(r'20\d\d|20[012]\d', prefix)[0].strip()
                                # 🔧 最终验证：长度>1 且 不含特殊字符
                                if len(prefix) > 1 and prefix.replace('药监械经营备', '').replace('食', '').replace('市监械经营备', ''):
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
                            # 🔧 FIX: 不再用硬件鼠标模拟 —— 已确认会导致详情页白屏
                            # 也不再用 scroll_into_view —— 已确认会干扰 Vue 状态
                            with self.context.expect_page(timeout=10000) as new_page_info:
                                btn.click(force=True)
                            detail_page = new_page_info.value
                            
                            if not detail_page and len(self.context.pages) > initial_page_count:
                                detail_page = self.context.pages[-1]

                            if detail_page:
                                try:
                                    detail_page.bring_to_front()
                                    detail_page.wait_for_load_state("domcontentloaded")
                                    
                                    # 🔧 FIX: 柔性等待，模仿测试脚本的前置缓冲
                                    time.sleep(2.0)
                                    
                                    # Content Polling (渐进式重试 10秒)
                                    data_ready = False
                                    for _ in range(20):
                                        try:
                                            # 使用更严格的数据判定（只看<tr>数量不够，需要检查是否有实质文本）
                                            if detail_page.locator("tr").count() > 5:
                                                # 🔧 响应架构师审计：采用“白名单特征确认”替代“黑名单排除法”
                                                has_val = detail_page.evaluate("""() => {
                                                    const rows = Array.from(document.querySelectorAll('tr'));
                                                    for (let row of rows) {
                                                        const cells = row.querySelectorAll('td');
                                                        if (cells.length >= 2) {
                                                            const label = cells[0].innerText.trim();
                                                            const value = cells[1].innerText.trim();
                                                            // 🔧 FIX: 增加脏数据过滤，防止 Vue 吐出 "无" 或 "******" 被当做正常数据
                                                            if ((label.includes("名称") || label.includes("代表人") || label.includes("范围") || label.includes("方式") || label.includes("部门")) && value.length > 2) {
                                                                // 排除无意义的高频空值
                                                                if (!value.includes("无") && !value.includes("***")) {
                                                                    return true;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    return false;
                                                }""")
                                                if has_val: data_ready = True; break
                                        except: pass
                                        time.sleep(0.5)
                                    
                                    # Persistent Reload Strategy -> Changed to "Close & Re-Click"
                                    reload_attempts = 0
                                    while not data_ready and reload_attempts < 7:
                                        reload_attempts += 1
                                        
                                        # Tell the brain we failed (延迟到第 3 次连败才判定为真・封锁)
                                        if reload_attempts == 3: self.limiter.record_block()
                                        
                                        # 🔧 FIX: 恢复用户要求的原版长时惩罚（才能越过防火墙拦截期）
                                        wait_time = self.limiter.get_backoff_wait(reload_attempts)
                                        print(f"[SmartLimiter] BLANK page! Penalty Base: {self.limiter.current_base:.1f}s. Waiting {wait_time:.1f}s (Attempt {reload_attempts}/7)...")
                                        
                                        try: detail_page.close()
                                        except: pass
                                        time.sleep(wait_time)
                                        
                                        try:
                                            # 用回原来的按钮去点击
                                            print(f"[Scraper] Re-clicking details button...")
                                            with self.context.expect_page(timeout=10000) as re_page_info:
                                                btn.click(force=True)
                                            detail_page = re_page_info.value
                                            detail_page.bring_to_front()
                                            detail_page.wait_for_load_state("domcontentloaded")
                                            time.sleep(1.0) # 缩减重新点开的无谓等待

                                            
                                            # Re-check data (渐进式重试 10秒)
                                            for _ in range(20):
                                                if detail_page.locator("tr").count() > 5:
                                                    has_val = detail_page.evaluate("""() => {
                                                        const rows = Array.from(document.querySelectorAll('tr'));
                                                        for (let row of rows) {
                                                            const cells = row.querySelectorAll('td');
                                                            if (cells.length >= 2) {
                                                                const label = cells[0].innerText.trim();
                                                                const value = cells[1].innerText.trim();
                                                                if ((label.includes("名称") || label.includes("代表人") || label.includes("范围") || label.includes("方式") || label.includes("部门")) && value.length > 2) {
                                                                    if (!value.includes("无") && !value.includes("***")) {
                                                                        return true;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        return false;
                                                    }""")
                                                    if has_val: data_ready = True; break
                                                time.sleep(0.5)
                                        except Exception as e_rel:
                                            print(f"[Warning] Re-click attempt {reload_attempts} failed: {e_rel}")

                                    if not data_ready:
                                        print(f"[CRITICAL] Still BLANK after {reload_attempts} attempts (~10 mins).")
                                        self._log_failure(base_info, "Blank Page / IP Block")
                                        self.save_failure_artifacts(detail_page, f"Ban_{base_info.get('entName', 'Unknown')}")
                                        raise Exception("ABORT: Consistent blank pages detected. Please check IP status or website availability.")


                                    # Extraction
                                    detail_item = self._extract_detail_fields(detail_page)
                                    
                                    if not detail_item:
                                        print(f"[Scraper] Extraction failed (Incomplete data) for: {base_info.get('entName')}. Fast Retrying...")
                                        try: detail_page.close()
                                        except: pass
                                        time.sleep(1.0) # 仅需短暂缓冲，快速重开刷新 Vue 状态
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
                                    
                                    # Double Check: Ensure we have a REAL detail payload (Defend against silent WAF packet drop)
                                    # 要求至少存在法人、负责人或经营方式中任意一项，且非极短无效字符
                                    def is_valid(val): return bool(val and len(str(val).strip()) > 1 and str(val).strip() not in ("无", "***", "暂无", "空"))
                                    
                                    if is_valid(final_item.get('legalRep')) or is_valid(final_item.get('resPerson')) or is_valid(final_item.get('opMode')):
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
                                        print(f"[Warning] Record for {final_item['entName']} filtered out: Empty Detail Fields (Zero Payload or Invalid).")
                                        self._log_failure(base_info, "Empty Detail payload dropped")
                                    
                                    # 🔧 FIX: 拉长拟人化休眠，增大方差防脚本检测
                                    sleep_time = random.uniform(2.5, 4.5)
                                    print(f"[BurstScraping] Resting for {sleep_time:.2f}s...")
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
                            # 🚨 致命修复：如果是 IP 封锁引起的持续白屏（ABORT异常），必须向上抛出
                            if "ABORT:" in str(e):
                                raise e
                            time.sleep(2)
                    
                    if not detail_success and base_info.get('entName'):
                        print(f"[Scraper] Failed details for: {base_info['entName']}. Item will NOT be saved to avoid ghost records.")
                        self._log_failure(base_info, "Extraction Failed / Closed unexpectedly")
# -------------------------------------------------------------
        except Exception as e:
            print(f"[Scraper] detail loop failure: {e}")
            # 🚨 致命修复：如果是 IP 封锁引起的持续白屏（ABORT异常），必须向上抛出，阻断整个爬虫！
            if "ABORT:" in str(e):
                raise e
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
                    "经营方式": "opMode", "经营范围": "scope", 
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

                            # 🧹 数据清洗：过滤无效的地址 (新增请求)
                            # 已移除 warehouseAddr (库房地址)，不再采集
                            if field_key in ("entAddress", "opAddress"):
                                invalid_addr = {"无", "无此项", "暂无", "无数据", "不适用", "未填写", "-", "/", "//", "\\", "——", "—", ".", "null", "NULL", "N/A", "n/a"}
                                if val in invalid_addr:
                                    val = ""
                                # 地址如果全是星号，或者包含 "无" 且长度极短 (<4)
                                elif set(val) == {'*'} or ( "*" in val and len(val) < 5 ): 
                                    val = ""
                                elif "无" in val and len(val) < 4 and "市" not in val and "县" not in val:
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
        # 🔧 FIX: 不再吞掉异常，让调用者(main.py)知道网络中断等错误
        # try:
        print("[Scraper] Attempting to go to next page...")
        
        # Element UI standard pagination "Next" button
        # We must ignore if it has 'disabled' attribute or class
        next_btn = self.page.locator("button.btn-next").first
        
        if next_btn.count() > 0:
            if next_btn.is_disabled() or "disabled" in next_btn.get_attribute("class"):
                print("[Scraper] Next button is disabled. End of list.")
                return False
            
            # 🔧 FIX: 三重点击保障，防止超时崩溃
            # 第1层：正常点击 (5s)
            # 第2层：强制点击 (5s)  
            # 第3层：JS直接触发点击事件
            click_success = False
            try:
                next_btn.click(timeout=5000)
                click_success = True
            except Exception as e1:
                print(f"[Scraper] Normal click failed. Trying FORCE CLICK... ({str(e1)[:50]})")
                try:
                    next_btn.click(force=True, timeout=5000)
                    click_success = True
                except Exception as e2:
                    print(f"[Scraper] Force click also failed. Trying JS CLICK... ({str(e2)[:50]})")
                    try:
                        next_btn.evaluate("el => el.click()")
                        click_success = True
                    except Exception as e3:
                        print(f"[Scraper] All 3 click methods failed! ({str(e3)[:50]})")
                        raise e3  # 三种都失败，说明是真正的网络/浏览器问题
            
            if click_success:
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
        # except Exception as e: 
        #     print(f"[Scraper] Pagination error: {e}")
        #     # 🔧 如果是超时或网络错误，应该抛出异常，而不是返回False(认为跑完)
        #     raise e

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
