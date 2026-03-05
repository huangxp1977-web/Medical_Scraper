import time
import random
import json
import os

class SmartRateLimiter:
    def __init__(self, default_base=2.5, min_base=2.2, max_base=120, penalty_add=20, recovery_step=1, log_path="logs/scraper_behavior.jsonl", aggressive_recovery=False):
        """
        Smart Adaptive Rate Limiter (PID-like Control).
        
        Args:
            aggressive_recovery: 🧪 EXPERIMENTAL MODE
                - False (default): Gradual recovery (每10次成功减少1-5秒)  
                - True: Aggressive recovery (连续成功3次后立即恢复到默认速度)
        """
        self.default_base = default_base
        self.current_base = default_base
        self.min_base = min_base
        self.max_base = max_base
        self.penalty_add = penalty_add
        self.recovery_step = recovery_step
        self.log_path = log_path
        
        # 🧪 Experimental Feature
        self.aggressive_recovery = aggressive_recovery
        
        # State
        self.consecutive_success = 0
        self.total_requests = 0
        self.last_adjustment_time = time.time()
        self.blocks_today = 0
        
        # Ensure log header
        self._log({
            "event": "session_start", 
            "config": {
                "default": default_base, 
                "penalty": penalty_add,
                "aggressive_recovery": aggressive_recovery  # Track experiment mode
            }
        })

    def _log(self, data):
        """Append structured log for future AI analysis."""
        try:
            entry = {
                "timestamp": int(time.time()),
                "iso_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                **data
            }
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except: pass

    def get_delay(self):
        """
        Returns a randomized delay based on the current stress level (current_base).
        Never returns an integer to avoid ML pattern detection.
        """
        # Jitter: +/- 10%
        jitter = random.uniform(0.9, 1.1)
        actual_delay = self.current_base * jitter
        
        return actual_delay

    def record_success(self):
        """
        Call this when a page loads successfully.
        """
        self.consecutive_success += 1
        self.total_requests += 1
        
        # Log basic heartbeat every 10 requests to keep file size manageable
        if self.total_requests % 10 == 0:
            self._log({
                "event": "heartbeat", 
                "total_req": self.total_requests, 
                "current_base": round(self.current_base, 2),
                "streak": self.consecutive_success,
                "mode": "AGGRESSIVE" if self.aggressive_recovery else "GRADUAL"
            })
        
        # 🧪 EXPERIMENTAL: Aggressive Recovery Mode
        if self.aggressive_recovery:
            # 快速恢复策略：连续成功10次后立即重置到默认速度
            if self.current_base > self.default_base and self.consecutive_success >= 10:
                old_base = self.current_base
                self.current_base = self.default_base
                print(f"[🧪 Aggressive Recovery] 10 wins in a row! INSTANT reset: {old_base:.2f}s → {self.current_base:.2f}s")
                
                self._log({
                    "event": "AGGRESSIVE_RESET", 
                    "streak": self.consecutive_success,
                    "old_base": round(old_base, 2),
                    "new_base": round(self.current_base, 2)
                })
            
            # 🚫 禁用额外加速 - 2.2秒会触发Block，保持2.5秒更安全
            # if self.current_base <= self.default_base and self.consecutive_success > 10 and self.consecutive_success % 5 == 0:
            #     new_target = max(self.min_base, self.current_base - 0.3)
            #     if new_target < self.current_base:
            #         old_base = self.current_base
            #         self.current_base = new_target
            #         print(f"[🧪 Aggressive Speedup] Turbo mode! {old_base:.2f}s → {self.current_base:.2f}s")
            #         self._log({
            #             "event": "aggressive_speedup", 
            #             "new_base": round(self.current_base, 2)
            #         })
        
        else:
            # 原有的渐进式恢复逻辑（Gradual Recovery）
            # 1. Elastic Recovery (Accelerated Healing)
            if self.current_base > self.default_base and self.consecutive_success % 10 == 0:
                old_base = self.current_base
                
                # Dynamic Step: The longer we are safe, the bolder we get.
                multiplier = 1
                if self.consecutive_success >= 50:
                    multiplier = 5 # Aggressive healing
                elif self.consecutive_success >= 20:
                    multiplier = 2 # Moderate healing
                    
                current_step = self.recovery_step * multiplier
                
                self.current_base = max(self.default_base, self.current_base - current_step)
                print(f"[RateLimiter] Elastic Recovery (Streak {self.consecutive_success}): Reducing base wait by {current_step}s ({multiplier}x speed) to {self.current_base:.2f}s")
                
                self._log({
                    "event": "recovery", 
                    "streak": self.consecutive_success,
                    "step_size": current_step,
                    "old_base": round(old_base, 2),
                    "new_base": round(self.current_base, 2)
                })

            # 2. Probing (Speed Up)
            if self.current_base <= self.default_base and self.consecutive_success > 50 and self.consecutive_success % 20 == 0:
                new_target = max(self.min_base, self.current_base - 0.5)
                if new_target < self.current_base:
                    self.current_base = new_target
                    print(f"[RateLimiter] Speed Probe: Accelerating! New base wait: {self.current_base:.2f}s")
                    self._log({
                        "event": "speed_up", 
                        "new_base": round(self.current_base, 2)
                    })

    def record_block(self):
        """
        Call this when a BLANK page is detected.
        """
        self.consecutive_success = 0
        self.blocks_today += 1
        
        old_base = self.current_base
        # Penalty: Add seconds (首犯直接拉到 30秒，后续以 penalty_add 步进)
        if self.current_base < 30.0:
            self.current_base = 30.0
        else:
            self.current_base = min(self.max_base, self.current_base + self.penalty_add)
        
        mode_label = "🧪 AGGRESSIVE" if self.aggressive_recovery else "GRADUAL"
        print(f"[RateLimiter {mode_label}] BLOCK DETECTED! Penalty applied.")
        print(f"[RateLimiter] Adjustment: {old_base:.2f}s -> {self.current_base:.2f}s")
        
        self._log({
            "event": "BLOCK", 
            "old_base": round(old_base, 2),
            "new_base": round(self.current_base, 2),
            "total_blocks": self.blocks_today,
            "mode": "AGGRESSIVE" if self.aggressive_recovery else "GRADUAL"
        })
        
    def get_backoff_wait(self, attempt):
        """
        Calculates the specific wait time for the persistent retry loop.
        Formula: CurrentBase + (Attempt * 10) + Jitter
        """
        # We use the current_base as the starting point, so if we are punished, retry waits are also longer.
        base = self.current_base
        increment = attempt * 10
        jitter = random.uniform(0, 4)
        return base + increment + jitter
