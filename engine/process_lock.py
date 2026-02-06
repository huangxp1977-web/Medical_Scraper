"""
Process Lock Utility - Prevents Multiple Instances

使用文件锁确保同一时间只有一个采集器实例运行
"""
import os
import sys
import time

class ProcessLock:
    def __init__(self, lock_file="resources/scraper.lock"):
        self.lock_file = lock_file
        self.acquired = False
        
    def acquire(self):
        """尝试获取锁"""
        if os.path.exists(self.lock_file):
            # 检查锁文件的创建时间
            lock_time = os.path.getmtime(self.lock_file)
            age = time.time() - lock_time
            
            # 如果锁文件超过 2 小时，认为是僵尸锁（程序异常退出）
            if age > 7200:
                print(f"[ProcessLock] Found stale lock file (age: {age:.0f}s). Removing...")
                try:
                    os.remove(self.lock_file)
                except:
                    pass
            else:
                print(f"[ERROR] Another scraper instance is already running!")
                print(f"[ERROR] Lock file: {os.path.abspath(self.lock_file)}")
                print(f"[ERROR] If you're sure no other instance is running, delete the lock file manually.")
                return False
        
        # 创建锁文件
        try:
            os.makedirs(os.path.dirname(self.lock_file), exist_ok=True)
            with open(self.lock_file, 'w') as f:
                f.write(f"PID: {os.getpid()}\n")
                f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            self.acquired = True
            print(f"[ProcessLock] Lock acquired: {self.lock_file}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to create lock file: {e}")
            return False
    
    def release(self):
        """释放锁"""
        if self.acquired and os.path.exists(self.lock_file):
            try:
                os.remove(self.lock_file)
                print(f"[ProcessLock] Lock released: {self.lock_file}")
            except Exception as e:
                print(f"[Warning] Failed to remove lock file: {e}")
            self.acquired = False
    
    def __enter__(self):
        if not self.acquire():
            sys.exit(1)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
