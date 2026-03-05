import pymysql
import config
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, TABLE_NAME, DB_PORT
try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = None

class Storage:
    def __init__(self):
        self.tunnel = None
        self.conn = self._get_connection(init=True)

    def _ensure_tunnel_alive(self):
        """检查SSH隧道是否存活，如果断开则重连（带重试机制）"""
        import time as _time
        
        if not getattr(config, 'USE_SSH', False):
            return  # 不使用SSH隧道
        
        if self.tunnel is None or not self.tunnel.is_active:
            print("[Storage] ⚠️ SSH Tunnel disconnected! Reconnecting...")
            # 关闭旧隧道（如果存在）
            if self.tunnel:
                try:
                    self.tunnel.stop()
                except:
                    pass
            self.tunnel = None
            
            # 🔧 FIX: 重试机制 —— 最多尝试3次，间隔递增（10s, 30s, 60s）
            retry_delays = [10, 30, 60]
            last_error = None
            for attempt, delay in enumerate(retry_delays, 1):
                try:
                    self.conn = self._get_connection(init=False)
                    print(f"[Storage] ✅ SSH Tunnel reconnected! (attempt {attempt})")
                    return  # 成功，直接返回
                except Exception as e:
                    last_error = e
                    print(f"[Storage] ❌ Reconnect attempt {attempt}/{len(retry_delays)} failed: {e}")
                    if attempt < len(retry_delays):
                        print(f"[Storage] ⏳ Waiting {delay}s before retrying...")
                        _time.sleep(delay)
            
            # 三次都失败，抛出异常
            raise Exception(f"SSH Tunnel reconnection failed after {len(retry_delays)} attempts: {last_error}")

    def _get_connection(self, init=False):
        # 1. Handle SSH Tunneling
        db_host = DB_HOST
        db_port = DB_PORT
        
        if getattr(config, 'USE_SSH', False):
            if not self.tunnel or not self.tunnel.is_active:
                print(f"[Storage] Opening SSH Tunnel to {config.SSH_HOST}:{config.SSH_PORT}...")
                self.tunnel = SSHTunnelForwarder(
                    (config.SSH_HOST, config.SSH_PORT),
                    ssh_username=config.SSH_USER,
                    ssh_password=config.SSH_PASSWORD,
                    remote_bind_address=('127.0.0.1', 3306),
                    local_bind_address=('127.0.0.1', 0) # Use random local port
                )
                self.tunnel.start()
            
            # Use the local port mapped by the tunnel
            db_host = self.tunnel.local_bind_host
            db_port = self.tunnel.local_bind_port
            print(f"[Storage] Tunnel active. Local endpoint: {db_host}:{db_port}")

        # 2. Database Connection
        # First connect without DB to create it if needed
        if init:
            conn = pymysql.connect(
                host=db_host,
                user=DB_USER,
                password=DB_PASSWORD,
                port=db_port,
                charset='utf8mb4'
            )
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.close()

        return pymysql.connect(
            host=db_host,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=db_port,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def init_db(self):
        with self.conn.cursor() as cursor:
            # User requested specific order:
            # enterprise_name, legal_representative, actual_controller, responsible_person, operation_mode, scope, address, 
            # operation_address, warehouse_address, filing_department, license_number, filing_date
            sql = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_name VARCHAR(255) NOT NULL,
                legal_representative VARCHAR(100),
                actual_controller VARCHAR(100),
                responsible_person VARCHAR(100),
                contact_phone VARCHAR(50),
                operation_mode VARCHAR(255),
                scope TEXT,
                address TEXT,
                operation_address TEXT,
                filing_department VARCHAR(255),
                license_number VARCHAR(255),
                filing_date VARCHAR(50),
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                has_online TINYINT(1) DEFAULT 0 COMMENT '是否有网店',
                shop_type TINYINT(1) DEFAULT 0 COMMENT '0:未确定, 1:品牌自营, 2:综合型',
                record_number VARCHAR(100) DEFAULT NULL COMMENT '网销备案号',
                shop_name TEXT DEFAULT NULL COMMENT '平台及店铺名,格式:[平台]店名',
                stage TINYINT(1) DEFAULT 0 COMMENT '0:待处理, 1:跟进中, 2:审核中, 3:已成交, 4:搁置, 5:无效',
                owner_id INT DEFAULT 0 COMMENT '当前跟进人',
                t_last DATE DEFAULT NULL COMMENT '最后联系日期',
                t_next DATE DEFAULT NULL COMMENT '计划回访日期',
                UNIQUE KEY uq_enterprise_name (enterprise_name),
                INDEX idx_stage (stage)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(sql)
            self.conn.commit()
            print(f"[Storage] Table '{TABLE_NAME}' checked.")

    def save_batch(self, data_list):
        if not data_list:
            return 0
        
        # 🔧 确保SSH隧道存活
        self._ensure_tunnel_alive()
        
        inserted_count = 0
        try:
            self.conn.ping(reconnect=True)
        except:
            self.conn = self._get_connection()

        with self.conn.cursor() as cursor:
            sql = f"""
            INSERT INTO {TABLE_NAME} 
            (enterprise_name, legal_representative, actual_controller, responsible_person, contact_phone, operation_mode, scope, address, operation_address, filing_department, license_number, filing_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            enterprise_name = VALUES(enterprise_name),
            legal_representative = VALUES(legal_representative),
            actual_controller = VALUES(actual_controller),
            responsible_person = VALUES(responsible_person),
            contact_phone = VALUES(contact_phone),
            operation_mode = VALUES(operation_mode),
            scope = VALUES(scope),
            address = VALUES(address),
            operation_address = VALUES(operation_address),
            filing_department = VALUES(filing_department),
            license_number = VALUES(license_number),
            filing_date = VALUES(filing_date),
            crawled_at = CURRENT_TIMESTAMP
            """
            
            values = []
            for item in data_list:
                # Helper to convert empty strings to None (NULL) for cleaner DB
                def get_val(key):
                    val = item.get(key, '').strip()
                    return val if val else None

                values.append((
                    get_val('entName') or get_val('enterprise_name'),
                    get_val('legalRep') or get_val('legal_representative'),
                    get_val('actualController') or get_val('actual_controller'),
                    get_val('resPerson') or get_val('responsible_person'),
                    get_val('contactPhone') or get_val('contact_phone'),
                    get_val('opMode') or get_val('operation_mode'),
                    get_val('scope'),
                    get_val('entAddress') or get_val('address'),
                    get_val('opAddress') or get_val('operation_address'),
                    get_val('filingDept') or get_val('filing_department'),
                    get_val('licenseNum') or get_val('license_number'),
                    get_val('filingDate') or get_val('filing_date')
                ))
            
            cursor.executemany(sql, values)
            self.conn.commit()
            inserted_count = cursor.rowcount
            
        print(f"[Storage] Batch saved (Rows affected: {inserted_count}). Updates count as 2.")
        return inserted_count

    def delete_by_name(self, enterprise_name):
        """Delete a record by enterprise name (for replacing truncated names)."""
        try:
            with self.conn.cursor() as cursor:
                sql = f"DELETE FROM {TABLE_NAME} WHERE enterprise_name = %s"
                cursor.execute(sql, (enterprise_name,))
                self.conn.commit()
                return cursor.rowcount
        except Exception as e:
            print(f"[Storage] Error deleting record: {e}")
            return 0


    def get_existing_records(self):
        """Fetch only COMPLETE (license_num, ent_name) pairs for deduplication using Streaming Cursor."""
        # 🔧 FIX: Use SSCursor (Server Side Cursor) to prevent "read_bytes" hang on large datasets over SSH
        # This streams rows one by one instead of loading all 50k+ rows into RAM/Network buffer at once
        existing = set()
        try:
            # Create a new connection specifically for this streaming operation to avoid cursor conflicts
            # We use the internal _get_connection logic but ensure it's a fresh handle
            stream_conn = self._get_connection()
            
            with stream_conn.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute(f"SELECT license_number, enterprise_name FROM {TABLE_NAME}")
                
                while True:
                    row = cursor.fetchone() # Stream one row
                    if not row: break
                    
                    # SSCursor returns tuple (lic, name) because we selected 2 columns
                    # Note: raw pymysql SSCursor returns tuples, not dicts unless configured otherwise
                    # Let's handle both just in case, but usually tuple is faster for this
                    
                    lic = row[0] if row and len(row) > 0 else ''
                    name = row[1] if row and len(row) > 1 else ''
                    
                    if lic or name:
                        clean_lic = lic.replace(" ", "").replace("\t", "").replace("\n", "") if lic else ''
                        clean_name = name.replace(" ", "").replace("\t", "").replace("\n", "") if name else ''
                        existing.add((clean_lic, clean_name))
            
            stream_conn.close()
            return existing
            
        except Exception as e:
            print(f"[Storage] Error fetching existing records: {e}")
            return set()

    def get_empty_records(self):
        """
        Fetch records that have incomplete data (Candidate for Re-scrape).
        Includes: 1) Empty detail fields, 2) Truncated names (ending with '...')
        """
        with self.conn.cursor() as cursor:
            # Query 1: Empty fields
            # 🔧 Updated: Use `scope` and `address` as critical fields.
            # Legal Rep and Responsible Person are often legitimately NULL after cleaning.
            # But `scope` and `address` are almost always present for valid records.
            sql_empty = f"""
            SELECT enterprise_name 
            FROM {TABLE_NAME} 
            WHERE ((scope IS NULL OR scope = '') 
               AND (address IS NULL OR address = ''))
               AND enterprise_name IS NOT NULL 
               AND enterprise_name != ''
            """
            
            # Query 2: Truncated names
            sql_truncated = f"""
            SELECT enterprise_name 
            FROM {TABLE_NAME} 
            WHERE enterprise_name LIKE '%...'
            """
            
            cursor.execute(sql_empty)
            empty_names = [row['enterprise_name'] for row in cursor.fetchall()]
            
            cursor.execute(sql_truncated)
            truncated_names = [row['enterprise_name'] for row in cursor.fetchall()]
            
            # Combine and deduplicate
            all_broken = list(set(empty_names + truncated_names))
            
            # Always show result (even if 0) for user visibility
            if all_broken:
                print(f"[Storage] Found {len(empty_names)} empty + {len(truncated_names)} truncated = {len(all_broken)} total to repair")
            else:
                print(f"[Storage] ✅ No incomplete records found. Database is clean!")
            
            return all_broken

    def close(self):
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        if hasattr(self, 'tunnel') and self.tunnel:
            self.tunnel.stop()
            print("[Storage] SSH Tunnel closed.")
