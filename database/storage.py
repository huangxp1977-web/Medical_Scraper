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

    def _get_connection(self, init=False):
        # 1. Handle SSH Tunneling
        db_host = DB_HOST
        db_port = DB_PORT
        
        if getattr(config, 'USE_SSH', False):
            if not self.tunnel:
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
                warehouse_address TEXT,
                filing_department VARCHAR(255),
                license_number VARCHAR(255),
                filing_date VARCHAR(50),
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_name_license (enterprise_name, license_number)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(sql)
            self.conn.commit()
            print(f"[Storage] Table '{TABLE_NAME}' checked.")

    def save_batch(self, data_list):
        if not data_list:
            return 0
        
        inserted_count = 0
        try:
            self.conn.ping(reconnect=True)
        except:
            self.conn = self._get_connection()

        with self.conn.cursor() as cursor:
            sql = f"""
            INSERT INTO {TABLE_NAME} 
            (enterprise_name, legal_representative, actual_controller, responsible_person, contact_phone, operation_mode, scope, address, operation_address, warehouse_address, filing_department, license_number, filing_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            enterprise_name = VALUES(enterprise_name),
            legal_representative = VALUES(legal_representative),
            responsible_person = VALUES(responsible_person),
            operation_mode = VALUES(operation_mode),
            scope = VALUES(scope),
            address = VALUES(address),
            operation_address = VALUES(operation_address),
            warehouse_address = VALUES(warehouse_address),
            filing_department = VALUES(filing_department),
            filing_date = VALUES(filing_date),
            crawled_at = CURRENT_TIMESTAMP
            """
            
            values = []
            for item in data_list:
                values.append((
                    item.get('entName', '') or item.get('enterprise_name', ''),
                    item.get('legalRep', '') or item.get('legal_representative', ''),
                    item.get('actualController', '') or item.get('actual_controller', ''),
                    item.get('resPerson', '') or item.get('responsible_person', ''),
                    item.get('contactPhone', '') or item.get('contact_phone', ''),
                    item.get('opMode', '') or item.get('operation_mode', ''),
                    item.get('scope', ''),
                    item.get('entAddress', '') or item.get('address', ''),
                    item.get('opAddress', '') or item.get('operation_address', ''),
                    item.get('warehouseAddr', '') or item.get('warehouse_address', ''),
                    item.get('filingDept', '') or item.get('filing_department', ''),
                    item.get('licenseNum', '') or item.get('license_number', ''),
                    item.get('filingDate', '') or item.get('filing_date', '')
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
        """Fetch only COMPLETE (license_num, ent_name) pairs for deduplication."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT license_number, enterprise_name FROM {TABLE_NAME}")
            rows = cursor.fetchall()
            
            return {
                (
                    row['license_number'].strip() if row['license_number'] else '',
                    row['enterprise_name'].strip() if row['enterprise_name'] else ''
                ) 
                for row in rows if row['license_number'] or row['enterprise_name']
            }

    def get_empty_records(self):
        """
        Fetch records that have incomplete data (Candidate for Re-scrape).
        Includes: 1) Empty detail fields, 2) Truncated names (ending with '...')
        """
        with self.conn.cursor() as cursor:
            # Query 1: Empty fields
            sql_empty = f"""
            SELECT enterprise_name 
            FROM {TABLE_NAME} 
            WHERE ((legal_representative IS NULL OR legal_representative = '') 
               AND (responsible_person IS NULL OR responsible_person = ''))
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
