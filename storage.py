import pymysql
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, TABLE_NAME, DB_PORT

class Storage:
    def __init__(self):
        self.conn = self._get_connection(init=True)

    def _get_connection(self, init=False):
        # First connect without DB to create it if needed
        if init:
            conn = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                charset='utf8mb4'
            )
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.close()

        return pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def init_db(self):
        with self.conn.cursor() as cursor:
            # User requested specific order:
            # enterprise_name, legal_representative, responsible_person, operation_mode, scope, address, 
            # operation_address, warehouse_address, filing_department, license_number, filing_date
            sql = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_name VARCHAR(255) NOT NULL,
                legal_representative VARCHAR(100),
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
            print(f"[Storage] Table '{TABLE_NAME}' checked (Note: If column order didn't change, please DROP TABLE first).")

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
            (enterprise_name, legal_representative, responsible_person, contact_phone, operation_mode, scope, address, operation_address, warehouse_address, filing_department, license_number, filing_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
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
            
        print(f"[Storage] Saved {inserted_count} new records.")
        return inserted_count


    def get_existing_records(self):
        """Fetch only COMPLETE (license_num, ent_name) pairs for deduplication.
        If a record exists but is missing data (e.g. legal_representative), we don't skip it."""
        with self.conn.cursor() as cursor:
            # Only count as 'existing' if it actually has the mission-critical detail
            cursor.execute(f"SELECT license_number, enterprise_name FROM {TABLE_NAME} WHERE legal_representative IS NOT NULL AND legal_representative != ''")
            rows = cursor.fetchall()
            
            return {(row['license_number'], row['enterprise_name']) for row in rows if row['license_number'] or row['enterprise_name']}

    def close(self):
        if self.conn:
            self.conn.close()
