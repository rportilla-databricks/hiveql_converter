#!/usr/bin/env python3
"""
Generate sample data for Trino → Databricks conversion testing.
Creates tables needed for the Trino test queries in Unity Catalog.
"""

import os
import configparser
from pathlib import Path
from typing import Tuple
from databricks import sql

# ANSI color codes
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def read_databricks_config(profile: str = 'DEFAULT') -> Tuple[str, str]:
    """Read Databricks configuration from .databrickscfg file."""
    config_path = Path.home() / '.databrickscfg'
    config = configparser.ConfigParser()
    config.read(config_path)
    
    host = config[profile]['host'].strip()
    token = config[profile]['token'].strip()
    
    return host, token


def execute_sql(cursor, sql: str, description: str):
    """Execute SQL with error handling and feedback."""
    try:
        print(f"  {Colors.YELLOW}▸ {description}...{Colors.RESET}", end=" ")
        cursor.execute(sql)
        print(f"{Colors.GREEN}✓{Colors.RESET}")
        return True
    except Exception as e:
        print(f"{Colors.RED}✗ Error: {str(e)[:100]}{Colors.RESET}")
        return False


def create_sample_tables(cursor):
    """Create all necessary sample tables for Trino test queries."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}Creating Sample Tables{Colors.RESET}")
    print("-" * 80)
    
    # Drop existing tables first
    tables = ['orders', 'user_data', 'customer_purchases']
    
    print(f"  {Colors.YELLOW}Cleaning up existing tables...{Colors.RESET}")
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        except:
            pass
    print(f"  {Colors.GREEN}✓ Cleanup complete{Colors.RESET}\n")
    
    # 1. orders table (for script0 and script2)
    execute_sql(cursor, """
        CREATE TABLE orders (
            order_id STRING,
            order_date DATE,
            customer_id STRING,
            total_amount DECIMAL(10,2),
            items ARRAY<STRUCT<product_id: STRING, quantity: INT, price: DECIMAL(10,2)>>
        ) USING ICEBERG
    """, "Creating orders")
    
    # 2. user_data table (for script1)
    execute_sql(cursor, """
        CREATE TABLE user_data (
            user_id STRING,
            user_profile STRING
        ) USING ICEBERG
    """, "Creating user_data")
    
    # 3. customer_purchases table (for script3)
    execute_sql(cursor, """
        CREATE TABLE customer_purchases (
            customer_id STRING,
            product_id STRING,
            purchase_amounts ARRAY<DECIMAL(10,2)>
        ) USING ICEBERG
    """, "Creating customer_purchases")


def insert_sample_data(cursor):
    """Insert sample data into all tables."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}Inserting Sample Data{Colors.RESET}")
    print("-" * 80)
    
    # 1. Insert orders
    execute_sql(cursor, """
        INSERT INTO orders VALUES
        ('ORD001', '2024-10-15', 'CUST001', 299.99, array(struct('PROD001', 2, 149.99), struct('PROD002', 1, 0.01))),
        ('ORD002', '2024-10-18', 'CUST002', 599.50, array(struct('PROD003', 1, 599.50))),
        ('ORD003', '2024-10-20', 'CUST001', 1250.00, array(struct('PROD001', 5, 149.99), struct('PROD004', 2, 125.01))),
        ('ORD004', '2024-09-15', 'CUST003', 89.99, array(struct('PROD005', 3, 29.99))),
        ('ORD005', '2024-09-20', 'CUST002', 450.00, array(struct('PROD006', 1, 450.00))),
        ('ORD006', '2024-08-10', 'CUST004', 199.99, array(struct('PROD007', 1, 199.99))),
        ('ORD007', '2024-11-01', 'CUST005', 750.00, array(struct('PROD001', 3, 149.99), struct('PROD008', 2, 75.01))),
        ('ORD008', '2024-11-03', 'CUST001', 320.50, array(struct('PROD002', 10, 32.05)))
    """, "Inserting into orders (8 rows)")
    
    # 2. Insert user_data with JSON profiles
    execute_sql(cursor, """
        INSERT INTO user_data VALUES
        ('USER001', '{"name": "John Smith", "address": {"city": "New York", "state": "NY"}, "tags": ["premium", "frequent"]}'),
        ('USER002', '{"name": "Jane Doe", "address": {"city": "Los Angeles", "state": "CA"}, "tags": ["new", "trial"]}'),
        ('USER003', '{"name": "Bob Johnson", "address": {"city": "Chicago", "state": "IL"}, "tags": ["premium", "vip", "longtime"]}'),
        ('USER004', '{"name": "Alice Williams", "address": {"city": "Houston", "state": "TX"}, "tags": ["regular"]}'),
        ('USER005', '{"name": "Charlie Brown", "address": {"city": "Phoenix", "state": "AZ"}, "tags": ["new", "referral"]}}')
    """, "Inserting into user_data (5 rows)")
    
    # 3. Insert customer_purchases
    execute_sql(cursor, """
        INSERT INTO customer_purchases VALUES
        ('CUST001', 'PROD001', array(50.00, 75.50, 120.00, 200.00)),
        ('CUST001', 'PROD002', array(25.99, 30.00, 45.50)),
        ('CUST002', 'PROD003', array(150.00, 200.00, 250.00, 300.00, 180.00)),
        ('CUST003', 'PROD004', array(99.99, 110.00, 95.50)),
        ('CUST003', 'PROD005', array(20.00, 25.00, 30.00, 22.50)),
        ('CUST004', 'PROD001', array(200.00, 180.00, 220.00, 195.00, 210.00)),
        ('CUST005', 'PROD006', array(500.00, 450.00, 550.00))
    """, "Inserting into customer_purchases (7 rows)")


def main():
    """Main execution function."""
    DATABRICKS_PROFILE = os.getenv('DATABRICKS_PROFILE', 'fe')
    WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')
    SCHEMA = 'hql_test'
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Sample Data Generator for Trino Migration Testing{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    
    if not WAREHOUSE_ID:
        print(f"\n{Colors.RED}✗ DATABRICKS_WAREHOUSE_ID environment variable not set{Colors.RESET}")
        print(f"{Colors.YELLOW}Set it with: export DATABRICKS_WAREHOUSE_ID=your_warehouse_id{Colors.RESET}")
        return
    
    # Read config
    try:
        host, token = read_databricks_config(DATABRICKS_PROFILE)
        print(f"\n{Colors.GREEN}✓ Databricks config loaded{Colors.RESET}")
        print(f"  Host: {host}")
    except Exception as e:
        print(f"\n{Colors.RED}✗ Failed to read config: {e}{Colors.RESET}")
        return
    
    # Connect
    connection = None
    try:
        print(f"\n{Colors.YELLOW}Connecting to Databricks SQL...{Colors.RESET}")
        server_hostname = host.replace('https://', '')
        http_path = f'/sql/1.0/warehouses/{WAREHOUSE_ID}'
        
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=token
        )
        print(f"{Colors.GREEN}✓ Connected successfully{Colors.RESET}")
        
        with connection.cursor() as cursor:
            # Use dedicated schema
            cursor.execute(f'USE SCHEMA {SCHEMA}')
            print(f"{Colors.CYAN}Using schema: {SCHEMA}{Colors.RESET}\n")
            
            # Create tables
            create_sample_tables(cursor)
            
            # Insert data
            insert_sample_data(cursor)
            
            # Verify counts
            print(f"\n{Colors.CYAN}{Colors.BOLD}Verifying Data{Colors.RESET}")
            print("-" * 80)
            
            tables = ['orders', 'user_data', 'customer_purchases']
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {Colors.GREEN}✓{Colors.RESET} {table}: {count} rows")
        
        print(f"\n{Colors.GREEN}{Colors.BOLD}{'='*80}{Colors.RESET}")
        print(f"{Colors.GREEN}{Colors.BOLD}Sample data created successfully!{Colors.RESET}")
        print(f"{Colors.GREEN}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
        
    except Exception as e:
        print(f"{Colors.RED}✗ Error: {e}{Colors.RESET}")
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    main()

