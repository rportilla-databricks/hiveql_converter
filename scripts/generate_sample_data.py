#!/usr/bin/env python3
"""
Generate sample data for HQL to Spark SQL validation.
Creates all necessary tables and inserts sample data using Databricks SQL.
"""

import os
import configparser
from pathlib import Path
from typing import Tuple
from databricks import sql
from datetime import datetime, timedelta
import random

# ANSI color codes
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
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
    """Create all necessary sample tables."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}Creating Sample Tables{Colors.RESET}")
    print("-" * 80)
    
    # Drop existing tables first to ensure clean schemas
    tables = [
        'raw_transactions', 'sales_fact', 'customers', 'products',
        'stores', 'raw_reviews', 'user_events', 'raw_event_stream'
    ]
    
    print(f"  {Colors.YELLOW}Cleaning up existing tables...{Colors.RESET}")
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        except:
            pass
    print(f"  {Colors.GREEN}✓ Cleanup complete{Colors.RESET}\n")
    
    # 1. raw_transactions (for scripts 0, 1)
    execute_sql(cursor, """
        CREATE TABLE raw_transactions (
            transaction_id STRING,
            customer_id STRING,
            transaction_date DATE,
            product_category STRING,
            amount DECIMAL(10,2),
            payment_method STRING,
            store_location STRING
        ) USING ICEBERG
    """, "Creating raw_transactions")
    
    # 2. sales_fact (for script 5)
    execute_sql(cursor, """
        CREATE TABLE sales_fact (
            transaction_id STRING,
            customer_id STRING,
            product_id STRING,
            store_id STRING,
            transaction_date DATE,
            transaction_time TIMESTAMP,
            quantity INT,
            unit_price DECIMAL(10,2),
            discount_percent DECIMAL(5,2),
            tax_amount DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            payment_method STRING,
            is_online BOOLEAN,
            shipping_cost DECIMAL(10,2)
        ) USING ICEBERG
    """, "Creating sales_fact")
    
    # 3. customers (for script 5)
    execute_sql(cursor, """
        CREATE TABLE customers (
            customer_id STRING,
            customer_name STRING,
            customer_segment STRING,
            customer_since_date DATE
        ) USING ICEBERG
    """, "Creating customers")
    
    # 4. products (for scripts 3, 5)
    execute_sql(cursor, """
        CREATE TABLE products (
            product_id STRING,
            product_name STRING,
            product_category STRING,
            product_subcategory STRING,
            brand STRING
        ) USING ICEBERG
    """, "Creating products")
    
    # 5. stores (for script 5)
    execute_sql(cursor, """
        CREATE TABLE stores (
            store_id STRING,
            store_name STRING,
            store_region STRING,
            store_size_category STRING
        ) USING ICEBERG
    """, "Creating stores")
    
    # 6. raw_reviews (for script 3)
    execute_sql(cursor, """
        CREATE TABLE raw_reviews (
            review_id STRING,
            product_id STRING,
            customer_id STRING,
            review_text STRING,
            rating INT,
            review_date DATE
        ) USING ICEBERG
    """, "Creating raw_reviews")
    
    # 7. user_events (for script 2)
    execute_sql(cursor, """
        CREATE TABLE user_events (
            event_id STRING,
            user_id STRING,
            page_url STRING,
            event_type STRING,
            event_timestamp TIMESTAMP,
            device_type STRING,
            event_date DATE
        ) USING ICEBERG
    """, "Creating user_events")
    
    # 8. raw_event_stream (for script 4)
    execute_sql(cursor, """
        CREATE TABLE raw_event_stream (
            event_id STRING,
            user_id STRING,
            event_timestamp TIMESTAMP,
            event_type STRING,
            event_payload STRING,
            event_date DATE
        ) USING ICEBERG
    """, "Creating raw_event_stream")


def insert_sample_data(cursor):
    """Insert sample data into all tables."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}Inserting Sample Data{Colors.RESET}")
    print("-" * 80)
    
    # 1. Insert raw_transactions
    execute_sql(cursor, """
        INSERT INTO raw_transactions VALUES
        ('TXN001', 'CUST001', '2024-01-15', 'Electronics', 1299.99, 'Credit Card', 'New York'),
        ('TXN002', 'CUST001', '2024-02-20', 'Clothing', 89.50, 'Debit Card', 'New York'),
        ('TXN003', 'CUST002', '2024-01-18', 'Electronics', 599.00, 'PayPal', 'Los Angeles'),
        ('TXN004', 'CUST002', '2024-03-10', 'Home & Garden', 249.99, 'Credit Card', 'Los Angeles'),
        ('TXN005', 'CUST003', '2024-01-22', 'Books', 45.00, 'Cash', 'Chicago'),
        ('TXN006', 'CUST003', '2024-02-14', 'Electronics', 799.99, 'Credit Card', 'Chicago'),
        ('TXN007', 'CUST003', '2024-03-05', 'Clothing', 159.99, 'Debit Card', 'Chicago'),
        ('TXN008', 'CUST004', '2024-01-25', 'Sports', 329.00, 'Credit Card', 'Houston'),
        ('TXN009', 'CUST004', '2024-02-28', 'Electronics', 1499.99, 'Credit Card', 'Houston'),
        ('TXN010', 'CUST005', '2024-01-30', 'Books', 125.50, 'PayPal', 'Phoenix'),
        ('TXN011', 'CUST005', '2024-03-15', 'Home & Garden', 449.99, 'Credit Card', 'Phoenix'),
        ('TXN012', 'CUST001', '2024-03-20', 'Electronics', 2199.99, 'Credit Card', 'New York'),
        ('TXN013', 'CUST002', '2024-04-05', 'Clothing', 199.99, 'Debit Card', 'Los Angeles'),
        ('TXN014', 'CUST003', '2024-04-10', 'Sports', 549.00, 'Credit Card', 'Chicago'),
        ('TXN015', 'CUST006', '2024-02-10', 'Electronics', 899.99, 'PayPal', 'Seattle')
    """, "Inserting into raw_transactions (15 rows)")
    
    # 2. Insert customers
    execute_sql(cursor, """
        INSERT INTO customers VALUES
        ('CUST001', 'John Smith', 'Premium', '2023-01-15'),
        ('CUST002', 'Jane Doe', 'Regular', '2023-03-20'),
        ('CUST003', 'Bob Johnson', 'VIP', '2022-06-10'),
        ('CUST004', 'Alice Williams', 'Premium', '2023-02-28'),
        ('CUST005', 'Charlie Brown', 'Regular', '2023-05-15'),
        ('CUST006', 'Diana Prince', 'New', '2024-01-05')
    """, "Inserting into customers (6 rows)")
    
    # 3. Insert products (product_id, product_name, product_category, product_subcategory, brand)
    execute_sql(cursor, """
        INSERT INTO products VALUES
        ('PROD001', 'Laptop Pro', 'Electronics', 'Computers', 'TechBrand'),
        ('PROD002', 'Smartphone X', 'Electronics', 'Mobile', 'PhoneCo'),
        ('PROD003', 'Running Shoes', 'Sports', 'Footwear', 'SportCo'),
        ('PROD004', 'Dress Shirt', 'Clothing', 'Mens', 'FashionInc'),
        ('PROD005', 'Garden Tools Set', 'Home & Garden', 'Tools', 'HomeDepot'),
        ('PROD006', 'Book Collection', 'Books', 'Fiction', 'Publisher'),
        ('PROD007', 'Tablet Plus', 'Electronics', 'Tablets', 'TechBrand'),
        ('PROD008', 'Yoga Mat', 'Sports', 'Fitness', 'FitLife')
    """, "Inserting into products (8 rows)")
    
    # 4. Insert stores
    execute_sql(cursor, """
        INSERT INTO stores VALUES
        ('STORE001', 'New York Flagship', 'Northeast', 'Large'),
        ('STORE002', 'LA Downtown', 'West', 'Medium'),
        ('STORE003', 'Chicago Central', 'Midwest', 'Large'),
        ('STORE004', 'Houston Mall', 'South', 'Medium'),
        ('STORE005', 'Phoenix Plaza', 'Southwest', 'Small')
    """, "Inserting into stores (5 rows)")
    
    # 5. Insert sales_fact
    execute_sql(cursor, """
        INSERT INTO sales_fact VALUES
        ('TXN001', 'CUST001', 'PROD001', 'STORE001', '2024-01-15', '2024-01-15 10:30:00', 1, 1299.99, 0, 104.00, 1403.99, 'Credit Card', false, 0),
        ('TXN002', 'CUST001', 'PROD004', 'STORE001', '2024-02-20', '2024-02-20 14:20:00', 2, 44.75, 5, 7.16, 89.50, 'Debit Card', false, 0),
        ('TXN003', 'CUST002', 'PROD002', 'STORE002', '2024-01-18', '2024-01-18 11:15:00', 1, 599.00, 0, 47.92, 646.92, 'PayPal', true, 9.99),
        ('TXN004', 'CUST002', 'PROD005', 'STORE002', '2024-03-10', '2024-03-10 16:45:00', 1, 249.99, 10, 22.50, 272.49, 'Credit Card', true, 12.99),
        ('TXN005', 'CUST003', 'PROD006', 'STORE003', '2024-01-22', '2024-01-22 09:00:00', 3, 15.00, 0, 3.60, 48.60, 'Cash', false, 0),
        ('TXN006', 'CUST003', 'PROD007', 'STORE003', '2024-02-14', '2024-02-14 13:30:00', 1, 799.99, 0, 64.00, 863.99, 'Credit Card', false, 0),
        ('TXN007', 'CUST003', 'PROD004', 'STORE003', '2024-03-05', '2024-03-05 15:00:00', 3, 53.33, 0, 12.80, 172.79, 'Debit Card', false, 0),
        ('TXN008', 'CUST004', 'PROD003', 'STORE004', '2024-01-25', '2024-01-25 10:00:00', 2, 164.50, 0, 26.32, 355.32, 'Credit Card', false, 0),
        ('TXN009', 'CUST004', 'PROD001', 'STORE004', '2024-02-28', '2024-02-28 12:00:00', 1, 1499.99, 0, 120.00, 1619.99, 'Credit Card', true, 19.99),
        ('TXN010', 'CUST005', 'PROD006', 'STORE005', '2024-01-30', '2024-01-30 11:30:00', 5, 25.10, 0, 10.04, 135.54, 'PayPal', true, 7.99)
    """, "Inserting into sales_fact (10 rows)")
    
    # 6. Insert raw_reviews
    execute_sql(cursor, """
        INSERT INTO raw_reviews VALUES
        ('REV001', 'PROD001', 'CUST001', 'Amazing laptop! Very fast and reliable. Great for work and gaming.', 5, '2024-01-20'),
        ('REV002', 'PROD002', 'CUST002', 'Good phone but battery life could be better. Screen is excellent.', 4, '2024-01-25'),
        ('REV003', 'PROD003', 'CUST004', 'Comfortable running shoes. Perfect fit and great cushioning.', 5, '2024-02-01'),
        ('REV004', 'PROD004', 'CUST001', 'Nice shirt but fabric is a bit thin. Fits well though.', 3, '2024-02-25'),
        ('REV005', 'PROD005', 'CUST002', 'Excellent tool set! Everything I needed for my garden.', 5, '2024-03-15'),
        ('REV006', 'PROD006', 'CUST003', 'Great book collection. Kept me entertained for weeks.', 5, '2024-02-05'),
        ('REV007', 'PROD007', 'CUST003', 'Tablet works fine but a bit slow sometimes. Good value.', 3, '2024-02-20'),
        ('REV008', 'PROD001', 'CUST004', 'Best laptop I have owned. Worth every penny!', 5, '2024-03-05'),
        ('REV009', 'PROD002', 'CUST005', 'Disappointed with camera quality. Otherwise okay.', 2, '2024-02-10'),
        ('REV010', 'PROD003', 'CUST006', 'Perfect for running. Highly recommend these shoes.', 5, '2024-02-15'),
        ('REV011', 'PROD001', 'CUST005', 'Good laptop but runs hot under heavy load. Still satisfied.', 4, '2024-03-10'),
        ('REV012', 'PROD004', 'CUST006', 'Terrible quality. Shirt shrank after first wash.', 1, '2024-03-01')
    """, "Inserting into raw_reviews (12 rows)")
    
    # 7. Insert user_events
    execute_sql(cursor, """
        INSERT INTO user_events VALUES
        ('EVT001', 'USER001', 'https://shop.com/home', 'page_view', '2024-01-15 10:00:00', 'desktop', '2024-01-15'),
        ('EVT002', 'USER001', 'https://shop.com/products/laptop', 'page_view', '2024-01-15 10:02:00', 'desktop', '2024-01-15'),
        ('EVT003', 'USER001', 'https://shop.com/products/laptop', 'add_to_cart', '2024-01-15 10:05:00', 'desktop', '2024-01-15'),
        ('EVT004', 'USER001', 'https://shop.com/cart', 'page_view', '2024-01-15 10:06:00', 'desktop', '2024-01-15'),
        ('EVT005', 'USER001', 'https://shop.com/checkout', 'page_view', '2024-01-15 10:08:00', 'desktop', '2024-01-15'),
        ('EVT006', 'USER001', 'https://shop.com/checkout', 'purchase', '2024-01-15 10:10:00', 'desktop', '2024-01-15'),
        ('EVT007', 'USER002', 'https://shop.com/home', 'page_view', '2024-01-15 11:00:00', 'mobile', '2024-01-15'),
        ('EVT008', 'USER002', 'https://shop.com/products', 'page_view', '2024-01-15 11:02:00', 'mobile', '2024-01-15'),
        ('EVT009', 'USER002', 'https://shop.com/products/phone', 'page_view', '2024-01-15 11:05:00', 'mobile', '2024-01-15'),
        ('EVT010', 'USER002', 'https://shop.com/products/tablet', 'page_view', '2024-01-15 11:08:00', 'mobile', '2024-01-15'),
        ('EVT011', 'USER003', 'https://shop.com/home', 'page_view', '2024-01-16 09:00:00', 'tablet', '2024-01-16'),
        ('EVT012', 'USER003', 'https://shop.com/search', 'search', '2024-01-16 09:02:00', 'tablet', '2024-01-16'),
        ('EVT013', 'USER003', 'https://shop.com/products/shoes', 'page_view', '2024-01-16 09:05:00', 'tablet', '2024-01-16'),
        ('EVT014', 'USER003', 'https://shop.com/products/shoes', 'add_to_cart', '2024-01-16 09:07:00', 'tablet', '2024-01-16')
    """, "Inserting into user_events (14 rows)")
    
    # 8. Insert raw_event_stream (JSON payloads)
    execute_sql(cursor, """
        INSERT INTO raw_event_stream VALUES
        ('EVT001', 'USER001', '2024-01-15 10:00:00', 'page_view', 
         '{"page":{"url":"https://shop.com/home","title":"Home"},"user":{"session_id":"SESS001","ip_address":"192.168.1.1"},"device":{"type":"desktop","browser":"Chrome"},"source":"google","medium":"organic","campaign":"summer_sale","custom_attributes":"color:blue,size:large","metadata":"{}"}', 
         '2024-01-15'),
        ('EVT002', 'USER001', '2024-01-15 10:02:00', 'page_view',
         '{"page":{"url":"https://shop.com/products/laptop","title":"Laptop"},"user":{"session_id":"SESS001","ip_address":"192.168.1.1"},"device":{"type":"desktop","browser":"Chrome"},"source":"google","medium":"organic","campaign":"summer_sale","custom_attributes":"category:electronics,price_range:high","metadata":"{}"}',
         '2024-01-15'),
        ('EVT003', 'USER002', '2024-01-15 11:00:00', 'page_view',
         '{"page":{"url":"https://shop.com/home","title":"Home"},"user":{"session_id":"SESS002","ip_address":"10.0.0.5"},"device":{"type":"mobile","browser":"Safari"},"source":"facebook","medium":"social","campaign":"mobile_promo","custom_attributes":"age_group:25-34,interest:tech","metadata":"{}"}',
         '2024-01-15'),
        ('EVT004', 'USER002', '2024-01-15 11:05:00', 'page_view',
         '{"page":{"url":"https://shop.com/products/phone","title":"Smartphone"},"user":{"session_id":"SESS002","ip_address":"10.0.0.5"},"device":{"type":"mobile","browser":"Safari"},"source":"facebook","medium":"social","campaign":"mobile_promo","custom_attributes":"category:electronics,brand_preference:premium","metadata":"{}"}',
         '2024-01-15'),
        ('EVT005', 'USER003', '2024-01-16 09:00:00', 'page_view',
         '{"page":{"url":"https://shop.com/home","title":"Home"},"user":{"session_id":"SESS003","ip_address":"172.16.0.10"},"device":{"type":"tablet","browser":"Firefox"},"source":"email","medium":"newsletter","campaign":"weekly_deals","custom_attributes":"subscriber:true,loyalty_tier:gold","metadata":"{}"}',
         '2024-01-16')
    """, "Inserting into raw_event_stream (5 rows)")


def main():
    """Main execution function."""
    DATABRICKS_PROFILE = os.getenv('DATABRICKS_PROFILE', 'fe')
    WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')
    SCHEMA = 'hql_test'  # Dedicated schema for migration testing
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Sample Data Generator for HQL Migration{Colors.RESET}")
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
        http_path = f'/sql/1.0/warehouses/{WAREHOUSE_ID}' if WAREHOUSE_ID else '/sql/1.0/warehouses/default'
        
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
            
            tables = [
                'raw_transactions', 'sales_fact', 'customers', 'products',
                'stores', 'raw_reviews', 'user_events', 'raw_event_stream'
            ]
            
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

