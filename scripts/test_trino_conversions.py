#!/usr/bin/env python3
"""
Quick test: Execute converted Trino SQL in Databricks to verify they work.
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


def test_query(cursor, query: str, query_name: str) -> bool:
    """Test a single query by executing it."""
    try:
        print(f"  {Colors.YELLOW}Testing: {query_name}...{Colors.RESET}", end=" ")
        cursor.execute(query)
        result = cursor.fetchall()
        print(f"{Colors.GREEN}✓ Success ({len(result)} rows){Colors.RESET}")
        return True
    except Exception as e:
        print(f"{Colors.RED}✗ Failed{Colors.RESET}")
        print(f"    {Colors.RED}Error: {str(e)[:150]}{Colors.RESET}")
        return False


def main():
    """Main execution."""
    DATABRICKS_PROFILE = os.getenv('DATABRICKS_PROFILE', 'fe')
    WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')
    SCHEMA = 'hql_test'
    
    # Get paths
    script_dir = Path(__file__).parent.parent
    converted_dir = script_dir / 'databricks_from_trino'
    
    # Find converted SQL files
    sql_files = sorted(converted_dir.glob('*_databricks.sql'))
    
    if not sql_files:
        print(f"{Colors.RED}No converted SQL files found in {converted_dir}{Colors.RESET}")
        return
    
    if not WAREHOUSE_ID:
        print(f"{Colors.RED}✗ DATABRICKS_WAREHOUSE_ID environment variable not set{Colors.RESET}")
        print(f"{Colors.YELLOW}Set it with: export DATABRICKS_WAREHOUSE_ID=your_warehouse_id{Colors.RESET}")
        return
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Testing Trino → Databricks Conversions{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    # Read config
    try:
        host, token = read_databricks_config(DATABRICKS_PROFILE)
        print(f"{Colors.GREEN}✓ Databricks config loaded{Colors.RESET}\n")
    except Exception as e:
        print(f"{Colors.RED}✗ Failed to read config: {e}{Colors.RESET}")
        return
    
    # Connect
    connection = None
    try:
        print(f"{Colors.YELLOW}Connecting to Databricks SQL...{Colors.RESET}")
        server_hostname = host.replace('https://', '')
        http_path = f'/sql/1.0/warehouses/{WAREHOUSE_ID}'
        
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=token
        )
        print(f"{Colors.GREEN}✓ Connected successfully{Colors.RESET}")
        
        cursor = connection.cursor()
        cursor.execute(f'USE SCHEMA {SCHEMA}')
        print(f"{Colors.CYAN}Using schema: {SCHEMA}{Colors.RESET}\n")
        
        # Test each file
        passed = 0
        failed = 0
        
        for sql_file in sql_files:
            print(f"\n{Colors.CYAN}{Colors.BOLD}Testing: {sql_file.name}{Colors.RESET}")
            
            with open(sql_file, 'r') as f:
                content = f.read()
            
            # Extract the actual SQL (skip comments and separator lines)
            lines = content.split('\n')
            sql_lines = []
            in_sql = False
            
            for line in lines:
                if line.startswith('--') or line.startswith('-----'):
                    continue
                if line.strip():
                    in_sql = True
                if in_sql:
                    sql_lines.append(line)
            
            query = '\n'.join(sql_lines).strip()
            
            if query:
                if test_query(cursor, query, sql_file.stem):
                    passed += 1
                else:
                    failed += 1
        
        cursor.close()
        
        # Summary
        print(f"\n{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")
        print(f"{Colors.MAGENTA}{Colors.BOLD}TEST SUMMARY{Colors.RESET}")
        print(f"{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
        
        total = passed + failed
        print(f"Total: {Colors.BOLD}{total}{Colors.RESET}")
        print(f"Passed: {Colors.GREEN}{Colors.BOLD}{passed}{Colors.RESET} ({(passed/total)*100:.1f}%)")
        print(f"Failed: {Colors.RED}{Colors.BOLD}{failed}{Colors.RESET} ({(failed/total)*100:.1f}%)\n")
        
        if failed == 0:
            print(f"{Colors.GREEN}{Colors.BOLD}🎉 All Trino conversions work!{Colors.RESET}\n")
        
    except Exception as e:
        print(f"{Colors.RED}✗ Error: {e}{Colors.RESET}")
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    main()

