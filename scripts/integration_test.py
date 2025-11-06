#!/usr/bin/env python3
"""
Integration Test: Actually execute the converted SQL in Databricks.
This verifies that all CREATE TABLE statements work end-to-end.

IMPORTANT: This test REQUIRES sample data tables to exist first!
Run: python scripts/generate_sample_data.py
"""

import os
import configparser
from pathlib import Path
from typing import Tuple, List, Dict
from databricks import sql
import re
import subprocess
import sys

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
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    if profile not in config:
        raise ValueError(f"Profile '{profile}' not found in {config_path}")
    
    host = config[profile]['host'].strip()
    token = config[profile]['token'].strip()
    
    return host, token


def extract_create_statements(sql_content: str) -> List[Dict[str, str]]:
    """Extract individual CREATE TABLE and CREATE FUNCTION statements from SQL file."""
    statements = []
    
    # Split by separator lines
    parts = sql_content.split('-' * 80)
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
            
        # Extract table name from comment
        table_match = re.search(r'-- Table: (\w+)', part)
        table_name = table_match.group(1) if table_match else "Unknown"
        
        # Extract CREATE TABLE or CREATE FUNCTION statements
        # Try CREATE TABLE first
        create_match = re.search(r'(CREATE (?:OR REPLACE )?(?:TABLE|FUNCTION).*?)(?=\n--|\n\n-{80}|$)', part, flags=re.DOTALL | re.IGNORECASE)
        if create_match:
            sql = create_match.group(1).strip()
            statements.append({
                'table_name': table_name,
                'sql': sql
            })
        # If no CREATE statement but has UDF definitions, extract all of them
        elif 'CREATE OR REPLACE FUNCTION' in part:
            statements.append({
                'table_name': table_name,
                'sql': part
            })
    
    return statements


def execute_statement(cursor, sql: str, table_name: str) -> Dict:
    """Execute a single SQL statement (CREATE TABLE or CREATE FUNCTION)."""
    result = {
        'table_name': table_name,
        'success': False,
        'error': None
    }
    
    try:
        # UDF definitions might have multiple statements
        if table_name == 'UDF_Definitions' or 'CREATE FUNCTION' in sql.upper():
            # Extract CREATE FUNCTION statements using regex (handles comments better)
            udf_pattern = r'(CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+\w+\s*\([^)]*\)\s*RETURNS\s+\w+\s+RETURN\s+[^;]+)'
            udf_statements = re.findall(udf_pattern, sql, flags=re.IGNORECASE | re.DOTALL)
            
            errors = []
            for udf_sql in udf_statements:
                udf_sql = udf_sql.strip()
                if udf_sql:
                    try:
                        cursor.execute(udf_sql)
                    except Exception as udf_error:
                        # Collect errors but continue with other UDFs
                        errors.append(str(udf_error)[:150])
            
            if not udf_statements:
                result['error'] = "No UDF statements found to execute"
                result['success'] = False
            elif errors:
                result['error'] = '; '.join(errors)
                result['success'] = False  
            else:
                result['success'] = True
        else:
            cursor.execute(sql)
            result['success'] = True
    except Exception as e:
        result['error'] = str(e)
        result['success'] = False
    
    return result


def cleanup_table(cursor, table_name: str):
    """Drop a table or function if it exists."""
    if table_name == 'UDF_Definitions':
        # Drop commonly created UDFs
        common_udfs = ['sentiment_score', 'normalize_text']
        for udf in common_udfs:
            try:
                cursor.execute(f"DROP FUNCTION IF EXISTS {udf}")
            except:
                pass
        return
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass


def test_sql_file(connection, file_path: Path, cleanup: bool = True) -> List[Dict]:
    """Test all statements in a SQL file."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}Testing: {file_path.name}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    with open(file_path, 'r') as f:
        sql_content = f.read()
    
    statements = extract_create_statements(sql_content)
    results = []
    tables_created = []
    
    with connection.cursor() as cursor:
        for i, stmt_info in enumerate(statements, 1):
            table_name = stmt_info['table_name']
            sql = stmt_info['sql']
            
            print(f"{Colors.YELLOW}[{i}/{len(statements)}] Testing: {table_name}{Colors.RESET}", end=" ")
            
            # Clean up first if table exists (from previous run)
            cleanup_table(cursor, table_name)
            
            # Execute the CREATE TABLE
            result = execute_statement(cursor, sql, table_name)
            result['file'] = file_path.stem
            results.append(result)
            
            if result['success']:
                print(f"{Colors.GREEN}✓ Success{Colors.RESET}")
                tables_created.append(table_name)
            else:
                print(f"{Colors.RED}✗ Failed{Colors.RESET}")
                print(f"  {Colors.RED}Error: {result['error'][:200]}{Colors.RESET}")
        
        # Clean up ALL tables from this file at the end (if cleanup requested)
        if cleanup and tables_created:
            print(f"\n{Colors.YELLOW}Cleaning up {len(tables_created)} table(s)...{Colors.RESET}")
            for table_name in tables_created:
                cleanup_table(cursor, table_name)
    
    return results


def print_summary(all_results: List[Dict]):
    """Print summary of test results."""
    total = len(all_results)
    passed = sum(1 for r in all_results if r['success'])
    failed = sum(1 for r in all_results if not r['success'])
    
    print(f"\n{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}INTEGRATION TEST SUMMARY{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    print(f"Total statements tested: {Colors.BOLD}{total}{Colors.RESET}")
    print(f"Passed: {Colors.GREEN}{Colors.BOLD}{passed}{Colors.RESET} ({(passed/total)*100:.1f}%)")
    print(f"Failed: {Colors.RED}{Colors.BOLD}{failed}{Colors.RESET} ({(failed/total)*100:.1f}%)\n")
    
    if failed > 0:
        print(f"{Colors.RED}{Colors.BOLD}Failed statements:{Colors.RESET}\n")
        for result in all_results:
            if not result['success']:
                print(f"  {Colors.RED}✗ {result['file']} - {result['table_name']}{Colors.RESET}")
                print(f"    Error: {result['error'][:150]}\n")
    else:
        print(f"{Colors.GREEN}{Colors.BOLD}🎉 All statements executed successfully!{Colors.RESET}")
        print(f"{Colors.GREEN}Your SQL is production-ready for Databricks!{Colors.RESET}\n")


def main():
    """Main execution function."""
    # Configuration
    DATABRICKS_PROFILE = os.getenv('DATABRICKS_PROFILE', 'fe')
    WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')
    SCHEMA = 'hql_test'  # Dedicated schema for migration testing
    CLEANUP_AFTER_TEST = True  # Set to False to keep tables for inspection
    
    # Get paths
    script_dir = Path(__file__).parent.parent
    sql_dir = script_dir / 'spark_sql_final'
    
    if not sql_dir.exists():
        print(f"{Colors.RED}✗ Directory not found: {sql_dir}{Colors.RESET}")
        return
    
    # Find all final SQL files
    sql_files = sorted(sql_dir.glob('script*_final.sql'))
    
    if not sql_files:
        print(f"{Colors.RED}No SQL files found in {sql_dir}{Colors.RESET}")
        return
    
    if not WAREHOUSE_ID:
        print(f"{Colors.RED}✗ DATABRICKS_WAREHOUSE_ID environment variable not set{Colors.RESET}")
        print(f"{Colors.YELLOW}Set it with: export DATABRICKS_WAREHOUSE_ID=your_warehouse_id{Colors.RESET}")
        return
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Integration Test: Execute Final SQL in Databricks{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"\n{Colors.CYAN}Found {len(sql_files)} SQL file(s) to test{Colors.RESET}")
    print(f"{Colors.YELLOW}Cleanup after test: {'Yes' if CLEANUP_AFTER_TEST else 'No'}{Colors.RESET}")
    
    # Read Databricks configuration
    try:
        host, token = read_databricks_config(DATABRICKS_PROFILE)
        print(f"\n{Colors.GREEN}✓ Databricks config loaded{Colors.RESET}")
        print(f"  Host: {host}")
        print(f"  Profile: {DATABRICKS_PROFILE}")
    except Exception as e:
        print(f"\n{Colors.RED}✗ Failed to read Databricks configuration: {e}{Colors.RESET}")
        return
    
    # Connect to Databricks SQL
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
        
        # Use dedicated schema
        cursor = connection.cursor()
        cursor.execute(f'USE SCHEMA {SCHEMA}')
        cursor.close()
        print(f"{Colors.CYAN}Using schema: {SCHEMA}{Colors.RESET}")
        
    except Exception as e:
        print(f"{Colors.RED}✗ Failed to connect: {e}{Colors.RESET}")
        return
    
    # Generate sample data FIRST (base tables must exist!)
    print(f"\n{Colors.CYAN}{Colors.BOLD}Step 1: Generating sample data...{Colors.RESET}")
    generate_script = Path(__file__).parent / 'generate_sample_data.py'
    try:
        result = subprocess.run(
            [sys.executable, str(generate_script)],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"{Colors.GREEN}✓ Sample data generated successfully{Colors.RESET}\n")
    except subprocess.CalledProcessError as e:
        print(f"{Colors.RED}✗ Failed to generate sample data:{Colors.RESET}")
        print(e.stdout)
        print(e.stderr)
        return
    
    # Test all SQL files
    print(f"{Colors.CYAN}{Colors.BOLD}Step 2: Testing converted SQL...{Colors.RESET}\n")
    all_results = []
    try:
        for sql_file in sql_files:
            file_results = test_sql_file(connection, sql_file, CLEANUP_AFTER_TEST)
            all_results.extend(file_results)
    finally:
        if connection:
            connection.close()
            print(f"\n{Colors.YELLOW}Connection closed{Colors.RESET}")
    
    # Print summary
    print_summary(all_results)


if __name__ == '__main__':
    main()

