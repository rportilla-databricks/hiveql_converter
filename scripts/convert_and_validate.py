#!/usr/bin/env python3
"""
Convert HQL to Spark SQL using AI_QUERY, then validate with EXPLAIN.
This script uses Databricks AI_QUERY to automatically convert HiveQL to Spark SQL.
"""

import os
import re
import configparser
from pathlib import Path
from typing import List, Dict, Tuple
from databricks import sql
from databricks.sql.client import Connection

# ANSI color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
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


def extract_statements(sql_content: str) -> List[Dict[str, str]]:
    """
    Extract individual SQL statements from a file.
    Split on CREATE TABLE statements.
    """
    # Remove single-line comments but preserve the SQL
    lines = sql_content.split('\n')
    cleaned_lines = []
    for line in lines:
        # Remove comments but keep the line structure
        if '--' in line:
            cleaned_lines.append(line.split('--')[0])
        else:
            cleaned_lines.append(line)
    
    sql_content = '\n'.join(cleaned_lines)
    
    # Split on CREATE TABLE
    statements = re.split(r'\n(?=CREATE\s+TABLE)', sql_content, flags=re.IGNORECASE)
    
    # Clean up and filter empty statements
    result = []
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            # Extract table name
            table_match = re.search(r'CREATE\s+TABLE\s+(\w+)', stmt, re.IGNORECASE)
            table_name = table_match.group(1) if table_match else "Unknown"
            result.append({
                'table_name': table_name,
                'hql': stmt
            })
    
    return result


def convert_hql_with_ai(connection: Connection, hql_query: str, table_name: str) -> Dict:
    """
    Use AI_QUERY to convert HiveQL to Spark SQL.
    """
    result = {
        'table_name': table_name,
        'original_hql': hql_query,
        'converted_sql': None,
        'conversion_notes': None,
        'valid': False,
        'validation_error': None,
        'explain_output': None
    }
    
    # Escape single quotes for AI_QUERY
    escaped_hql = hql_query.replace("'", "''")
    
    # Truncate if too long (AI_QUERY has limits)
    if len(escaped_hql) > 3000:
        escaped_hql = escaped_hql[:3000]
        result['conversion_notes'] = "Query truncated for AI_QUERY processing"
    
    print(f"  {Colors.YELLOW}Step 1: Converting HQL to Spark SQL with AI...{Colors.RESET}")
    
    try:
        with connection.cursor() as cursor:
            # Use AI_QUERY to convert HQL to Spark SQL
            conversion_prompt = f"""Convert this HiveQL query to Databricks Spark SQL. 

Key conversions needed:
- Remove DISTRIBUTE BY, SORT BY (use CLUSTERED BY in table definition or remove entirely)
- Replace STORED AS ORC/PARQUET with USING DELTA or USING PARQUET
- Add OPTIONS clause for table properties
- Replace MAPJOIN hint with BROADCAST hint
- Remove STREAMTABLE hint
- Remove TABLESAMPLE from CTAS (can be used in queries)
- Replace CREATE TEMPORARY FUNCTION with proper registration
- Ensure window functions are compatible

Return ONLY the converted SQL, no explanations. If there are critical issues, add a comment at the top.

HiveQL Query:
{escaped_hql}

Converted Spark SQL:"""

            ai_query = f"""
            SELECT AI_QUERY(
                'databricks-meta-llama-3-1-70b-instruct',
                '{conversion_prompt}'
            ) as converted_sql
            """
            
            cursor.execute(ai_query)
            ai_result = cursor.fetchone()
            
            if ai_result and ai_result[0]:
                converted_sql = ai_result[0]
                result['converted_sql'] = converted_sql
                print(f"  {Colors.GREEN}✓ Conversion completed{Colors.RESET}")
            else:
                result['validation_error'] = "AI_QUERY returned empty result"
                print(f"  {Colors.RED}✗ Conversion failed{Colors.RESET}")
                return result
                
    except Exception as e:
        result['validation_error'] = f"AI_QUERY conversion failed: {str(e)}"
        print(f"  {Colors.RED}✗ Conversion error: {str(e)}{Colors.RESET}")
        return result
    
    # Now validate the converted SQL with EXPLAIN
    print(f"  {Colors.YELLOW}Step 2: Validating with EXPLAIN...{Colors.RESET}")
    
    try:
        with connection.cursor() as cursor:
            # Extract SELECT portion for EXPLAIN
            converted_sql = result['converted_sql']
            
            if 'AS SELECT' in converted_sql.upper() or 'AS\nSELECT' in converted_sql.upper():
                match = re.search(r'\bAS\s+(SELECT\b.*)', converted_sql, flags=re.IGNORECASE | re.DOTALL)
                if match:
                    select_statement = match.group(1).rstrip(';').strip()
                    explain_query = f"EXPLAIN {select_statement}"
                    cursor.execute(explain_query)
                    explain_results = cursor.fetchall()
                    result['explain_output'] = '\n'.join([str(row[0]) for row in explain_results[:10]])
                    result['valid'] = True
                    print(f"  {Colors.GREEN}✓ Validation passed{Colors.RESET}")
            else:
                # Try to explain the whole thing
                explain_query = f"EXPLAIN {converted_sql.rstrip(';')}"
                cursor.execute(explain_query)
                explain_results = cursor.fetchall()
                result['explain_output'] = '\n'.join([str(row[0]) for row in explain_results[:10]])
                result['valid'] = True
                print(f"  {Colors.GREEN}✓ Validation passed{Colors.RESET}")
                
    except Exception as explain_error:
        result['validation_error'] = f"EXPLAIN validation failed: {str(explain_error)}"
        result['valid'] = False
        print(f"  {Colors.RED}✗ Validation failed: {str(explain_error)}{Colors.RESET}")
    
    return result


def process_hql_file(connection: Connection, file_path: Path, output_dir: Path) -> List[Dict]:
    """
    Process a single HQL file: convert all statements and validate.
    """
    print(f"\n{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}Processing: {file_path.name}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    with open(file_path, 'r') as f:
        hql_content = f.read()
    
    statements = extract_statements(hql_content)
    results = []
    converted_statements = []
    
    for i, stmt_info in enumerate(statements, 1):
        table_name = stmt_info['table_name']
        hql_query = stmt_info['hql']
        
        print(f"{Colors.YELLOW}{Colors.BOLD}[{i}/{len(statements)}] Converting: {table_name}{Colors.RESET}")
        result = convert_hql_with_ai(connection, hql_query, table_name)
        result['file'] = file_path.stem
        results.append(result)
        
        if result['converted_sql']:
            converted_statements.append(f"-- Table: {table_name}")
            converted_statements.append(f"-- Original file: {file_path.name}")
            converted_statements.append(f"-- Validation: {'PASSED' if result['valid'] else 'FAILED'}")
            if result['conversion_notes']:
                converted_statements.append(f"-- Notes: {result['conversion_notes']}")
            converted_statements.append("")
            converted_statements.append(result['converted_sql'])
            converted_statements.append("")
            converted_statements.append("-" * 80)
            converted_statements.append("")
        
        print()
    
    # Save converted SQL to file
    if converted_statements:
        output_file = output_dir / f"{file_path.stem}_converted.sql"
        with open(output_file, 'w') as f:
            f.write('\n'.join(converted_statements))
        print(f"{Colors.GREEN}✓ Saved converted SQL to: {output_file.name}{Colors.RESET}")
    
    return results


def print_summary(all_results: List[Dict]):
    """Print a summary of conversion and validation results."""
    total = len(all_results)
    converted = sum(1 for r in all_results if r['converted_sql'] is not None)
    valid = sum(1 for r in all_results if r['valid'])
    
    print(f"\n{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}CONVERSION & VALIDATION SUMMARY{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    print(f"Total queries processed: {Colors.BOLD}{total}{Colors.RESET}")
    print(f"Successfully converted: {Colors.GREEN}{Colors.BOLD}{converted}{Colors.RESET}")
    print(f"Validation passed: {Colors.GREEN}{Colors.BOLD}{valid}{Colors.RESET}")
    print(f"Validation failed: {Colors.RED}{Colors.BOLD}{converted - valid}{Colors.RESET}")
    print(f"Conversion success rate: {Colors.BOLD}{(converted/total)*100:.1f}%{Colors.RESET}")
    print(f"Validation success rate: {Colors.BOLD}{(valid/total)*100:.1f}%{Colors.RESET}\n")
    
    if converted - valid > 0:
        print(f"{Colors.RED}{Colors.BOLD}Queries that failed validation:{Colors.RESET}\n")
        for result in all_results:
            if result['converted_sql'] and not result['valid']:
                print(f"  {Colors.RED}✗ {result['file']} - {result['table_name']}{Colors.RESET}")
                print(f"    Error: {result['validation_error']}\n")


def save_detailed_results(all_results: List[Dict], output_file: Path):
    """Save detailed conversion and validation results to a file."""
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("HQL to Spark SQL Conversion & Validation Results\n")
        f.write("Using AI_QUERY for conversion and EXPLAIN for validation\n")
        f.write("=" * 80 + "\n\n")
        
        for result in all_results:
            f.write(f"\nFile: {result['file']}\n")
            f.write(f"Table: {result['table_name']}\n")
            f.write(f"Conversion: {'✓ SUCCESS' if result['converted_sql'] else '✗ FAILED'}\n")
            f.write(f"Validation: {'✓ PASSED' if result['valid'] else '✗ FAILED'}\n")
            f.write("-" * 80 + "\n")
            
            if result['conversion_notes']:
                f.write(f"\nNotes:\n{result['conversion_notes']}\n")
            
            if result['validation_error']:
                f.write(f"\nError:\n{result['validation_error']}\n")
            
            if result['converted_sql']:
                f.write(f"\nConverted SQL:\n{result['converted_sql']}\n")
            
            if result['explain_output']:
                f.write(f"\nEXPLAIN Output (first 10 lines):\n{result['explain_output']}\n")
            
            f.write("\n" + "=" * 80 + "\n")


def main():
    """Main execution function."""
    # Configuration
    DATABRICKS_PROFILE = 'fe'  # Change this to 'fe' or 'logfood' if needed
    WAREHOUSE_ID = '4b9b953939869799'  # Set this if you have a specific warehouse ID
    
    # Get script directory and set up paths
    script_dir = Path(__file__).parent.parent
    hql_dir = script_dir / 'hql_original'
    output_dir = script_dir / 'spark_sql_converted'
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    # Find all .hql files
    hql_files = sorted(hql_dir.glob('script*.hql'))
    
    if not hql_files:
        print(f"{Colors.RED}No HQL files found in {hql_dir}{Colors.RESET}")
        return
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}HQL to Spark SQL Converter (AI-Powered){Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"\n{Colors.CYAN}Found {len(hql_files)} HQL files to convert{Colors.RESET}")
    
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
        
        if WAREHOUSE_ID:
            http_path = f'/sql/1.0/warehouses/{WAREHOUSE_ID}'
        else:
            print(f"{Colors.YELLOW}No WAREHOUSE_ID set. Attempting default connection...{Colors.RESET}")
            http_path = '/sql/1.0/warehouses/default'
        
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=token
        )
        print(f"{Colors.GREEN}✓ Connected successfully{Colors.RESET}")
        
    except Exception as e:
        print(f"{Colors.RED}✗ Failed to connect: {e}{Colors.RESET}")
        print(f"\n{Colors.YELLOW}Note: Make sure you have a SQL warehouse running.{Colors.RESET}")
        print(f"{Colors.YELLOW}Update WAREHOUSE_ID in the script with your warehouse ID.{Colors.RESET}")
        return
    
    # Process all HQL files
    all_results = []
    try:
        for hql_file in hql_files:
            file_results = process_hql_file(connection, hql_file, output_dir)
            all_results.extend(file_results)
    finally:
        if connection:
            connection.close()
            print(f"\n{Colors.YELLOW}Connection closed{Colors.RESET}")
    
    # Print summary
    print_summary(all_results)
    
    # Save detailed results to file
    results_file = output_dir / 'conversion_validation_results.txt'
    save_detailed_results(all_results, results_file)
    
    print(f"\n{Colors.BLUE}Converted SQL files saved to: {output_dir}/{Colors.RESET}")
    print(f"{Colors.BLUE}Detailed results saved to: {results_file}{Colors.RESET}\n")


if __name__ == '__main__':
    main()

