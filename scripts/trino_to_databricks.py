#!/usr/bin/env python3
"""
Smart Trino SQL to Databricks SQL Converter with AI_QUERY fallback.

Environment Variables Required:
- DATABRICKS_WAREHOUSE_ID: Your SQL warehouse ID

Strategy:
1. Apply deterministic auto-fixes for Trino→Databricks conversions
2. Validate SQL with EXPLAIN (works even when dependent tables don't exist yet)
3. If validation fails → Use AI_QUERY to convert (Databricks Claude Sonnet 4.5)
4. Save production-ready SQL

Auto-fixes:
- Data types: VARCHAR → STRING, VARBINARY → BINARY
- JSON functions: json_extract_scalar() → get_json_object()
- Array functions: cardinality() → size(), array_agg() → collect_list()
- Date functions: date_add() → date_add(), date_diff() → datediff()
- UNNEST → LATERAL VIEW explode()
- Aggregates: approx_percentile() → percentile_approx(), arbitrary() → first()

AI Model: databricks-claude-sonnet-4-5 (Claude Sonnet 4.5)
"""

import os
import re
import configparser
from pathlib import Path
from typing import List, Dict, Tuple
from databricks import sql
from databricks.sql.client import Connection

# ANSI color codes
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
    """Extract individual SQL statements from a file."""
    # For files with single SELECT statement, just return it
    sql_content = sql_content.strip()
    
    if not sql_content:
        return []
    
    # Check if it's a single statement (no semicolons or only one at the end)
    statements_by_semicolon = [s.strip() for s in sql_content.split(';') if s.strip()]
    
    if len(statements_by_semicolon) == 1:
        # Single statement file
        result = []
        stmt = statements_by_semicolon[0]
        
        # Extract table/view name if CREATE statement
        table_match = re.search(r'CREATE\s+(?:TABLE|VIEW)\s+(\w+)', stmt, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)
        else:
            # Standalone SELECT query - use generic name
            table_name = "query_1"
        
        result.append({
            'table_name': table_name,
            'sql': stmt
        })
        return result
    
    # Multiple statements - split properly
    result = []
    for i, stmt in enumerate(statements_by_semicolon, 1):
        stmt = stmt.strip()
        if not stmt:
            continue
            
        # Extract table/view name
        table_match = re.search(r'CREATE\s+(?:TABLE|VIEW)\s+(\w+)', stmt, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)
        else:
            # Standalone SELECT query
            table_name = f"query_{i}"
        
        result.append({
            'table_name': table_name,
            'sql': stmt
        })
    
    return result


def apply_trino_to_databricks_fixes(sql: str) -> tuple[str, list]:
    """
    Apply automatic Trino→Databricks conversions.
    Returns (fixed_sql, list_of_fixes_applied).
    """
    fixes_applied = []
    fixed_sql = sql
    
    # Fix 1: VARCHAR/VARCHAR(n) → STRING
    if re.search(r'\bVARCHAR(?:\(\d+\))?\b', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\bVARCHAR(?:\(\d+\))?\b', 'STRING', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted VARCHAR to STRING")
    
    # Fix 2: VARBINARY → BINARY
    if re.search(r'\bVARBINARY\b', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\bVARBINARY\b', 'BINARY', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted VARBINARY to BINARY")
    
    # Fix 3: cardinality(array) → size(array)
    if re.search(r'\bcardinality\s*\(', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\bcardinality\s*\(', 'size(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted cardinality() to size()")
    
    # Fix 4: json_extract_scalar() → get_json_object()
    if re.search(r'\bjson_extract_scalar\s*\(', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\bjson_extract_scalar\s*\(', 'get_json_object(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted json_extract_scalar() to get_json_object()")
    
    # Fix 5: array_agg(DISTINCT x) → collect_set(x)
    if re.search(r'\barray_agg\s*\(\s*DISTINCT\s+', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\barray_agg\s*\(\s*DISTINCT\s+', 'collect_set(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted array_agg(DISTINCT) to collect_set()")
    
    # Fix 6: array_agg(x) → collect_list(x)
    if re.search(r'\barray_agg\s*\(', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\barray_agg\s*\(', 'collect_list(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted array_agg() to collect_list()")
    
    # Fix 7: approx_percentile() → percentile_approx()
    if re.search(r'\bapprox_percentile\s*\(', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\bapprox_percentile\s*\(', 'percentile_approx(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted approx_percentile() to percentile_approx()")
    
    # Fix 8: arbitrary() → first()
    if re.search(r'\barbitrary\s*\(', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\barbitrary\s*\(', 'first(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted arbitrary() to first()")
    
    # Fix 9: date_trunc('unit', date) → date_trunc('unit', date)
    # Compatible! But Spark uses different syntax sometimes
    # Trino: date_trunc('month', date_col)
    # Spark: date_trunc('MONTH', date_col) or trunc(date_col, 'MONTH')
    # Keep as-is, Spark supports date_trunc
    
    # Fix 10: date_add('unit', value, date) → date_add(date, value) or add_months()
    # Trino: date_add('day', 7, date_col) or date_add('month', 1, date_col)
    # Spark: date_add(date_col, 7) for days, add_months(date_col, 1) for months
    
    # Handle date_add for days
    day_add_pattern = r"date_add\s*\(\s*'day'\s*,\s*(-?\d+)\s*,\s*([^)]+)\)"
    if re.search(day_add_pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(day_add_pattern, r'date_add(\2, \1)', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted date_add('day', n, date) to date_add(date, n)")
    
    # Handle date_add for months
    month_add_pattern = r"date_add\s*\(\s*'month'\s*,\s*(-?\d+)\s*,\s*([^)]+)\)"
    if re.search(month_add_pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(month_add_pattern, r'add_months(\2, \1)', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted date_add('month', n, date) to add_months(date, n)")
    
    # Handle date_add for years
    year_add_pattern = r"date_add\s*\(\s*'year'\s*,\s*(-?\d+)\s*,\s*([^)]+)\)"
    if re.search(year_add_pattern, fixed_sql, flags=re.IGNORECASE):
        # Convert years to months (n years = n*12 months)
        def year_to_months(match):
            years = int(match.group(1))
            date_expr = match.group(2)
            months = years * 12
            return f'add_months({date_expr}, {months})'
        fixed_sql = re.sub(year_add_pattern, year_to_months, fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted date_add('year', n, date) to add_months(date, n*12)")
    
    # Fix 11: date_diff('unit', date1, date2) → datediff(date2, date1) for days
    # Trino: date_diff('day', start_date, end_date)
    # Spark: datediff(end_date, start_date)
    date_diff_pattern = r"date_diff\s*\(\s*'day'\s*,\s*([^,]+),\s*([^)]+)\)"
    if re.search(date_diff_pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(date_diff_pattern, r'datediff(\2, \1)', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted date_diff('day', start, end) to datediff(end, start)")
    
    # Fix 12: UNNEST with CROSS JOIN → LATERAL VIEW explode()
    # Trino: FROM table t CROSS JOIN UNNEST(t.array_col) AS u(elem)
    # Spark: FROM table t LATERAL VIEW explode(t.array_col) table_alias AS elem
    
    # Pattern 1: CROSS JOIN UNNEST(array) AS alias(column)
    unnest_pattern = r'CROSS\s+JOIN\s+UNNEST\s*\(\s*([^)]+)\s*\)\s+(?:AS\s+)?(\w+)\s*\(\s*(\w+)\s*\)'
    if re.search(unnest_pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(
            unnest_pattern,
            r'LATERAL VIEW explode(\1) \2 AS \3',
            fixed_sql,
            flags=re.IGNORECASE
        )
        fixes_applied.append("Converted CROSS JOIN UNNEST() to LATERAL VIEW explode()")
    
    # Fix 13: ROW(...) → STRUCT(...)
    if re.search(r'\bROW\s*\(', fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(r'\bROW\s*\(', 'STRUCT(', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted ROW() to STRUCT()")
    
    # Fix 14: CAST(x AS JSON) → to_json(x)
    json_cast_pattern = r'CAST\s*\(([^)]+)\s+AS\s+JSON\)'
    if re.search(json_cast_pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(json_cast_pattern, r'to_json(\1)', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted CAST(x AS JSON) to to_json(x)")
    
    # Fix 15: INTERVAL 'n' UNIT → INTERVAL n UNIT
    interval_pattern = r"INTERVAL\s+'(\d+)'\s+(DAY|HOUR|MINUTE|SECOND|MONTH|YEAR)"
    if re.search(interval_pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(interval_pattern, r'INTERVAL \1 \2', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Converted INTERVAL 'n' UNIT to INTERVAL n UNIT")
    
    # Fix 16: element_at() is compatible between Trino and Spark!
    
    # Fix 17: json_extract() with CAST → from_json() - complex, add TODO
    json_extract_pattern = r'CAST\s*\(\s*json_extract\s*\([^)]+\)\s+AS\s+ARRAY'
    if re.search(json_extract_pattern, fixed_sql, flags=re.IGNORECASE):
        fixes_applied.append("TODO: Review json_extract() with CAST - may need from_json() with schema")
    
    # Fix 18: WITH (format = 'X') → USING format
    with_format_pattern = r"WITH\s*\(\s*format\s*=\s*'(PARQUET|ORC|AVRO)'\s*\)"
    if re.search(with_format_pattern, fixed_sql, flags=re.IGNORECASE):
        format_type = re.search(with_format_pattern, fixed_sql, flags=re.IGNORECASE).group(1)
        fixed_sql = re.sub(with_format_pattern, 'USING ICEBERG', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append(f"Converted WITH (format = '{format_type}') to USING ICEBERG")
    
    return fixed_sql, fixes_applied


def try_original_query(connection: Connection, query: str, table_name: str) -> Dict:
    """
    Try to validate the Trino query after auto-fixes with EXPLAIN.
    Returns success/failure status.
    """
    result = {
        'table_name': table_name,
        'original_works': False,
        'error': None,
        'explain_output': None
    }
    
    try:
        with connection.cursor() as cursor:
            # For SELECT statements, try EXPLAIN directly
            if query.strip().upper().startswith('SELECT'):
                explain_query = f"EXPLAIN {query.rstrip(';')}"
                cursor.execute(explain_query)
                explain_results = cursor.fetchall()
                result['explain_output'] = '\n'.join([str(row[0]) for row in explain_results[:5]])
                result['original_works'] = True
            # For CREATE statements, extract AS SELECT portion
            elif 'AS SELECT' in query.upper() or 'AS\nSELECT' in query.upper():
                match = re.search(r'\bAS\s+(SELECT\b.*)', query, flags=re.IGNORECASE | re.DOTALL)
                if match:
                    select_statement = match.group(1).rstrip(';').strip()
                    explain_query = f"EXPLAIN {select_statement}"
                    cursor.execute(explain_query)
                    explain_results = cursor.fetchall()
                    result['explain_output'] = '\n'.join([str(row[0]) for row in explain_results[:5]])
                    result['original_works'] = True
            else:
                # Try to explain the whole thing
                explain_query = f"EXPLAIN {query.rstrip(';')}"
                cursor.execute(explain_query)
                explain_results = cursor.fetchall()
                result['explain_output'] = '\n'.join([str(row[0]) for row in explain_results[:5]])
                result['original_works'] = True
                
    except Exception as e:
        result['error'] = str(e)
        result['original_works'] = False
    
    return result


def convert_with_ai(connection: Connection, query: str, table_name: str, error_msg: str) -> Dict:
    """
    Use AI_QUERY to convert Trino SQL to Databricks SQL.
    """
    result = {
        'converted_sql': None,
        'conversion_error': None,
        'validation_works': False,
        'validation_error': None
    }
    
    # Escape single quotes for AI_QUERY
    escaped_query = query.replace("'", "''")
    
    # Truncate if too long
    if len(escaped_query) > 3000:
        escaped_query = escaped_query[:3000]
    
    try:
        with connection.cursor() as cursor:
            # Use AI_QUERY to convert
            conversion_prompt = f"""You are a SQL converter. Convert this Trino SQL to Databricks Spark SQL.

ERROR: {error_msg[:200]}

CRITICAL RULES:
- Return ONLY executable SQL code
- NO explanations, NO markdown, NO commentary
- Apply these conversions:
  * VARCHAR → STRING
  * cardinality() → size()
  * json_extract_scalar() → get_json_object()
  * array_agg() → collect_list()
  * date_add('day', n, date) → date_add(date, n)
  * date_diff('day', d1, d2) → datediff(d2, d1)
  * CROSS JOIN UNNEST() → LATERAL VIEW explode()
  * approx_percentile() → percentile_approx()
  * ROW() → STRUCT()

INPUT TRINO SQL:
{escaped_query}

OUTPUT (SQL only):"""

            # Escape the entire prompt for SQL string
            escaped_prompt = conversion_prompt.replace("'", "''")

            ai_query = f"""
            SELECT AI_QUERY(
                'databricks-claude-sonnet-4-5',
                '{escaped_prompt}'
            ) as converted_sql
            """
            
            cursor.execute(ai_query)
            ai_result = cursor.fetchone()
            
            if ai_result and ai_result[0]:
                converted_sql = ai_result[0]
                # Clean up the response (remove markdown if present)
                converted_sql = re.sub(r'^```sql\s*', '', converted_sql, flags=re.MULTILINE)
                converted_sql = re.sub(r'```\s*$', '', converted_sql, flags=re.MULTILINE)
                result['converted_sql'] = converted_sql.strip()
            else:
                result['conversion_error'] = "AI_QUERY returned empty result"
                return result
                
    except Exception as e:
        result['conversion_error'] = f"AI_QUERY conversion failed: {str(e)}"
        return result
    
    # Validate the converted SQL with EXPLAIN
    try:
        with connection.cursor() as cursor:
            converted_sql = result['converted_sql']
            
            if converted_sql.strip().upper().startswith('SELECT'):
                explain_query = f"EXPLAIN {converted_sql.rstrip(';')}"
                cursor.execute(explain_query)
                result['validation_works'] = True
            elif 'AS SELECT' in converted_sql.upper() or 'AS\nSELECT' in converted_sql.upper():
                match = re.search(r'\bAS\s+(SELECT\b.*)', converted_sql, flags=re.IGNORECASE | re.DOTALL)
                if match:
                    select_statement = match.group(1).rstrip(';').strip()
                    explain_query = f"EXPLAIN {select_statement}"
                    cursor.execute(explain_query)
                    result['validation_works'] = True
            else:
                explain_query = f"EXPLAIN {converted_sql.rstrip(';')}"
                cursor.execute(explain_query)
                result['validation_works'] = True
                
    except Exception as validation_error:
        result['validation_error'] = f"Converted SQL validation failed: {str(validation_error)}"
        result['validation_works'] = False
    
    return result


def process_query(connection: Connection, query: str, table_name: str) -> Dict:
    """
    Process a single query: try auto-fixes, convert if needed.
    """
    result = {
        'table_name': table_name,
        'original_sql': query,
        'final_sql': None,
        'status': 'unknown',
        'conversion_notes': []
    }
    
    print(f"  {Colors.YELLOW}Step 1: Applying auto-fixes...{Colors.RESET}", end=" ")
    
    # Apply automatic fixes
    fixed_sql, fixes_applied = apply_trino_to_databricks_fixes(query)
    
    if fixes_applied:
        print(f"{Colors.GREEN}✓ Applied {len(fixes_applied)} fix(es){Colors.RESET}")
    else:
        print(f"{Colors.CYAN}✓ No fixes needed{Colors.RESET}")
    
    # Test the fixed SQL
    print(f"  {Colors.YELLOW}Step 2: Validating with EXPLAIN...{Colors.RESET}", end=" ")
    validation_result = try_original_query(connection, fixed_sql, table_name)
    
    if validation_result['original_works']:
        print(f"{Colors.GREEN}✓ Valid!{Colors.RESET}")
        result['final_sql'] = fixed_sql
        result['status'] = 'auto_fixed' if fixes_applied else 'unchanged'
        for fix in fixes_applied:
            result['conversion_notes'].append(f"Auto-fix: {fix}")
        if not fixes_applied:
            result['conversion_notes'].append("Query is already Databricks-compatible")
        return result
    else:
        print(f"{Colors.RED}✗ Failed{Colors.RESET}")
        if validation_result.get('error'):
            print(f"    {Colors.RED}Error: {validation_result['error'][:100]}...{Colors.RESET}")
    
    # Try AI conversion
    print(f"  {Colors.YELLOW}Step 3: Converting with AI_QUERY...{Colors.RESET}", end=" ")
    
    conversion_result = convert_with_ai(connection, query, table_name, validation_result['error'])
    
    if conversion_result['conversion_error']:
        print(f"{Colors.RED}✗ Conversion failed{Colors.RESET}")
        result['status'] = 'failed'
        result['conversion_notes'].append(f"AI conversion failed: {conversion_result['conversion_error']}")
        return result
    
    print(f"{Colors.GREEN}✓ Converted{Colors.RESET}")
    
    # Validate converted SQL
    print(f"  {Colors.YELLOW}Step 4: Validating converted SQL...{Colors.RESET}", end=" ")
    
    if conversion_result['validation_works']:
        print(f"{Colors.GREEN}✓ Valid!{Colors.RESET}")
        result['final_sql'] = conversion_result['converted_sql']
        result['status'] = 'ai_converted'
        result['conversion_notes'].append("Auto-fixes failed - successfully converted with AI_QUERY")
    else:
        print(f"{Colors.RED}✗ Validation failed{Colors.RESET}")
        result['status'] = 'failed'
        result['conversion_notes'].append(f"Conversion validation failed: {conversion_result['validation_error']}")
    
    return result


def process_trino_file(connection: Connection, file_path: Path, output_dir: Path) -> List[Dict]:
    """
    Process a single Trino SQL file: try auto-fixes, convert what's needed.
    """
    print(f"\n{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}Processing: {file_path.name}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    with open(file_path, 'r') as f:
        trino_content = f.read()
    
    statements = extract_statements(trino_content)
    results = []
    output_statements = []
    
    for i, stmt_info in enumerate(statements, 1):
        table_name = stmt_info['table_name']
        query = stmt_info['sql']
        
        print(f"{Colors.YELLOW}{Colors.BOLD}[{i}/{len(statements)}] Processing: {table_name}{Colors.RESET}")
        result = process_query(connection, query, table_name)
        result['file'] = file_path.stem
        results.append(result)
        
        # Add to output
        if result['final_sql']:
            output_statements.append(f"-- Table/Query: {table_name}")
            output_statements.append(f"-- Original file: {file_path.name}")
            output_statements.append(f"-- Status: {result['status'].upper().replace('_', ' ')}")
            for note in result['conversion_notes']:
                output_statements.append(f"-- Note: {note}")
            output_statements.append("")
            output_statements.append(result['final_sql'])
            output_statements.append("")
            output_statements.append("-" * 80)
            output_statements.append("")
        
        print()
    
    # Save to file
    if output_statements:
        output_file = output_dir / f"{file_path.stem}_databricks.sql"
        with open(output_file, 'w') as f:
            f.write('\n'.join(output_statements))
        print(f"{Colors.GREEN}✓ Saved Databricks SQL to: {output_file.name}{Colors.RESET}")
    
    return results


def print_summary(all_results: List[Dict]):
    """Print a summary of processing results."""
    total = len(all_results)
    
    if total == 0:
        print(f"\n{Colors.YELLOW}No queries were processed. Check that SQL files contain valid statements.{Colors.RESET}\n")
        return
    
    unchanged = sum(1 for r in all_results if r['status'] == 'unchanged')
    auto_fixed = sum(1 for r in all_results if r['status'] == 'auto_fixed')
    ai_converted = sum(1 for r in all_results if r['status'] == 'ai_converted')
    failed = sum(1 for r in all_results if r['status'] == 'failed')
    
    print(f"\n{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}TRINO → DATABRICKS CONVERSION SUMMARY{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    print(f"Total queries processed: {Colors.BOLD}{total}{Colors.RESET}")
    print(f"Already compatible: {Colors.GREEN}{Colors.BOLD}{unchanged}{Colors.RESET} ({(unchanged/total)*100:.1f}%)")
    print(f"Auto-fixed: {Colors.CYAN}{Colors.BOLD}{auto_fixed}{Colors.RESET} ({(auto_fixed/total)*100:.1f}%)")
    print(f"AI-converted successfully: {Colors.CYAN}{Colors.BOLD}{ai_converted}{Colors.RESET} ({(ai_converted/total)*100:.1f}%)")
    print(f"Failed (needs manual review): {Colors.RED}{Colors.BOLD}{failed}{Colors.RESET} ({(failed/total)*100:.1f}%)")
    print(f"\nSuccess rate: {Colors.BOLD}{((unchanged + auto_fixed + ai_converted)/total)*100:.1f}%{Colors.RESET}\n")
    
    if failed > 0:
        print(f"{Colors.RED}{Colors.BOLD}Queries that need manual review:{Colors.RESET}\n")
        for result in all_results:
            if result['status'] == 'failed':
                print(f"  {Colors.RED}✗ {result['file']} - {result['table_name']}{Colors.RESET}")
                for note in result['conversion_notes']:
                    print(f"    {note}")
                print()
    else:
        print(f"{Colors.GREEN}{Colors.BOLD}🎉 All queries converted successfully!{Colors.RESET}")
        print(f"{Colors.GREEN}Your SQL is ready for Databricks!{Colors.RESET}\n")


def save_detailed_results(all_results: List[Dict], output_file: Path):
    """Save detailed processing results to a file."""
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("Smart Trino to Databricks SQL Conversion Results\n")
        f.write("Strategy: Auto-fix first, AI-convert only what fails\n")
        f.write("=" * 80 + "\n\n")
        
        for result in all_results:
            f.write(f"\nFile: {result['file']}\n")
            f.write(f"Table/Query: {result['table_name']}\n")
            f.write(f"Status: {result['status'].upper().replace('_', ' ')}\n")
            f.write("-" * 80 + "\n")
            
            for note in result['conversion_notes']:
                f.write(f"\nNote: {note}\n")
            
            if result['final_sql']:
                f.write(f"\nFinal SQL:\n{result['final_sql']}\n")
            
            f.write("\n" + "=" * 80 + "\n")


def main():
    """Main execution function."""
    # Configuration
    DATABRICKS_PROFILE = os.getenv('DATABRICKS_PROFILE', 'fe')
    WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')
    
    # Get script directory and set up paths
    script_dir = Path(__file__).parent.parent
    trino_dir = script_dir / 'trino_original'
    output_dir = script_dir / 'databricks_from_trino'
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    # Find all .sql files
    trino_files = sorted(trino_dir.glob('*.sql'))
    
    if not trino_files:
        print(f"{Colors.RED}No Trino SQL files found in {trino_dir}{Colors.RESET}")
        return
    
    if not WAREHOUSE_ID:
        print(f"{Colors.RED}✗ DATABRICKS_WAREHOUSE_ID environment variable not set{Colors.RESET}")
        print(f"{Colors.YELLOW}Set it with: export DATABRICKS_WAREHOUSE_ID=your_warehouse_id{Colors.RESET}")
        return
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Smart Trino → Databricks SQL Converter{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Strategy: Auto-fix first, AI-convert only what fails{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"\n{Colors.CYAN}Found {len(trino_files)} Trino SQL file(s) to process{Colors.RESET}")
    
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
        
    except Exception as e:
        print(f"{Colors.RED}✗ Failed to connect: {e}{Colors.RESET}")
        return
    
    # Process all Trino SQL files
    all_results = []
    try:
        for trino_file in trino_files:
            file_results = process_trino_file(connection, trino_file, output_dir)
            all_results.extend(file_results)
    finally:
        if connection:
            connection.close()
            print(f"\n{Colors.YELLOW}Connection closed{Colors.RESET}")
    
    # Print summary
    print_summary(all_results)
    
    # Save detailed results to file
    results_file = output_dir / 'trino_conversion_results.txt'
    save_detailed_results(all_results, results_file)
    
    print(f"\n{Colors.BLUE}Databricks SQL files saved to: {output_dir}/{Colors.RESET}")
    print(f"{Colors.BLUE}Detailed results saved to: {results_file}{Colors.RESET}\n")


if __name__ == '__main__':
    main()
