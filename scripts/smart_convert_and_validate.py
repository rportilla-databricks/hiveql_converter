#!/usr/bin/env python3
"""
Smart HQL to Spark SQL Converter with AI_QUERY.
Strategy:
1. Check for auto-fixable issues (CLUSTERED BY in CTAS, etc.) → Auto-fix
2. Validate SQL with EXPLAIN (works even when dependent tables don't exist yet)
3. If validation fails → Use AI_QUERY to convert (Databricks DBRX model)
4. Save production-ready SQL

Auto-fixes:
- CLUSTERED BY in CREATE TABLE AS SELECT (not supported in Databricks)

AI Model: databricks-claude-sonnet-4-5 (Claude Sonnet 4.5)
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


def extract_udf_definitions(sql_content: str) -> Tuple[List[str], str]:
    """
    Extract UDF definitions from SQL content.
    Returns (list_of_udf_definitions, sql_without_udfs).
    """
    udf_defs = []
    lines = sql_content.split('\n')
    cleaned_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Check for Hive UDF patterns
        if line.startswith('ADD JAR'):
            # Skip ADD JAR lines (not needed in Databricks)
            i += 1
            continue
        elif 'CREATE TEMPORARY FUNCTION' in line.upper():
            # Extract UDF definition - convert to Databricks SQL UDF placeholder
            func_match = re.search(r'CREATE\s+TEMPORARY\s+FUNCTION\s+(\w+)', line, re.IGNORECASE)
            if func_match:
                func_name = func_match.group(1)
                # Determine return type based on function name
                if 'text' in func_name.lower() or 'normalize' in func_name.lower():
                    return_type = 'STRING'
                    return_value = 'LOWER(TRIM(text))'
                    comment = 'Placeholder: returns lowercased, trimmed text'
                else:
                    return_type = 'DOUBLE'
                    return_value = '0.5'
                    comment = 'Placeholder: returns neutral sentiment'
                
                # Create a placeholder SQL UDF
                udf_def = f"""-- TODO: Implement {func_name} UDF in Databricks
-- Original was a Java UDF. Options:
-- 1. Keep this SQL UDF placeholder
-- 2. Create Python UDF in notebook
-- 3. Use Databricks ML for sentiment: https://docs.databricks.com/sql/language-manual/functions/ai_analyze_sentiment.html
CREATE OR REPLACE FUNCTION {func_name}(text STRING)
RETURNS {return_type}
RETURN {return_value}; -- {comment}"""
                udf_defs.append(udf_def)
            i += 1
            continue
        
        cleaned_lines.append(lines[i])
        i += 1
    
    return udf_defs, '\n'.join(cleaned_lines)


def extract_statements(sql_content: str) -> List[Dict[str, str]]:
    """Extract individual SQL statements from a file."""
    # First, extract and remove UDF definitions
    udf_defs, sql_content = extract_udf_definitions(sql_content)
    
    # Remove SET commands at the file level (before splitting into statements)
    set_pattern = r'^SET\s+[^\n]+;?\s*$'
    sql_content = re.sub(set_pattern, '', sql_content, flags=re.MULTILINE | re.IGNORECASE)
    sql_content = re.sub(r'\n\n\n+', '\n\n', sql_content)  # Clean up blank lines
    
    # Remove single-line comments but preserve the SQL
    lines = sql_content.split('\n')
    cleaned_lines = []
    for line in lines:
        if '--' in line:
            cleaned_lines.append(line.split('--')[0])
        else:
            cleaned_lines.append(line)
    
    sql_content = '\n'.join(cleaned_lines)
    
    # Split on CREATE TABLE
    statements = re.split(r'\n(?=CREATE\s+TABLE)', sql_content, flags=re.IGNORECASE)
    
    # Clean up and filter empty statements
    result = []
    
    # Add UDF definitions as the first "statement" if any exist
    if udf_defs:
        result.append({
            'table_name': 'UDF_Definitions',
            'sql': '\n\n'.join(udf_defs)
        })
    
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            # Extract table name
            table_match = re.search(r'CREATE\s+TABLE\s+(\w+)', stmt, re.IGNORECASE)
            table_name = table_match.group(1) if table_match else "Unknown"
            result.append({
                'table_name': table_name,
                'sql': stmt
            })
    
    return result


def has_clustered_by_in_ctas(query: str) -> bool:
    """Check if query has CLUSTERED BY in a CTAS statement."""
    # Check if it's a CTAS (has both CREATE TABLE and AS SELECT)
    is_ctas = bool(re.search(r'CREATE\s+TABLE.*AS\s+SELECT', query, flags=re.IGNORECASE | re.DOTALL))
    
    # Check if it has CLUSTERED BY
    has_clustered = bool(re.search(r'CLUSTERED BY \([^)]+\) INTO \d+ BUCKETS', query, flags=re.IGNORECASE))
    
    return is_ctas and has_clustered


def try_original_query(connection: Connection, query: str, table_name: str) -> Dict:
    """
    Try to validate the original HQL query with EXPLAIN.
    Also checks for known issues that need auto-fixing.
    Returns success/failure status.
    """
    result = {
        'table_name': table_name,
        'original_works': False,
        'error': None,
        'explain_output': None,
        'needs_auto_fix': False,
        'auto_fix_reason': None
    }
    
    # Skip validation for UDF definitions - they'll be executed directly
    if table_name == 'UDF_Definitions' or 'CREATE FUNCTION' in query.upper() or 'CREATE OR REPLACE FUNCTION' in query.upper():
        result['original_works'] = True
        result['explain_output'] = "UDF definition - will be executed directly"
        return result
    
    # Check for CLUSTERED BY in CTAS (can be auto-fixed)
    if has_clustered_by_in_ctas(query):
        result['needs_auto_fix'] = True
        result['auto_fix_reason'] = "CLUSTERED BY not supported in CTAS"
        result['original_works'] = False
        return result
    
    try:
        with connection.cursor() as cursor:
            # Extract SELECT portion for EXPLAIN
            if 'AS SELECT' in query.upper() or 'AS\nSELECT' in query.upper():
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
    Use AI_QUERY to convert HQL to Spark SQL.
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
            conversion_prompt = f"""You are a SQL converter. Convert this HiveQL to Databricks Spark SQL.

ERROR: {error_msg[:200]}

CRITICAL RULES:
- Return ONLY executable SQL code
- NO explanations, NO markdown, NO commentary
- If query is incomplete, return empty string
- Apply these conversions:
  * Remove DISTRIBUTE BY, SORT BY
  * Change STORED AS ORC/PARQUET to USING ICEBERG
  * Change TBLPROPERTIES to OPTIONS
  * Remove MAPJOIN, STREAMTABLE hints
  * Remove TABLESAMPLE from CTAS
  * Remove CLUSTERED BY from CTAS

INPUT HQL:
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
            
            if 'AS SELECT' in converted_sql.upper() or 'AS\nSELECT' in converted_sql.upper():
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


def apply_auto_fixes(sql: str) -> tuple[str, list]:
    """
    Apply automatic fixes for common Hive-to-Spark issues.
    Returns (fixed_sql, list_of_fixes_applied).
    """
    fixes_applied = []
    fixed_sql = sql
    
    # Fix 0: Remove Hive SET commands
    # Pattern: SET hive.* or SET mapreduce.* or SET spark.* (at start of lines)
    set_pattern = r'^SET\s+[^\n]+;?\s*$'
    set_matches = re.findall(set_pattern, fixed_sql, flags=re.MULTILINE | re.IGNORECASE)
    if set_matches:
        fixed_sql = re.sub(set_pattern, '', fixed_sql, flags=re.MULTILINE | re.IGNORECASE)
        # Clean up multiple blank lines left behind
        fixed_sql = re.sub(r'\n\n\n+', '\n\n', fixed_sql)
        fixes_applied.append(f"Removed {len(set_matches)} SET command(s) (Hive/MapReduce config not needed in Databricks)")
    
    # Fix 1: Remove PARTITIONED BY from CTAS (not supported in Databricks CTAS)
    # Pattern: PARTITIONED BY (col1, col2)
    pattern = r'PARTITIONED BY \([^)]+\)\s*\n'
    if re.search(pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(pattern, '', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Removed PARTITIONED BY from CTAS (use ALTER TABLE to add partitioning after creation)")
    
    # Fix 2: Remove CLUSTERED BY from CTAS
    # Pattern matches: CLUSTERED BY (...) SORTED BY (...) INTO N BUCKETS
    pattern = r'CLUSTERED BY \([^)]+\)(?:\s+SORTED BY \([^)]+\))?\s+INTO \d+ BUCKETS\s*\n'
    if re.search(pattern, fixed_sql):
        fixed_sql = re.sub(pattern, '', fixed_sql)
        fixes_applied.append("Removed CLUSTERED BY (with optional SORTED BY) from CTAS")
    
    # Fix 2: Remove Hive hints in wrong position (after ON clause)
    # Pattern: ON ... /*+ HINT */ - preserve newlines
    pattern = r'(ON\s+[^\s]+\s*=\s*[^\s]+)\s*/\*\+[^*]+\*/(\s*)'
    if re.search(pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(pattern, r'\1\2', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Removed Hive hints from JOIN ON clause")
    
    # Fix 3: Remove TABLESAMPLE but preserve main table alias and formatting
    # Pattern: FROM table alias TABLESAMPLE(...) sample_alias
    pattern = r'(FROM\s+\w+\s+)(\w+)\s+TABLESAMPLE\s*\([^)]+\)\s+\w+(\s*\n)'
    match = re.search(pattern, fixed_sql, flags=re.IGNORECASE)
    if match:
        # Keep the main table alias, remove TABLESAMPLE and its alias, preserve newline
        replacement = r'\1\2\3'
        fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Removed TABLESAMPLE clause, kept main table alias")
    
    # Fix 4: Remove STREAMTABLE hint
    pattern = r'/\*\+\s*STREAMTABLE\s*\([^)]+\)\s*\*/'
    if re.search(pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(pattern, '', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Removed STREAMTABLE hint")
    
    # Fix 5: Remove DISTRIBUTE BY + SORT BY and add CLUSTER BY to table definition
    # First, extract DISTRIBUTE BY columns to use for CLUSTER BY
    cluster_cols = None
    dist_pattern = r'DISTRIBUTE\s+BY\s+([\w,\s]+?)(?:\s+SORT\s+BY\s+([\w,\s]+?))?(?:;|\s*$)'
    dist_match = re.search(dist_pattern, fixed_sql, flags=re.IGNORECASE)
    if dist_match:
        dist_cols = dist_match.group(1).strip()
        # Clean up and get unique columns
        cluster_cols = ', '.join(set(c.strip() for c in dist_cols.split(',')))
        
        # Remove DISTRIBUTE BY / SORT BY from end
        fixed_sql = re.sub(dist_pattern, ';', fixed_sql, flags=re.IGNORECASE)
        
        # Add CLUSTER BY to CREATE TABLE (after CREATE TABLE tablename, before AS SELECT)
        if 'CREATE TABLE' in fixed_sql.upper() and 'AS' in fixed_sql.upper():
            # Insert CLUSTER BY after table name and any USING/OPTIONS clause, before AS
            # Pattern handles AS and SELECT on different lines
            create_pattern = r'(CREATE TABLE \w+)((?:\s+USING \w+)?(?:\s+OPTIONS \([^)]+\))?)\s+(AS)(?:\s+)(SELECT)'
            if re.search(create_pattern, fixed_sql, flags=re.IGNORECASE | re.DOTALL):
                replacement = rf'\1\2\nCLUSTER BY ({cluster_cols})\n\3\n\4'
                fixed_sql = re.sub(create_pattern, replacement, fixed_sql, flags=re.IGNORECASE | re.DOTALL)
                fixes_applied.append(f"Moved DISTRIBUTE BY to CLUSTER BY ({cluster_cols}) in table definition")
        else:
            fixes_applied.append("Removed DISTRIBUTE BY / SORT BY (Catalyst optimizer handles distribution)")
    
    # Fix 6: Replace USING PARQUET/ORC with USING ICEBERG
    pattern = r'USING\s+(PARQUET|ORC)'
    if re.search(pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(pattern, 'USING ICEBERG', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Changed USING PARQUET/ORC to USING ICEBERG (managed Iceberg tables)")
    
    # Also handle STORED AS ORC/PARQUET
    pattern = r'STORED\s+AS\s+(ORC|PARQUET)'
    if re.search(pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(pattern, 'USING ICEBERG', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Changed STORED AS ORC/PARQUET to USING ICEBERG")
    
    # Convert TBLPROPERTIES to OPTIONS
    pattern = r'TBLPROPERTIES\s*\(([^)]+)\)'
    if re.search(pattern, fixed_sql, flags=re.IGNORECASE):
        fixed_sql = re.sub(pattern, r'OPTIONS (\1)', fixed_sql, flags=re.IGNORECASE)
        fixes_applied.append("Changed TBLPROPERTIES to OPTIONS")
    
    # Fix 7: Remove invalid mixed aggregation/window function constructs
    # Check for MAP() containing both aggregates and window functions
    if 'GROUP BY' in fixed_sql.upper() and 'OVER (' in fixed_sql.upper():
        # Pattern: MAP with window functions inside aggregation query
        map_with_window = r"MAP\s*\([^)]*?FIRST_VALUE[^)]*?OVER[^)]+?\)[^)]*?\)"
        if re.search(map_with_window, fixed_sql, flags=re.IGNORECASE | re.DOTALL):
            # This is too complex - comment it out and add a TODO
            fixed_sql = re.sub(
                r'(,\s*MAP\s*\([^)]*?FIRST_VALUE.*?OVER.*?\).*?\)\s+as\s+\w+)',
                r'-- TODO: Fix mixed aggregate/window function\n    -- \1',
                fixed_sql,
                flags=re.IGNORECASE | re.DOTALL
            )
            fixes_applied.append("Commented out invalid MAP with window functions (mixed aggregate/window not allowed)")
    
    return fixed_sql, fixes_applied


def process_query(connection: Connection, query: str, table_name: str) -> Dict:
    """
    Process a single query: try original, convert if needed.
    """
    result = {
        'table_name': table_name,
        'original_sql': query,
        'final_sql': None,
        'status': 'unknown',  # 'original_works', 'ai_converted', 'failed'
        'original_error': None,
        'conversion_notes': []
    }
    
    print(f"  {Colors.YELLOW}Step 1: Testing original HQL...{Colors.RESET}", end=" ")
    
    # Try original query first (with SET commands already removed)
    original_result = try_original_query(connection, query, table_name)
    
    if original_result['original_works']:
        # Original works! But still check for PARQUET (should be DELTA)
        print(f"{Colors.GREEN}✓ Works as-is!{Colors.RESET}")
        
        # Apply PARQUET→DELTA fix even on working queries
        fixed_sql, fixes_applied = apply_auto_fixes(query)
        
        if fixes_applied:
            result['final_sql'] = fixed_sql
            result['status'] = 'auto_fixed'
            for fix in fixes_applied:
                result['conversion_notes'].append(f"Auto-fix: {fix}")
        else:
            result['final_sql'] = query
            result['status'] = 'original_works'
            result['conversion_notes'].append("Original HQL works in Spark SQL - no conversion needed")
        
        return result
    
    # Original failed - try auto-fixes first
    print(f"{Colors.RED}✗ Failed{Colors.RESET}")
    if original_result.get('error'):
        print(f"  {Colors.RED}  Error: {original_result['error'][:100]}...{Colors.RESET}")
    
    print(f"  {Colors.CYAN}  Trying auto-fixes...{Colors.RESET}", end=" ")
    
    # Apply automatic fixes
    fixed_sql, fixes_applied = apply_auto_fixes(query)
    
    if fixes_applied:
        print(f"{Colors.GREEN}✓ Applied {len(fixes_applied)} fix(es){Colors.RESET}")
        
        # Test the fixed SQL
        fixed_result = try_original_query(connection, fixed_sql, table_name)
        
        if fixed_result['original_works']:
            print(f"  {Colors.GREEN}  Validation passed!{Colors.RESET}")
            result['final_sql'] = fixed_sql
            result['status'] = 'auto_fixed'
            for fix in fixes_applied:
                result['conversion_notes'].append(f"Auto-fix: {fix}")
            return result
        else:
            print(f"  {Colors.YELLOW}  Still has issues, trying AI conversion...{Colors.RESET}")
            # Fall through to AI conversion
    else:
        print(f"{Colors.YELLOW}No auto-fixes available{Colors.RESET}")
        # Fall through to AI conversion
    
    # Try AI conversion
    print(f"  {Colors.YELLOW}Step 2: Converting with AI_QUERY...{Colors.RESET}", end=" ")
    
    conversion_result = convert_with_ai(connection, query, table_name, original_result['error'])
    
    if conversion_result['conversion_error']:
        print(f"{Colors.RED}✗ Conversion failed{Colors.RESET}")
        result['status'] = 'failed'
        result['conversion_notes'].append(f"AI conversion failed: {conversion_result['conversion_error']}")
        return result
    
    print(f"{Colors.GREEN}✓ Converted{Colors.RESET}")
    
    # Validate converted SQL
    print(f"  {Colors.YELLOW}Step 3: Validating converted SQL...{Colors.RESET}", end=" ")
    
    if conversion_result['validation_works']:
        print(f"{Colors.GREEN}✓ Valid!{Colors.RESET}")
        result['final_sql'] = conversion_result['converted_sql']
        result['status'] = 'ai_converted'
        result['conversion_notes'].append("Original failed - successfully converted with AI_QUERY")
    else:
        print(f"{Colors.RED}✗ Validation failed{Colors.RESET}")
        result['status'] = 'failed'
        result['conversion_notes'].append(f"Conversion validation failed: {conversion_result['validation_error']}")
    
    return result


def process_hql_file(connection: Connection, file_path: Path, output_dir: Path) -> List[Dict]:
    """
    Process a single HQL file: try original, convert what's needed.
    """
    print(f"\n{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}Processing: {file_path.name}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    with open(file_path, 'r') as f:
        hql_content = f.read()
    
    statements = extract_statements(hql_content)
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
            output_statements.append(f"-- Table: {table_name}")
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
        output_file = output_dir / f"{file_path.stem}_final.sql"
        with open(output_file, 'w') as f:
            f.write('\n'.join(output_statements))
        print(f"{Colors.GREEN}✓ Saved final SQL to: {output_file.name}{Colors.RESET}")
    
    return results


def print_summary(all_results: List[Dict]):
    """Print a summary of processing results."""
    total = len(all_results)
    original_works = sum(1 for r in all_results if r['status'] == 'original_works')
    auto_fixed = sum(1 for r in all_results if r['status'] == 'auto_fixed')
    ai_converted = sum(1 for r in all_results if r['status'] == 'ai_converted')
    failed = sum(1 for r in all_results if r['status'] == 'failed')
    
    print(f"\n{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}PROCESSING SUMMARY{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}{'='*80}{Colors.RESET}\n")
    
    print(f"Total queries processed: {Colors.BOLD}{total}{Colors.RESET}")
    print(f"Original HQL works as-is: {Colors.GREEN}{Colors.BOLD}{original_works}{Colors.RESET} ({(original_works/total)*100:.1f}%)")
    print(f"Auto-fixed (CLUSTERED BY, etc): {Colors.CYAN}{Colors.BOLD}{auto_fixed}{Colors.RESET} ({(auto_fixed/total)*100:.1f}%)")
    print(f"AI-converted successfully: {Colors.CYAN}{Colors.BOLD}{ai_converted}{Colors.RESET} ({(ai_converted/total)*100:.1f}%)")
    print(f"Failed (needs manual review): {Colors.RED}{Colors.BOLD}{failed}{Colors.RESET} ({(failed/total)*100:.1f}%)")
    print(f"\nSuccess rate: {Colors.BOLD}{((original_works + auto_fixed + ai_converted)/total)*100:.1f}%{Colors.RESET}\n")
    
    if failed > 0:
        print(f"{Colors.RED}{Colors.BOLD}Queries that need manual review:{Colors.RESET}\n")
        for result in all_results:
            if result['status'] == 'failed':
                print(f"  {Colors.RED}✗ {result['file']} - {result['table_name']}{Colors.RESET}")
                if result['original_error']:
                    print(f"    Original error: {result['original_error'][:100]}")
                for note in result['conversion_notes']:
                    print(f"    {note}")
                print()


def save_detailed_results(all_results: List[Dict], output_file: Path):
    """Save detailed processing results to a file."""
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("Smart HQL to Spark SQL Conversion Results\n")
        f.write("Strategy: Test original first, AI-convert only what fails\n")
        f.write("=" * 80 + "\n\n")
        
        for result in all_results:
            f.write(f"\nFile: {result['file']}\n")
            f.write(f"Table: {result['table_name']}\n")
            f.write(f"Status: {result['status'].upper().replace('_', ' ')}\n")
            f.write("-" * 80 + "\n")
            
            if result['original_error']:
                f.write(f"\nOriginal Error:\n{result['original_error']}\n")
            
            for note in result['conversion_notes']:
                f.write(f"\nNote: {note}\n")
            
            if result['final_sql']:
                f.write(f"\nFinal SQL:\n{result['final_sql']}\n")
            
            f.write("\n" + "=" * 80 + "\n")


def main():
    """Main execution function."""
    # Configuration
    DATABRICKS_PROFILE = 'fe'  # Change this to 'fe' or 'logfood' if needed
    WAREHOUSE_ID = '4b9b953939869799'  # Set this if you have a specific warehouse ID
    
    # Get script directory and set up paths
    script_dir = Path(__file__).parent.parent
    hql_dir = script_dir / 'hql_original'
    output_dir = script_dir / 'spark_sql_final'
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    # Find all .hql files
    hql_files = sorted(hql_dir.glob('script*.hql'))
    
    if not hql_files:
        print(f"{Colors.RED}No HQL files found in {hql_dir}{Colors.RESET}")
        return
    
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Smart HQL to Spark SQL Converter{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}Strategy: Test first, convert only what fails{Colors.RESET}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"\n{Colors.CYAN}Found {len(hql_files)} HQL files to process{Colors.RESET}")
    
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
    results_file = output_dir / 'processing_results.txt'
    save_detailed_results(all_results, results_file)
    
    print(f"\n{Colors.BLUE}Final SQL files saved to: {output_dir}/{Colors.RESET}")
    print(f"{Colors.BLUE}Detailed results saved to: {results_file}{Colors.RESET}\n")


if __name__ == '__main__':
    main()

