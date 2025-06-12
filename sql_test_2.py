import re
import sqlparse
from sqlparse import sql, tokens as T
from typing import Dict, List, Optional, Tuple, Set, Any, Union
from collections import OrderedDict
import json
from datetime import datetime
import hashlib
import xml.etree.ElementTree as ET

class TSQLToPandasConverter:
    def __init__(self, connection_var: str = "connection"):
        self.table_aliases = {}
        self.joins = []
        self.pandas_code = []
        self.temp_tables = {}
        self.cte_definitions = {}
        self.variables = {}
        self.connection_var = connection_var
        self.df_counter = 0
        self.subquery_counter = 0
        self.cursor_counter = 0
        self.cursors = {}
        
        # SQL function mappings (enhanced with JSON and STRING_AGG)
        self.sql_functions = {
            'GETDATE': 'pd.Timestamp.now()',
            'DATEPART': self._convert_datepart,
            'DATEDIFF': self._convert_datediff,
            'DATEADD': self._convert_dateadd,
            'CAST': self._convert_cast,
            'CONVERT': self._convert_convert,
            'ISNULL': self._convert_isnull,
            'COALESCE': self._convert_coalesce,
            'LEN': 'len',
            'LOWER': 'str.lower',
            'UPPER': 'str.upper',
            'TRIM': 'str.strip',
            'LTRIM': 'str.lstrip',
            'RTRIM': 'str.rstrip',
            'SUBSTRING': self._convert_substring,
            'CHARINDEX': self._convert_charindex,
            'REPLACE': 'str.replace',
            'ROUND': 'round',
            'ABS': 'abs',
            'CEILING': 'np.ceil',
            'FLOOR': 'np.floor',
            'SQRT': 'np.sqrt',
            'POWER': 'np.power',
            'LOG': 'np.log',
            'EXP': 'np.exp',
            'HASHBYTES': self._convert_hashbytes,
            'IIF': self._convert_iif,
            'OBJECT_ID': self._convert_object_id,
            'STUFF': self._convert_stuff,
            'PATINDEX': self._convert_patindex,
            'QUOTENAME': self._convert_quotename,
            'PARSENAME': self._convert_parsename,
            'CHECKSUM': self._convert_checksum,
            'NEWID': 'lambda: str(uuid.uuid4())',
            'RAND': 'np.random.rand',
            'ASCII': 'ord',
            'CHAR': 'chr',
            'SPACE': self._convert_space,
            'REPLICATE': self._convert_replicate,
            'REVERSE': 'lambda x: x[::-1]',
            'LEFT': self._convert_left,
            'RIGHT': self._convert_right,
            # JSON functions
            'JSON_VALUE': self._convert_json_value,
            'JSON_QUERY': self._convert_json_query,
            'JSON_MODIFY': self._convert_json_modify,
            'ISJSON': self._convert_isjson,
            'OPENJSON': self._convert_openjson,
            # String aggregation
            'STRING_AGG': self._convert_string_agg
        }
        
        # XML function mappings
        self.xml_functions = {
            'value': self._convert_xml_value,
            'query': self._convert_xml_query,
            'exist': self._convert_xml_exist,
            'nodes': self._convert_xml_nodes,
            'modify': self._convert_xml_modify
        }
        
        # Information schema mappings
        self.information_schema_tables = {
            'INFORMATION_SCHEMA.TABLES': self._get_tables_query,
            'INFORMATION_SCHEMA.COLUMNS': self._get_columns_query,
            'INFORMATION_SCHEMA.KEY_COLUMN_USAGE': self._get_key_columns_query,
            'INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS': self._get_foreign_keys_query,
            'INFORMATION_SCHEMA.TABLE_CONSTRAINTS': self._get_constraints_query,
            'INFORMATION_SCHEMA.VIEWS': self._get_views_query,
            'INFORMATION_SCHEMA.ROUTINES': self._get_routines_query,
            'INFORMATION_SCHEMA.PARAMETERS': self._get_parameters_query
        }
        
    def convert_sql_to_pandas(self, sql_query: str) -> str:
        """Main method to convert T-SQL query to Pandas equivalent"""
        self.reset_state()
        
        # Preprocess the SQL
        sql_query = self._preprocess_sql(sql_query)
        
        # Handle multiple statements
        statements = self._split_statements(sql_query)
        all_pandas_code = []
        
        # Add imports
        imports = [
            "import pandas as pd",
            "import numpy as np",
            "from datetime import datetime, timedelta",
            "import hashlib",
            "import xml.etree.ElementTree as ET",
            "import uuid",
            "import json",
            "from sqlalchemy import create_engine, inspect",
            "import re"
        ]
        all_pandas_code.extend(imports)
        all_pandas_code.append("")
        
        for i, statement in enumerate(statements):
            if not statement.strip():
                continue
                
            # Add statement separator
            if i > 0:
                all_pandas_code.append(f"\n# --- Statement {i + 1} ---")
            
            try:
                parsed = sqlparse.parse(statement)[0]
                statement_code = self.process_statement(parsed, statement)
                all_pandas_code.append(statement_code)
            except Exception as e:
                all_pandas_code.append(f"# Error parsing statement: {str(e)}")
                all_pandas_code.append(f"# Original statement: {statement}")
        
        return '\n'.join(all_pandas_code)
    
    def _preprocess_sql(self, sql_query: str) -> str:
        """Preprocess SQL to handle special cases"""
        # Replace SQL Server specific syntax
        sql_query = re.sub(r'\[([^\]]+)\]', r'\1', sql_query)  # Remove square brackets
        sql_query = re.sub(r'""([^""]+)""', r'"\1"', sql_query)  # Normalize quotes
        
        # Handle GO statements
        sql_query = re.sub(r'\bGO\b', ';', sql_query, flags=re.IGNORECASE)
        
        # Handle sp_executesql
        sql_query = re.sub(r'sp_executesql\s+', 'EXEC ', sql_query, flags=re.IGNORECASE)
        
        return sql_query
    
    def _split_statements(self, sql_query: str) -> List[str]:
        """Improved statement splitting"""
        # Use sqlparse for better statement splitting
        statements = sqlparse.split(sql_query)
        
        # Filter out empty statements
        return [stmt.strip() for stmt in statements if stmt.strip()]
    
    def reset_state(self):
        """Reset converter state for new query"""
        self.table_aliases = {}
        self.joins = []
        self.pandas_code = []
        self.temp_tables = {}
        self.cte_definitions = {}
        self.variables = {}
        self.df_counter = 0
        self.subquery_counter = 0
        self.cursor_counter = 0
        self.cursors = {}
    
    def process_statement(self, parsed_statement, original_statement: str) -> str:
        """Process individual SQL statement with improved parsing"""
        statement_type = self._identify_statement_type(parsed_statement)
        
        handlers = {
            'CTE': self.handle_cte_statement,
            'CREATE_TEMP': self.handle_create_temp_table,
            'CREATE_VIEW': self.handle_create_view,
            'CREATE_PROC': self.handle_create_procedure,
            'CREATE_FUNCTION': self.handle_create_function,
            'DROP': self.handle_drop_statement,
            'TRUNCATE': self.handle_truncate_statement,
            'INSERT': self.handle_insert_statement,
            'UPDATE': self.handle_update_statement,
            'DELETE': self.handle_delete_statement,
            'DECLARE': self.handle_declare_statement,
            'SET': self.handle_set_statement,
            'SELECT': self.handle_select_statement,
            'MERGE': self.handle_merge_statement,
            'IF': self.handle_if_statement,
            'WHILE': self.handle_while_statement,
            'PRINT': self.handle_print_statement,
            'EXEC': self.handle_exec_statement,
            'CURSOR': self.handle_cursor_statement,
            'FETCH': self.handle_fetch_statement,
            'OPEN': self.handle_open_cursor,
            'CLOSE': self.handle_close_cursor,
            'DEALLOCATE': self.handle_deallocate_cursor,
            'BEGIN_TRAN': self.handle_begin_transaction,
            'COMMIT': self.handle_commit_transaction,
            'ROLLBACK': self.handle_rollback_transaction,
            'TRY': self.handle_try_catch,
            'THROW': self.handle_throw_statement,
            'RAISERROR': self.handle_raiserror_statement,
            'OPENQUERY': self.handle_openquery_statement,
            'OPENJSON': self.handle_openjson_statement
        }
        
        handler = handlers.get(statement_type, self.handle_unknown_statement)
        return handler(parsed_statement, original_statement)
    
    def _identify_statement_type(self, parsed_statement) -> str:
        """Improved statement type identification using parsed tokens"""
        first_token = None
        for token in parsed_statement.tokens:
            if not token.is_whitespace:
                first_token = token
                break
        
        if not first_token:
            return 'UNKNOWN'
        
        # Check for CTE
        if isinstance(first_token, sql.Token) and first_token.ttype is T.Keyword.CTE:
            return 'CTE'
        
        # Get the first keyword
        keyword = str(first_token).upper()
        
        # Map keywords to statement types
        keyword_map = {
            'WITH': 'CTE',
            'CREATE': self._identify_create_type,
            'DROP': 'DROP',
            'TRUNCATE': 'TRUNCATE',
            'INSERT': 'INSERT',
            'UPDATE': 'UPDATE',
            'DELETE': 'DELETE',
            'DECLARE': self._identify_declare_type,
            'SET': 'SET',
            'SELECT': 'SELECT',
            'MERGE': 'MERGE',
            'IF': 'IF',
            'WHILE': 'WHILE',
            'PRINT': 'PRINT',
            'EXEC': 'EXEC',
            'EXECUTE': 'EXEC',
            'OPEN': 'OPEN',
            'CLOSE': 'CLOSE',
            'FETCH': 'FETCH',
            'DEALLOCATE': 'DEALLOCATE',
            'BEGIN': self._identify_begin_type,
            'COMMIT': 'COMMIT',
            'ROLLBACK': 'ROLLBACK',
            'TRY': 'TRY',
            'THROW': 'THROW',
            'RAISERROR': 'RAISERROR',
            'OPENQUERY': 'OPENQUERY',
            'OPENJSON': 'OPENJSON'
        }
        
        result = keyword_map.get(keyword, 'UNKNOWN')
        
        # Handle sub-types
        if callable(result):
            return result(parsed_statement)
        
        return result
    
    def _identify_create_type(self, parsed_statement) -> str:
        """Identify specific CREATE statement type"""
        statement_str = str(parsed_statement).upper()
        
        if '#' in statement_str or 'TEMP' in statement_str:
            return 'CREATE_TEMP'
        elif 'VIEW' in statement_str:
            return 'CREATE_VIEW'
        elif 'PROCEDURE' in statement_str or 'PROC ' in statement_str:
            return 'CREATE_PROC'
        elif 'FUNCTION' in statement_str:
            return 'CREATE_FUNCTION'
        elif 'INDEX' in statement_str:
            return 'CREATE_INDEX'
        else:
            return 'CREATE_TEMP'  # Default to temp table
    
    def _identify_declare_type(self, parsed_statement) -> str:
        """Identify DECLARE statement type"""
        statement_str = str(parsed_statement).upper()
        
        if 'CURSOR' in statement_str:
            return 'CURSOR'
        else:
            return 'DECLARE'
    
    def _identify_begin_type(self, parsed_statement) -> str:
        """Identify BEGIN statement type"""
        statement_str = str(parsed_statement).upper()
        
        if 'TRAN' in statement_str or 'TRANSACTION' in statement_str:
            return 'BEGIN_TRAN'
        elif 'TRY' in statement_str:
            return 'TRY'
        else:
            return 'BEGIN'
    
    # JSON function converters
    def _convert_json_value(self, args: str) -> str:
        """Convert JSON_VALUE function"""
        parts = self._split_respecting_parens(args, ',')
        if len(parts) >= 2:
            json_col = parts[0].strip()
            json_path = parts[1].strip().strip("'\"")
            
            # Convert SQL JSON path to Python
            # SQL: $.customer.name -> Python: ['customer']['name']
            python_path = self._convert_json_path(json_path)
            
            return f"""(lambda x: json.loads(x){python_path} if pd.notna(x) and x else None)({json_col})"""
        
        return f"# JSON_VALUE({args})"
    
    def _convert_json_query(self, args: str) -> str:
        """Convert JSON_QUERY function"""
        parts = self._split_respecting_parens(args, ',')
        if len(parts) >= 2:
            json_col = parts[0].strip()
            json_path = parts[1].strip().strip("'\"")
            
            python_path = self._convert_json_path(json_path)
            
            return f"""(lambda x: json.dumps(json.loads(x){python_path}) if pd.notna(x) and x else None)({json_col})"""
        
        return f"# JSON_QUERY({args})"
    
    def _convert_json_modify(self, args: str) -> str:
        """Convert JSON_MODIFY function"""
        parts = self._split_respecting_parens(args, ',')
        if len(parts) >= 3:
            json_col = parts[0].strip()
            json_path = parts[1].strip().strip("'\"")
            new_value = parts[2].strip()
            
            # This is more complex and would need custom implementation
            return f"""# JSON_MODIFY - requires custom implementation
# Original: JSON_MODIFY({args})
# Use: df['{json_col}'] = df['{json_col}'].apply(lambda x: modify_json(x, '{json_path}', {new_value}))"""
        
        return f"# JSON_MODIFY({args})"
    
    def _convert_isjson(self, args: str) -> str:
        """Convert ISJSON function"""
        json_col = args.strip()
        
        return f"""(lambda x: 1 if pd.notna(x) and x and (lambda: (json.loads(x), True))()[1] else 0 
                except (json.JSONDecodeError, TypeError): 0)({json_col})"""
    
    def _convert_openjson(self, args: str) -> str:
        """Convert OPENJSON function"""
        parts = self._split_respecting_parens(args, ',')
        json_col = parts[0].strip()
        
        if len(parts) > 1:
            # WITH clause specified
            return f"""# OPENJSON with schema
# pd.json_normalize(json.loads({json_col}))"""
        else:
            # Simple OPENJSON
            return f"""pd.json_normalize(json.loads({json_col}) if pd.notna({json_col}) else {{}})"""
    
    def _convert_json_path(self, json_path: str) -> str:
        """Convert SQL JSON path to Python dictionary access"""
        # Remove $ prefix
        if json_path.startswith('$'):
            json_path = json_path[1:]
        
        # Handle lax/strict mode
        json_path = re.sub(r'^(lax|strict)\s+', '', json_path, flags=re.IGNORECASE)
        
        # Convert path to Python syntax
        if not json_path or json_path == '.':
            return ''
        
        # Split by dots and convert to Python dictionary access
        parts = json_path.strip('.').split('.')
        python_path = ''
        
        for part in parts:
            # Handle array indices
            if '[' in part and ']' in part:
                key = part[:part.index('[')]
                index = part[part.index('['):part.index(']')+1]
                if key:
                    python_path += f"['{key}']{index}"
                else:
                    python_path += index
            else:
                python_path += f"['{part}']"
        
        return python_path
    
    def _convert_string_agg(self, args: str) -> str:
        """Convert STRING_AGG function"""
        parts = self._split_respecting_parens(args, ',')
        
        if len(parts) >= 2:
            column = parts[0].strip()
            separator = parts[1].strip().strip("'\"")
            
            # Check if there's a WITHIN GROUP clause
            within_group_pattern = r'WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\)'
            within_match = re.search(within_group_pattern, args, re.IGNORECASE)
            
            if within_match:
                order_by = within_match.group(1)
                # Extract order column and direction
                order_parts = order_by.strip().split()
                order_col = order_parts[0]
                ascending = True if len(order_parts) < 2 or order_parts[1].upper() != 'DESC' else False
                
                return f".sort_values('{order_col}', ascending={ascending})['{column}'].apply(lambda x: '{separator}'.join(x.dropna().astype(str)))"
            else:
                # No ordering specified
                return f"['{column}'].apply(lambda x: '{separator}'.join(x.dropna().astype(str)))"
        
        return f"# STRING_AGG({args})"
    
    def handle_openjson_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle OPENJSON as a table function"""
        code_lines = []
        code_lines.append("# OPENJSON table function")
        
        # Parse OPENJSON usage
        openjson_pattern = r'OPENJSON\s*\(([^)]+)\)(?:\s+WITH\s*\(([^)]+)\))?(?:\s+AS\s+(\w+))?'
        match = re.search(openjson_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            json_expr = match.group(1)
            with_clause = match.group(2)
            alias = match.group(3)
            
            df_name = alias if alias else f"json_df_{self.df_counter}"
            self.df_counter += 1
            
            code_lines.append(f"# Parse JSON: {json_expr}")
            
            if with_clause:
                # Parse WITH clause for schema
                code_lines.append("# WITH clause defines schema")
                schema_items = [item.strip() for item in with_clause.split(',')]
                
                code_lines.append(f"# Schema definition:")
                for item in schema_items:
                    code_lines.append(f"#   {item}")
                
                code_lines.append(f"{df_name} = pd.json_normalize(json.loads({json_expr}))")
                code_lines.append(f"# Apply schema transformations as needed")
            else:
                code_lines.append(f"{df_name} = pd.json_normalize(json.loads({json_expr}))")
        
        return '\n'.join(code_lines)
    
    def handle_select_statement(self, parsed_statement, original_statement: str) -> str:
        """Enhanced SELECT statement handling with XML, JSON and INFORMATION_SCHEMA support"""
        code_lines = []
        
        # Check for INFORMATION_SCHEMA queries
        if 'INFORMATION_SCHEMA' in original_statement.upper():
            return self._handle_information_schema_query(original_statement)
        
        # Check for FOR XML
        if 'FOR XML' in original_statement.upper():
            return self._handle_xml_query(original_statement)
        
        # Check for FOR JSON
        if 'FOR JSON' in original_statement.upper():
            return self._handle_json_query(original_statement)
        
        # Regular SELECT processing
        code_lines.append("# SELECT statement")
        
        # Extract components
        components = self._extract_query_components_enhanced(parsed_statement)
        
        # Generate Pandas code
        pandas_code = self.generate_pandas_code(components, original_statement)
        code_lines.append(pandas_code)
        
        return '\n'.join(code_lines)
    
    def _handle_json_query(self, query: str) -> str:
        """Handle SELECT ... FOR JSON queries"""
        code_lines = []
        code_lines.append("# SELECT ... FOR JSON query")
        
        # Remove FOR JSON clause for regular processing
        base_query = re.sub(r'FOR\s+JSON\s+\w+.*', '', query, flags=re.IGNORECASE)
        
        # Identify JSON mode
        json_mode_match = re.search(r'FOR\s+JSON\s+(\w+)', query, re.IGNORECASE)
        json_mode = json_mode_match.group(1).upper() if json_mode_match else 'AUTO'
        
        # Check for additional options
        include_null = 'INCLUDE_NULL_VALUES' in query.upper()
        without_array = 'WITHOUT_ARRAY_WRAPPER' in query.upper()
        root_name = None
        root_match = re.search(r'ROOT\s*\(\s*[\'"](\w+)[\'"]\s*\)', query, re.IGNORECASE)
        if root_match:
            root_name = root_match.group(1)
        
        # Convert base query
        code_lines.append("# Convert query to DataFrame first")
        base_code = self.convert_select_query(base_query)
        code_lines.append(f"df = {base_code}")
        code_lines.append("")
        
        # Generate JSON based on mode
        if json_mode == 'AUTO':
            code_lines.extend(self._generate_json_auto(include_null, without_array, root_name))
        elif json_mode == 'PATH':
            code_lines.extend(self._generate_json_path(include_null, without_array, root_name))
        elif json_mode == 'RAW':
            code_lines.extend(self._generate_json_raw(include_null, without_array, root_name))
        
        return '\n'.join(code_lines)
    
    def _generate_json_auto(self, include_null: bool = False, without_array: bool = False, root_name: str = None) -> List[str]:
        """Generate JSON in AUTO mode"""
        code = [
            "# Generate JSON in AUTO mode",
            "# Convert DataFrame to JSON"
        ]
        
        # Build the to_json parameters
        params = ["orient='records'"]
        if not include_null:
            code.append("# Remove null values")
            code.append("df = df.where(pd.notnull(df), None)")
        
        json_line = f"json_result = df.to_json({', '.join(params)})"
        code.append(json_line)
        
        if without_array:
            code.append("# WITHOUT_ARRAY_WRAPPER - return single object if one row")
            code.append("import json")
            code.append("json_data = json.loads(json_result)")
            code.append("if len(json_data) == 1:")
            code.append("    json_result = json.dumps(json_data[0])")
        
        if root_name:
            code.append(f"# Add root element: {root_name}")
            code.append("import json")
            code.append("json_data = json.loads(json_result)")
            code.append(f"json_result = json.dumps({{'{root_name}': json_data}})")
        
        code.append("print(json_result)")
        
        return code
    
    def _generate_json_path(self, include_null: bool = False, without_array: bool = False, root_name: str = None) -> List[str]:
        """Generate JSON in PATH mode"""
        code = [
            "# Generate JSON in PATH mode",
            "# PATH mode allows custom property names using column aliases",
            "import json",
            "",
            "# Build custom JSON structure",
            "json_data = []",
            "for _, row in df.iterrows():",
            "    obj = {}",
            "    for col in df.columns:",
            "        # Handle nested paths in column names (e.g., 'Customer.Name' -> nested object)",
            "        if '.' in col:",
            "            parts = col.split('.')",
            "            current = obj",
            "            for i, part in enumerate(parts[:-1]):",
            "                if part not in current:",
            "                    current[part] = {}",
            "                current = current[part]",
            "            current[parts[-1]] = row[col]",
            "        else:",
            "            obj[col] = row[col]",
            "    json_data.append(obj)",
            "",
        ]
        
        if not include_null:
            code.append("    # Remove null values")
            code.append("    obj = {k: v for k, v in obj.items() if v is not None}")
            code.append("")
        
        if without_array and not root_name:
            code.append("# WITHOUT_ARRAY_WRAPPER")
            code.append("json_result = json.dumps(json_data[0] if len(json_data) == 1 else json_data)")
        elif root_name:
            code.append(f"# Add root element: {root_name}")
            code.append(f"json_result = json.dumps({{'{root_name}': json_data}})")
        else:
            code.append("json_result = json.dumps(json_data)")
        
        code.append("print(json_result)")
        
        return code
    
    def _generate_json_raw(self, include_null: bool = False, without_array: bool = False, root_name: str = None) -> List[str]:
        """Generate JSON in RAW mode - not standard T-SQL but included for completeness"""
        code = [
            "# Generate JSON in RAW mode",
            "# Note: RAW is not a standard T-SQL JSON mode, treating as AUTO",
        ]
        code.extend(self._generate_json_auto(include_null, without_array, root_name))
        return code
    
    def _detect_aggregation(self, expression: str) -> Optional[Dict[str, str]]:
        """Detect aggregation function in expression (enhanced with STRING_AGG)"""
        agg_functions = {
            'COUNT': 'count',
            'SUM': 'sum',
            'AVG': 'mean',
            'MEAN': 'mean',
            'MIN': 'min',
            'MAX': 'max',
            'STDDEV': 'std',
            'STDEV': 'std',
            'VAR': 'var',
            'VARIANCE': 'var',
            'STRING_AGG': 'string_agg'  # Custom handling needed
        }
        
        for sql_func, pandas_func in agg_functions.items():
            pattern = rf'\b{sql_func}\s*\(([^)]+)\)'
            match = re.search(pattern, expression, re.IGNORECASE)
            if match:
                if sql_func == 'STRING_AGG':
                    # STRING_AGG needs special handling
                    args = match.group(1)
                    parts = self._split_respecting_parens(args, ',')
                    column = self.clean_identifier(parts[0].strip()) if parts else 'index'
                    separator = parts[1].strip().strip("'\"") if len(parts) > 1 else ','
                    
                    return {
                        'function': 'apply',
                        'column': column,
                        'custom_func': f"lambda x: '{separator}'.join(x.dropna().astype(str))"
                    }
                else:
                    column = self.clean_identifier(match.group(1).strip())
                    return {
                        'function': pandas_func,
                        'column': column if column != '*' else 'index'
                    }
        
        return None
    
    def _convert_group_by_enhanced(self, group_by_tokens: List[str], 
                                  select_tokens: List[str], 
                                  having_tokens: List[str] = None) -> str:
        """Enhanced GROUP BY with HAVING and STRING_AGG support"""
        group_clause = ' '.join(group_by_tokens).strip()
        group_columns = [self.clean_identifier(col.strip()) for col in group_clause.split(',')]
        
        # Parse SELECT for aggregations
        select_clause = ' '.join(select_tokens).strip()
        select_items = self._parse_select_items_enhanced(select_clause)
        
        # Detect aggregation functions
        agg_dict = {}
        custom_aggs = []
        
        for item in select_items:
            if item['is_expression']:
                agg_info = self._detect_aggregation(item['expression'])
                if agg_info:
                    col_name = item['alias'] or agg_info['column']
                    
                    if 'custom_func' in agg_info:
                        # Handle STRING_AGG and other custom aggregations
                        custom_aggs.append({
                            'column': agg_info['column'],
                            'alias': col_name,
                            'func': agg_info['custom_func']
                        })
                    else:
                        if agg_info['column'] not in agg_dict:
                            agg_dict[agg_info['column']] = []
                        agg_dict[agg_info['column']].append((agg_info['function'], col_name))
        
        # Build groupby code
        if agg_dict or custom_aggs:
            # Create aggregation specification
            agg_spec = {}
            for col, funcs in agg_dict.items():
                if len(funcs) == 1:
                    agg_spec[col] = funcs[0][0]
                else:
                    agg_spec[col] = [f[0] for f in funcs]
            
            if agg_spec:
                group_code = f".groupby({group_columns}).agg({agg_spec})"
            else:
                group_code = f".groupby({group_columns})"
            
            # Add column renaming if needed
            renames = {}
            for col, funcs in agg_dict.items():
                for func, alias in funcs:
                    if alias != col:
                        renames[f"{col}_{func}" if len(funcs) > 1 else col] = alias
            
            if renames:
                group_code += f".rename(columns={renames})"
            
            # Handle custom aggregations (like STRING_AGG)
            for custom_agg in custom_aggs:
                group_code += f".assign({custom_agg['alias']}=lambda x: x.groupby({group_columns})['{custom_agg['column']}'].transform({custom_agg['func']}))"
            
            group_code += ".reset_index()"
        else:
            # No aggregations specified, count by default
            group_code = f".groupby({group_columns}).size().reset_index(name='count')"
        
        # Handle HAVING
        if having_tokens:
            having_condition = self._convert_where_enhanced(having_tokens)
            group_code += f".query('{having_condition}')"
        
        return group_code
    
    # Continue with all existing methods from the original implementation...
    # (All other methods remain the same)
    
    def cursor_counter(self, parsed_statement, original_statement: str) -> str:
        """Handle CURSOR declaration"""
        code_lines = []
        code_lines.append("# Cursor declaration")
        
        # Parse cursor declaration
        cursor_pattern = r'DECLARE\s+(\w+)\s+CURSOR\s+(?:.*?)\s+FOR\s+(SELECT.*)'
        match = re.search(cursor_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            cursor_name = match.group(1)
            select_query = match.group(2)
            
            self.cursor_counter += 1
            cursor_var = f"cursor_{cursor_name.lower()}"
            
            code_lines.append(f"# Cursor: {cursor_name}")
            code_lines.append(f"# Convert cursor to DataFrame iteration")
            
            # Convert SELECT query
            select_code = self.convert_select_query(select_query)
            code_lines.append(f"{cursor_var}_df = {select_code}")
            code_lines.append(f"{cursor_var}_index = 0")
            
            # Store cursor info
            self.cursors[cursor_name] = {
                'var': cursor_var,
                'df_var': f"{cursor_var}_df",
                'index_var': f"{cursor_var}_index"
            }
        else:
            code_lines.append(f"# Could not parse cursor declaration: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_open_cursor(self, parsed_statement, original_statement: str) -> str:
        """Handle OPEN cursor statement"""
        code_lines = []
        
        cursor_pattern = r'OPEN\s+(\w+)'
        match = re.search(cursor_pattern, original_statement, re.IGNORECASE)
        
        if match:
            cursor_name = match.group(1)
            if cursor_name in self.cursors:
                cursor_info = self.cursors[cursor_name]
                code_lines.append(f"# Open cursor: {cursor_name}")
                code_lines.append(f"{cursor_info['index_var']} = 0  # Reset cursor position")
            else:
                code_lines.append(f"# Unknown cursor: {cursor_name}")
        
        return '\n'.join(code_lines)
    
    def handle_fetch_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle FETCH statement"""
        code_lines = []
        
        fetch_pattern = r'FETCH\s+(?:NEXT\s+)?FROM\s+(\w+)(?:\s+INTO\s+(.+))?'
        match = re.search(fetch_pattern, original_statement, re.IGNORECASE)
        
        if match:
            cursor_name = match.group(1)
            into_vars = match.group(2)
            
            if cursor_name in self.cursors:
                cursor_info = self.cursors[cursor_name]
                code_lines.append(f"# Fetch from cursor: {cursor_name}")
                code_lines.append(f"if {cursor_info['index_var']} < len({cursor_info['df_var']}):")
                code_lines.append(f"    cursor_row = {cursor_info['df_var']}.iloc[{cursor_info['index_var']}]")
                
                if into_vars:
                    # Parse INTO variables
                    vars_list = [v.strip() for v in into_vars.split(',')]
                    for i, var in enumerate(vars_list):
                        py_var = self.variables.get(var, var.replace('@', 'var_'))
                        code_lines.append(f"    {py_var} = cursor_row.iloc[{i}] if len(cursor_row) > {i} else None")
                
                code_lines.append(f"    {cursor_info['index_var']} += 1")
                code_lines.append(f"    cursor_fetch_status = 0  # Success")
                code_lines.append(f"else:")
                code_lines.append(f"    cursor_fetch_status = -1  # No more rows")
        
        return '\n'.join(code_lines)
    
    def handle_close_cursor(self, parsed_statement, original_statement: str) -> str:
        """Handle CLOSE cursor statement"""
        cursor_pattern = r'CLOSE\s+(\w+)'
        match = re.search(cursor_pattern, original_statement, re.IGNORECASE)
        
        if match:
            cursor_name = match.group(1)
            return f"# Close cursor: {cursor_name}\n# No action needed in pandas"
        
        return f"# CLOSE cursor statement"
    
    def handle_deallocate_cursor(self, parsed_statement, original_statement: str) -> str:
        """Handle DEALLOCATE cursor statement"""
        cursor_pattern = r'DEALLOCATE\s+(\w+)'
        match = re.search(cursor_pattern, original_statement, re.IGNORECASE)
        
        if match:
            cursor_name = match.group(1)
            if cursor_name in self.cursors:
                cursor_info = self.cursors[cursor_name]
                return f"# Deallocate cursor: {cursor_name}\n# Clean up cursor variables\ndel {cursor_info['df_var']}, {cursor_info['index_var']}"
        
        return f"# DEALLOCATE cursor statement"
    
    def handle_openquery_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle OPENQUERY statement"""
        code_lines = []
        code_lines.append("# OPENQUERY - Distributed query")
        
        # Parse OPENQUERY
        openquery_pattern = r'OPENQUERY\s*\(\s*(\w+)\s*,\s*[\'"](.+?)[\'"]\s*\)'
        match = re.search(openquery_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            linked_server = match.group(1)
            remote_query = match.group(2)
            
            code_lines.append(f"# Linked server: {linked_server}")
            code_lines.append(f"# Remote query: {remote_query}")
            code_lines.append("")
            code_lines.append("# Option 1: Use SQLAlchemy with appropriate connection string")
            code_lines.append(f"# remote_engine = create_engine('connection_string_for_{linked_server}')")
            code_lines.append(f"# df = pd.read_sql_query('''{remote_query}''', remote_engine)")
            code_lines.append("")
            code_lines.append("# Option 2: Use pyodbc directly")
            code_lines.append("# import pyodbc")
            code_lines.append(f"# conn = pyodbc.connect('DSN={linked_server}')")
            code_lines.append(f"# df = pd.read_sql_query('''{remote_query}''', conn)")
            code_lines.append("")
            code_lines.append("# Placeholder DataFrame")
            code_lines.append("df = pd.DataFrame()  # Replace with actual remote query")
        else:
            code_lines.append(f"# Could not parse OPENQUERY: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_create_procedure(self, parsed_statement, original_statement: str) -> str:
        """Handle CREATE PROCEDURE statement"""
        code_lines = []
        
        proc_pattern = r'CREATE\s+PROCEDURE\s+(\w+)(?:\s*\((.*?)\))?\s+AS\s+BEGIN\s+(.*?)\s+END'
        match = re.search(proc_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            proc_name = match.group(1)
            parameters = match.group(2) if match.group(2) else ""
            body = match.group(3)
            
            code_lines.append(f"# Create stored procedure: {proc_name}")
            code_lines.append(f"def sp_{proc_name.lower()}({self._convert_proc_params(parameters)}):")
            code_lines.append("    '''")
            code_lines.append(f"    Converted from SQL Server stored procedure: {proc_name}")
            code_lines.append("    '''")
            code_lines.append("    # Procedure body")
            
            # Add placeholder for body conversion
            body_lines = body.strip().split('\n')
            for line in body_lines[:3]:  # Show first 3 lines
                code_lines.append(f"    # {line.strip()}")
            if len(body_lines) > 3:
                code_lines.append("    # ... (additional logic)")
            
            code_lines.append("    pass  # Implement procedure logic")
        
        return '\n'.join(code_lines)
    
    def handle_create_function(self, parsed_statement, original_statement: str) -> str:
        """Handle CREATE FUNCTION statement"""
        code_lines = []
        
        # Parse function
        func_pattern = r'CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s+RETURNS\s+(\w+).*?AS\s+BEGIN\s+(.*?)\s+END'
        match = re.search(func_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            func_name = match.group(1)
            parameters = match.group(2)
            return_type = match.group(3)
            body = match.group(4)
            
            code_lines.append(f"# Create function: {func_name}")
            code_lines.append(f"def fn_{func_name.lower()}({self._convert_proc_params(parameters)}):")
            code_lines.append("    '''")
            code_lines.append(f"    Converted from SQL Server function: {func_name}")
            code_lines.append(f"    Returns: {return_type}")
            code_lines.append("    '''")
            code_lines.append("    # Function body")
            code_lines.append("    # Implement function logic")
            code_lines.append("    return None  # Replace with actual return value")
        else:
            # Try table-valued function
            tvf_pattern = r'CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s+RETURNS\s+TABLE'
            tvf_match = re.search(tvf_pattern, original_statement, re.IGNORECASE)
            
            if tvf_match:
                func_name = tvf_match.group(1)
                parameters = tvf_match.group(2)
                
                code_lines.append(f"# Create table-valued function: {func_name}")
                code_lines.append(f"def tvf_{func_name.lower()}({self._convert_proc_params(parameters)}):")
                code_lines.append("    '''")
                code_lines.append(f"    Table-valued function: {func_name}")
                code_lines.append("    Returns: pd.DataFrame")
                code_lines.append("    '''")
                code_lines.append("    # Return DataFrame")
                code_lines.append("    return pd.DataFrame()  # Implement logic")
        
        return '\n'.join(code_lines)
    
    def _convert_proc_params(self, params_str: str) -> str:
        """Convert procedure/function parameters to Python"""
        if not params_str:
            return ""
        
        params = []
        param_list = [p.strip() for p in params_str.split(',')]
        
        for param in param_list:
            parts = param.split()
            if parts:
                param_name = parts[0].replace('@', '').lower()
                if len(parts) > 2 and '=' in param:
                    # Has default value
                    default_val = param.split('=')[1].strip()
                    params.append(f"{param_name}={default_val}")
                else:
                    params.append(param_name)
        
        return ', '.join(params)
    
    def handle_begin_transaction(self, parsed_statement, original_statement: str) -> str:
        """Handle BEGIN TRANSACTION"""
        return "# BEGIN TRANSACTION\n# Note: Pandas operations are not transactional\n# Consider using database transactions if needed"
    
    def handle_commit_transaction(self, parsed_statement, original_statement: str) -> str:
        """Handle COMMIT TRANSACTION"""
        return "# COMMIT TRANSACTION\n# If using database connection, commit changes\n# connection.commit()"
    
    def handle_rollback_transaction(self, parsed_statement, original_statement: str) -> str:
        """Handle ROLLBACK TRANSACTION"""
        return "# ROLLBACK TRANSACTION\n# If using database connection, rollback changes\n# connection.rollback()"
    
    def handle_try_catch(self, parsed_statement, original_statement: str) -> str:
        """Handle TRY...CATCH block"""
        code_lines = []
        code_lines.append("# TRY...CATCH block")
        
        # Simple pattern for TRY...CATCH
        try_catch_pattern = r'BEGIN\s+TRY\s+(.*?)\s+END\s+TRY\s+BEGIN\s+CATCH\s+(.*?)\s+END\s+CATCH'
        match = re.search(try_catch_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            try_block = match.group(1)
            catch_block = match.group(2)
            
            code_lines.append("try:")
            code_lines.append("    # TRY block")
            for line in try_block.strip().split('\n')[:3]:
                code_lines.append(f"    # {line.strip()}")
            code_lines.append("    pass  # Implement try logic")
            code_lines.append("except Exception as e:")
            code_lines.append("    # CATCH block")
            code_lines.append("    error_message = str(e)")
            code_lines.append("    error_number = getattr(e, 'errno', -1)")
            code_lines.append("    error_severity = 16  # Default severity")
            code_lines.append("    error_state = 1")
            code_lines.append("    # Handle error")
            code_lines.append("    print(f'Error: {error_message}')")
        
        return '\n'.join(code_lines)
    
    def handle_throw_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle THROW statement"""
        code_lines = []
        
        throw_pattern = r'THROW\s+(\d+)\s*,\s*[\'"](.+?)[\'"]\s*,\s*(\d+)'
        match = re.search(throw_pattern, original_statement, re.IGNORECASE)
        
        if match:
            error_num = match.group(1)
            error_msg = match.group(2)
            state = match.group(3)
            
            code_lines.append(f"# THROW error")
            code_lines.append(f"raise Exception(f'Error {error_num}: {error_msg} (State: {state})')")
        else:
            code_lines.append("# THROW - Re-raise last error")
            code_lines.append("raise  # Re-raise the last exception")
        
        return '\n'.join(code_lines)
    
    def handle_raiserror_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle RAISERROR statement"""
        code_lines = []
        
        raiserror_pattern = r'RAISERROR\s*\(\s*[\'"](.+?)[\'"]\s*,\s*(\d+)\s*,\s*(\d+)'
        match = re.search(raiserror_pattern, original_statement, re.IGNORECASE)
        
        if match:
            error_msg = match.group(1)
            severity = match.group(2)
            state = match.group(3)
            
            code_lines.append(f"# RAISERROR")
            code_lines.append(f"# Severity: {severity}, State: {state}")
            
            if int(severity) >= 16:
                code_lines.append(f"raise Exception('{error_msg}')")
            else:
                code_lines.append(f"print('Warning: {error_msg}')")
        
        return '\n'.join(code_lines)
    
    def handle_cursor_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle CURSOR declaration"""
        code_lines = []
        code_lines.append("# Cursor declaration")
        
        # Parse cursor declaration
        cursor_pattern = r'DECLARE\s+(\w+)\s+CURSOR\s+(?:.*?)\s+FOR\s+(SELECT.*)'
        match = re.search(cursor_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            cursor_name = match.group(1)
            select_query = match.group(2)
            
            self.cursor_counter += 1
            cursor_var = f"cursor_{cursor_name.lower()}"
            
            code_lines.append(f"# Cursor: {cursor_name}")
            code_lines.append(f"# Convert cursor to DataFrame iteration")
            
            # Convert SELECT query
            select_code = self.convert_select_query(select_query)
            code_lines.append(f"{cursor_var}_df = {select_code}")
            code_lines.append(f"{cursor_var}_index = 0")
            
            # Store cursor info
            self.cursors[cursor_name] = {
                'var': cursor_var,
                'df_var': f"{cursor_var}_df",
                'index_var': f"{cursor_var}_index"
            }
        else:
            code_lines.append(f"# Could not parse cursor declaration: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def _handle_information_schema_query(self, query: str) -> str:
        """Handle INFORMATION_SCHEMA queries"""
        code_lines = []
        code_lines.append("# INFORMATION_SCHEMA query")
        
        # Identify which information schema table
        for schema_table, handler in self.information_schema_tables.items():
            if schema_table in query.upper():
                code_lines.append(f"# Query: {schema_table}")
                code_lines.extend(handler())
                break
        else:
            code_lines.append("# Generic metadata query")
            code_lines.append("# Use SQLAlchemy inspector for metadata")
            code_lines.append("from sqlalchemy import create_engine, inspect")
            code_lines.append("inspector = inspect(engine)")
            code_lines.append("# Use inspector methods like:")
            code_lines.append("# - inspector.get_table_names()")
            code_lines.append("# - inspector.get_columns('table_name')")
            code_lines.append("# - inspector.get_foreign_keys('table_name')")
        
        return '\n'.join(code_lines)
    
    def _get_tables_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.TABLES query"""
        return [
            "# Get all tables",
            "inspector = inspect(engine)",
            "tables = inspector.get_table_names()",
            "df_tables = pd.DataFrame({",
            "    'TABLE_CATALOG': 'database_name',",
            "    'TABLE_SCHEMA': 'dbo',",
            "    'TABLE_NAME': tables,",
            "    'TABLE_TYPE': 'BASE TABLE'",
            "})"
        ]
    
    def _get_columns_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.COLUMNS query"""
        return [
            "# Get column information",
            "inspector = inspect(engine)",
            "all_columns = []",
            "for table in inspector.get_table_names():",
            "    columns = inspector.get_columns(table)",
            "    for col in columns:",
            "        all_columns.append({",
            "            'TABLE_CATALOG': 'database_name',",
            "            'TABLE_SCHEMA': 'dbo',",
            "            'TABLE_NAME': table,",
            "            'COLUMN_NAME': col['name'],",
            "            'DATA_TYPE': str(col['type']),",
            "            'IS_NULLABLE': 'YES' if col['nullable'] else 'NO'",
            "        })",
            "df_columns = pd.DataFrame(all_columns)"
        ]
    
    def _get_key_columns_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.KEY_COLUMN_USAGE query"""
        return [
            "# Get key column information",
            "inspector = inspect(engine)",
            "key_columns = []",
            "for table in inspector.get_table_names():",
            "    pk = inspector.get_pk_constraint(table)",
            "    for col in pk['constrained_columns']:",
            "        key_columns.append({",
            "            'TABLE_NAME': table,",
            "            'COLUMN_NAME': col,",
            "            'CONSTRAINT_NAME': pk['name']",
            "        })",
            "df_key_columns = pd.DataFrame(key_columns)"
        ]
    
    def _get_foreign_keys_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS query"""
        return [
            "# Get foreign key information",
            "inspector = inspect(engine)",
            "foreign_keys = []",
            "for table in inspector.get_table_names():",
            "    fks = inspector.get_foreign_keys(table)",
            "    for fk in fks:",
            "        foreign_keys.append({",
            "            'CONSTRAINT_NAME': fk['name'],",
            "            'TABLE_NAME': table,",
            "            'REFERENCED_TABLE': fk['referred_table']",
            "        })",
            "df_foreign_keys = pd.DataFrame(foreign_keys)"
        ]
    
    def _get_constraints_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.TABLE_CONSTRAINTS query"""
        return [
            "# Get table constraints",
            "inspector = inspect(engine)",
            "constraints = []",
            "for table in inspector.get_table_names():",
            "    # Primary keys",
            "    pk = inspector.get_pk_constraint(table)",
            "    if pk['constrained_columns']:",
            "        constraints.append({",
            "            'TABLE_NAME': table,",
            "            'CONSTRAINT_NAME': pk['name'],",
            "            'CONSTRAINT_TYPE': 'PRIMARY KEY'",
            "        })",
            "    # Foreign keys",
            "    for fk in inspector.get_foreign_keys(table):",
            "        constraints.append({",
            "            'TABLE_NAME': table,",
            "            'CONSTRAINT_NAME': fk['name'],",
            "            'CONSTRAINT_TYPE': 'FOREIGN KEY'",
            "        })",
            "df_constraints = pd.DataFrame(constraints)"
        ]
    
    def _get_views_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.VIEWS query"""
        return [
            "# Get view information",
            "inspector = inspect(engine)",
            "views = inspector.get_view_names()",
            "df_views = pd.DataFrame({",
            "    'TABLE_CATALOG': 'database_name',",
            "    'TABLE_SCHEMA': 'dbo',",
            "    'TABLE_NAME': views,",
            "    'VIEW_DEFINITION': 'N/A'  # View definitions not easily accessible",
            "})"
        ]
    
    def _get_routines_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.ROUTINES query"""
        return [
            "# Get stored procedures and functions",
            "# Note: This requires direct database query",
            "query = '''",
            "SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION",
            "FROM INFORMATION_SCHEMA.ROUTINES",
            "'''",
            "df_routines = pd.read_sql_query(query, connection)"
        ]
    
    def _get_parameters_query(self) -> List[str]:
        """Generate code for INFORMATION_SCHEMA.PARAMETERS query"""
        return [
            "# Get procedure/function parameters",
            "# Note: This requires direct database query",
            "query = '''",
            "SELECT SPECIFIC_NAME, PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE",
            "FROM INFORMATION_SCHEMA.PARAMETERS",
            "'''",
            "df_parameters = pd.read_sql_query(query, connection)"
        ]
    
    def _handle_xml_query(self, query: str) -> str:
        """Handle SELECT ... FOR XML queries"""
        code_lines = []
        code_lines.append("# SELECT ... FOR XML query")
        
        # Remove FOR XML clause for regular processing
        base_query = re.sub(r'FOR\s+XML\s+\w+.*', '', query, flags=re.IGNORECASE)
        
        # Identify XML mode
        xml_mode_match = re.search(r'FOR\s+XML\s+(\w+)', query, re.IGNORECASE)
        xml_mode = xml_mode_match.group(1).upper() if xml_mode_match else 'AUTO'
        
        # Convert base query
        code_lines.append("# Convert query to DataFrame first")
        base_code = self.convert_select_query(base_query)
        code_lines.append(f"df = {base_code}")
        code_lines.append("")
        
        # Generate XML based on mode
        if xml_mode == 'RAW':
            code_lines.extend(self._generate_xml_raw())
        elif xml_mode == 'AUTO':
            code_lines.extend(self._generate_xml_auto())
        elif xml_mode == 'PATH':
            code_lines.extend(self._generate_xml_path())
        elif xml_mode == 'EXPLICIT':
            code_lines.extend(self._generate_xml_explicit())
        
        return '\n'.join(code_lines)
    
    def _generate_xml_raw(self) -> List[str]:
        """Generate XML in RAW mode"""
        return [
            "# Generate XML in RAW mode",
            "import xml.etree.ElementTree as ET",
            "root = ET.Element('root')",
            "for _, row in df.iterrows():",
            "    row_elem = ET.SubElement(root, 'row')",
            "    for col, val in row.items():",
            "        row_elem.set(str(col), str(val))",
            "xml_string = ET.tostring(root, encoding='unicode')",
            "print(xml_string)"
        ]
    
    def _generate_xml_auto(self) -> List[str]:
        """Generate XML in AUTO mode"""
        return [
            "# Generate XML in AUTO mode",
            "import xml.etree.ElementTree as ET",
            "root = ET.Element('root')",
            "for _, row in df.iterrows():",
            "    table_elem = ET.SubElement(root, 'Table')",
            "    for col, val in row.items():",
            "        col_elem = ET.SubElement(table_elem, col)",
            "        col_elem.text = str(val)",
            "xml_string = ET.tostring(root, encoding='unicode')",
            "print(xml_string)"
        ]
    
    def _generate_xml_path(self) -> List[str]:
        """Generate XML in PATH mode"""
        return [
            "# Generate XML in PATH mode",
            "import xml.etree.ElementTree as ET",
            "root = ET.Element('root')",
            "for _, row in df.iterrows():",
            "    # PATH mode allows custom element structure",
            "    # Customize based on your needs",
            "    elem = ET.SubElement(root, 'item')",
            "    for col, val in row.items():",
            "        # Handle nested paths (e.g., 'Customer/Name')",
            "        if '/' in col:",
            "            parts = col.split('/')",
            "            current = elem",
            "            for part in parts[:-1]:",
            "                current = ET.SubElement(current, part)",
            "            ET.SubElement(current, parts[-1]).text = str(val)",
            "        else:",
            "            ET.SubElement(elem, col).text = str(val)",
            "xml_string = ET.tostring(root, encoding='unicode')",
            "print(xml_string)"
        ]
    
    def _generate_xml_explicit(self) -> List[str]:
        """Generate XML in EXPLICIT mode"""
        return [
            "# Generate XML in EXPLICIT mode",
            "# EXPLICIT mode requires specific column naming convention",
            "# Tag, Parent, [ElementName!TagNumber!AttributeName]",
            "import xml.etree.ElementTree as ET",
            "# Complex XML generation based on EXPLICIT mode rules",
            "# This requires custom implementation based on your specific needs",
            "root = ET.Element('root')",
            "# Implement EXPLICIT mode logic here",
            "xml_string = df.to_xml()  # Simplified - customize as needed"
        ]
    
    def generate_pandas_code(self, components: Dict, original_query: str = "") -> str:
        """Generate Pandas code from extracted components"""
        code_lines = []
        
        # Check for XML data type operations
        if '.value(' in original_query or '.query(' in original_query or '.exist(' in original_query:
            code_lines.append("# XML data type operations detected")
            code_lines.append("# Note: XML operations require special handling")
            code_lines.append("")
        
        # Check for JSON data type operations
        if any(func in original_query.upper() for func in ['JSON_VALUE', 'JSON_QUERY', 'JSON_MODIFY', 'ISJSON']):
            code_lines.append("# JSON data type operations detected")
            code_lines.append("# Note: JSON operations handled with Python json module")
            code_lines.append("")
        
        # Handle FROM clause
        if components['from']:
            main_table = self.clean_identifier(components['from'])
            
            # Check if it's OPENQUERY
            if 'OPENQUERY' in components['from'].upper():
                code_lines.append("# OPENQUERY in FROM clause")
                code_lines.append("# See OPENQUERY handling above")
                code_lines.append("df = pd.DataFrame()  # Replace with OPENQUERY result")
            # Check if it's OPENJSON
            elif 'OPENJSON' in components['from'].upper():
                code_lines.append("# OPENJSON in FROM clause")
                json_pattern = r'OPENJSON\s*\(([^)]+)\)'
                json_match = re.search(json_pattern, components['from'], re.IGNORECASE)
                if json_match:
                    json_expr = json_match.group(1)
                    code_lines.append(f"df = pd.json_normalize(json.loads({json_expr}))")
                else:
                    code_lines.append("df = pd.DataFrame()  # Replace with OPENJSON result")
            else:
                code_lines.append(f"# Load main table")
                code_lines.append(f"df = pd.read_sql_table('{main_table}', {self.connection_var})")
                code_lines.append("# Or load from CSV: df = pd.read_csv('{main_table}.csv')")
            code_lines.append("")
        
        # Continue with regular processing
        # Handle JOINs
        if components['joins']:
            join_code = self._convert_joins_enhanced(components['joins'])
            code_lines.extend(join_code)
        
        # Handle WHERE clause
        if components['where']:
            where_code = self._convert_where_enhanced(components['where'])
            code_lines.append(f"# Apply WHERE conditions")
            code_lines.append(f"df = df.query('{where_code}')")
            code_lines.append("")
        
        # Handle GROUP BY
        if components['group_by']:
            group_code = self._convert_group_by_enhanced(
                components['group_by'], 
                components['select'],
                components['having']
            )
            code_lines.append("# GROUP BY operation")
            code_lines.append(f"df = df{group_code}")
            code_lines.append("")
        
        # Handle SELECT
        if components['select']:
            select_code = self._convert_select_enhanced(components['select'])
            if not components['group_by'] and select_code:
                code_lines.append("# Select columns")
                code_lines.append(f"df = df{select_code}")
                code_lines.append("")
        
        # Handle ORDER BY
        if components['order_by']:
            order_code = self._convert_order_by_enhanced(components['order_by'])
            code_lines.append("# Sort results")
            code_lines.append(f"df = df{order_code}")
            code_lines.append("")
        
        # Handle LIMIT/TOP
        if components['limit']:
            limit_code = self._convert_limit(components['limit'])
            code_lines.append("# Limit results")
            code_lines.append(f"df = df{limit_code}")
        
        return '\n'.join(code_lines)
    
    # Continue with all other methods from the original implementation...
    # (All remaining methods stay the same as in the original code)
    
    # Enhanced SQL function converters
    def _convert_hashbytes(self, args: str) -> str:
        """Convert HASHBYTES function"""
        parts = [p.strip().strip("'\"") for p in args.split(',', 1)]
        if len(parts) == 2:
            algorithm = parts[0].upper()
            data = parts[1]
            
            algo_map = {
                'MD5': 'md5',
                'SHA1': 'sha1',
                'SHA2_256': 'sha256',
                'SHA2_512': 'sha512'
            }
            
            if algorithm in algo_map:
                return f"hashlib.{algo_map[algorithm]}({data}.encode()).hexdigest()"
            
        return f"# HASHBYTES({args})"
    
    def _convert_iif(self, args: str) -> str:
        """Convert IIF function"""
        parts = self._split_respecting_parens(args, ',')
        if len(parts) == 3:
            condition = parts[0].strip()
            true_val = parts[1].strip()
            false_val = parts[2].strip()
            
            # Convert condition
            condition = self._convert_condition(condition)
            
            return f"({true_val} if {condition} else {false_val})"
        
        return f"# IIF({args})"
    
    def _convert_object_id(self, args: str) -> str:
        """Convert OBJECT_ID function"""
        object_name = args.strip().strip("'\"")
        
        code = [
            f"# OBJECT_ID('{object_name}')",
            "# Check if database object exists",
            "from sqlalchemy import inspect",
            "inspector = inspect(engine)",
            f"table_exists = '{object_name}' in inspector.get_table_names()",
            f"object_id = hash('{object_name}') if table_exists else None"
        ]
        
        return '\n'.join(code)
    
    def _convert_stuff(self, args: str) -> str:
        """Convert STUFF function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 4:
            string_expr = parts[0]
            start = int(parts[1]) - 1  # Convert to 0-based
            length = int(parts[2])
            replacement = parts[3].strip("'\"")
            
            return f"({string_expr}[:start] + '{replacement}' + {string_expr}[start + length:])"
        
        return f"# STUFF({args})"
    
    def _convert_patindex(self, args: str) -> str:
        """Convert PATINDEX function"""
        parts = [p.strip() for p in args.split(',', 1)]
        if len(parts) == 2:
            pattern = parts[0].strip("'\"")
            string_expr = parts[1]
            
            # Convert SQL pattern to regex
            regex_pattern = pattern.replace('%', '.*').replace('_', '.')
            
            return f"({string_expr}.str.find(r'{regex_pattern}') + 1)"
        
        return f"# PATINDEX({args})"
    
    def _convert_quotename(self, args: str) -> str:
        """Convert QUOTENAME function"""
        parts = [p.strip() for p in args.split(',')]
        name = parts[0]
        quote_char = parts[1].strip("'\"") if len(parts) > 1 else '['
        
        if quote_char == '[':
            return f"'[' + {name} + ']'"
        else:
            return f"'{quote_char}' + {name} + '{quote_char}'"
    
    def _convert_parsename(self, args: str) -> str:
        """Convert PARSENAME function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            object_name = parts[0]
            part_num = int(parts[1])
            
            # PARSENAME splits by '.' and returns parts from right
            return f"({object_name}.split('.')[-{part_num}] if len({object_name}.split('.')) >= {part_num} else None)"
        
        return f"# PARSENAME({args})"
    
    def _convert_checksum(self, args: str) -> str:
        """Convert CHECKSUM function"""
        # Simple checksum using hash
        return f"hash(tuple({args}))"
    
    def _convert_space(self, args: str) -> str:
        """Convert SPACE function"""
        return f"' ' * {args}"
    
    def _convert_replicate(self, args: str) -> str:
        """Convert REPLICATE function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            string_expr = parts[0].strip("'\"")
            times = parts[1]
            return f"'{string_expr}' * {times}"
        
        return f"# REPLICATE({args})"
    
    def _convert_left(self, args: str) -> str:
        """Convert LEFT function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            string_expr = parts[0]
            length = parts[1]
            return f"{string_expr}.str[:int({length})]"
        
        return f"# LEFT({args})"
    
    def _convert_right(self, args: str) -> str:
        """Convert RIGHT function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            string_expr = parts[0]
            length = parts[1]
            return f"{string_expr}.str[-int({length}):]"
        
        return f"# RIGHT({args})"
    
    # XML function converters
    def _convert_xml_value(self, args: str) -> str:
        """Convert XML value() method"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            xpath = parts[0].strip("'\"")
            sql_type = parts[1].strip("'\"")
            
            return f"ET.fromstring({args.split('.')[0]}).find('{xpath}').text"
        
        return f"# XML.value({args})"
    
    def _convert_xml_query(self, args: str) -> str:
        """Convert XML query() method"""
        xpath = args.strip("'\"")
        return f"ET.fromstring(xml_column).findall('{xpath}')"
    
    def _convert_xml_exist(self, args: str) -> str:
        """Convert XML exist() method"""
        xpath = args.strip("'\"")
        return f"(1 if ET.fromstring(xml_column).find('{xpath}') is not None else 0)"
    
    def _convert_xml_nodes(self, args: str) -> str:
        """Convert XML nodes() method"""
        xpath = args.strip("'\"")
        return f"[node for node in ET.fromstring(xml_column).findall('{xpath}')]"
    
    def _convert_xml_modify(self, args: str) -> str:
        """Convert XML modify() method"""
        return f"# XML.modify({args}) - Requires custom implementation"
    
    # Continue with existing methods from previous implementation...
    # (All the methods from the previous version remain the same)
    
    def convert_select_query(self, query: str, context: Dict[str, str] = None) -> str:
        """Enhanced SELECT query conversion with better parsing"""
        try:
            parsed = sqlparse.parse(query)[0]
            
            # Check for UNION
            if 'UNION' in query.upper():
                return self._convert_union_query(query)
            
            # Extract components with enhanced parsing
            components = self._extract_query_components_enhanced(parsed)
            
            # Build the query step by step
            df_var = f"df_{self.df_counter}"
            self.df_counter += 1
            
            code_parts = []
            
            # Handle FROM clause
            if components['from']:
                from_code = self._convert_from_clause(components['from'], context)
                code_parts.append(from_code)
            else:
                code_parts.append("pd.DataFrame()")  # Empty DataFrame for SELECT without FROM
            
            # Build method chain
            method_chain = []
            
            # JOINs
            if components['joins']:
                join_code = self._convert_joins_enhanced(components['joins'])
                method_chain.extend(join_code)
            
            # WHERE
            if components['where']:
                where_code = self._convert_where_enhanced(components['where'])
                method_chain.append(f".query('{where_code}')")
            
            # GROUP BY
            if components['group_by']:
                group_code = self._convert_group_by_enhanced(
                    components['group_by'], 
                    components['select'],
                    components['having']
                )
                method_chain.append(group_code)
            
            # SELECT (with computed columns)
            if components['select'] and not components['group_by']:
                select_code = self._convert_select_enhanced(components['select'])
                if select_code:
                    method_chain.append(select_code)
            
            # HAVING (if not handled in GROUP BY)
            if components['having'] and not components['group_by']:
                having_code = self._convert_where_enhanced(components['having'])
                method_chain.append(f".query('{having_code}')")
            
            # ORDER BY
            if components['order_by']:
                order_code = self._convert_order_by_enhanced(components['order_by'])
                method_chain.append(order_code)
            
            # LIMIT/TOP
            if components['limit']:
                limit_code = self._convert_limit(components['limit'])
                method_chain.append(limit_code)
            
            # Combine all parts
            if method_chain:
                result = f"({code_parts[0]}{''.join(method_chain)})"
            else:
                result = code_parts[0]
            
            return result
            
        except Exception as e:
            return f"pd.DataFrame()  # Error converting SELECT: {str(e)}"
    
    def _extract_query_components_enhanced(self, parsed_query) -> Dict:
        """Enhanced component extraction with better token handling"""
        components = {
            'select': [],
            'from': None,
            'joins': [],
            'where': [],
            'group_by': [],
            'having': [],
            'order_by': [],
            'limit': None,
            'distinct': False
        }
        
        current_section = None
        join_buffer = []
        
        for token in parsed_query.tokens:
            if token.is_whitespace:
                continue
            
            # Check for keywords
            if token.ttype is T.Keyword or token.ttype is T.Keyword.DML:
                keyword = token.value.upper()
                
                if keyword == 'SELECT':
                    current_section = 'select'
                elif keyword == 'DISTINCT':
                    components['distinct'] = True
                elif keyword == 'FROM':
                    current_section = 'from'
                elif keyword in ['JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']:
                    current_section = 'join'
                    join_buffer = [keyword]
                elif keyword == 'WHERE':
                    current_section = 'where'
                elif keyword == 'GROUP':
                    current_section = 'group'
                elif keyword == 'HAVING':
                    current_section = 'having'
                elif keyword == 'ORDER':
                    current_section = 'order'
                elif keyword in ['LIMIT', 'TOP']:
                    current_section = 'limit'
                    if keyword == 'TOP':
                        components['limit'] = {'type': 'top', 'value': None}
                elif keyword == 'BY' and current_section in ['group', 'order']:
                    current_section = current_section + '_by'
                elif keyword == 'ON' and current_section == 'join':
                    join_buffer.append('ON')
            
            # Process tokens based on current section
            elif current_section:
                self._add_token_to_component(components, current_section, token, join_buffer)
        
        return components
    
    def _add_token_to_component(self, components: Dict, section: str, token, join_buffer: List):
        """Add token to appropriate component section"""
        if section == 'select':
            components['select'].append(str(token))
        elif section == 'from':
            if not components['from']:
                components['from'] = str(token)
            else:
                components['from'] += ' ' + str(token)
        elif section == 'join':
            join_buffer.append(str(token))
            if 'ON' in str(token).upper() or token.match(T.Keyword, 'ON'):
                # Complete join clause
                components['joins'].append(' '.join(join_buffer))
                join_buffer.clear()
        elif section == 'where':
            components['where'].append(str(token))
        elif section == 'group_by':
            components['group_by'].append(str(token))
        elif section == 'having':
            components['having'].append(str(token))
        elif section == 'order_by':
            components['order_by'].append(str(token))
        elif section == 'limit':
            if components['limit'] and components['limit']['type'] == 'top':
                components['limit']['value'] = str(token).strip()
    
    def _convert_from_clause(self, from_clause: str, context: Dict[str, str] = None) -> str:
        """Convert FROM clause with subquery support"""
        from_clause = from_clause.strip()
        
        # Check for subquery
        if from_clause.startswith('(') and from_clause.endswith(')'):
            # Handle subquery
            subquery = from_clause[1:-1]
            subquery_code = self.convert_select_query(subquery, context)
            return f"({subquery_code})"
        
        # Check for OPENJSON
        if 'OPENJSON' in from_clause.upper():
            json_pattern = r'OPENJSON\s*\(([^)]+)\)'
            json_match = re.search(json_pattern, from_clause, re.IGNORECASE)
            if json_match:
                json_expr = json_match.group(1)
                return f"pd.json_normalize(json.loads({json_expr}))"
        
        # Check for table with alias
        alias_match = re.match(r'(\w+)\s+(?:AS\s+)?(\w+)', from_clause, re.IGNORECASE)
        if alias_match:
            table_name = self.clean_identifier(alias_match.group(1))
            alias = alias_match.group(2)
            self.table_aliases[alias] = table_name
        else:
            table_name = self.clean_identifier(from_clause)
        
        # Check if it's a CTE reference
        if context and table_name in context:
            return context[table_name]
        elif table_name in self.cte_definitions:
            return self.cte_definitions[table_name]
        # Check if it's a temp table
        elif table_name.replace('#', 'temp_') in self.temp_tables:
            return table_name.replace('#', 'temp_')
        else:
            # Regular table
            return f"pd.read_sql_table('{table_name}', {self.connection_var})"
    
    def _convert_joins_enhanced(self, joins: List[str]) -> List[str]:
        """Convert JOIN clauses with proper handling"""
        join_methods = []
        
        for join_clause in joins:
            # Parse join type and details
            join_pattern = r'(INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.*)'
            match = re.search(join_pattern, join_clause, re.IGNORECASE)
            
            if match:
                join_type = (match.group(1) or 'INNER').lower()
                table_name = self.clean_identifier(match.group(2))
                alias = match.group(3)
                join_condition = match.group(4)
                
                if alias:
                    self.table_aliases[alias] = table_name
                
                # Convert join condition to merge parameters
                merge_params = self._parse_join_condition(join_condition)
                
                # Generate merge code
                how_map = {
                    'inner': 'inner',
                    'left': 'left',
                    'right': 'right',
                    'full': 'outer',
                    'cross': 'cross'
                }
                
                how = how_map.get(join_type, 'inner')
                
                if merge_params['left_on'] and merge_params['right_on']:
                    join_methods.append(
                        f".merge(pd.read_sql_table('{table_name}', {self.connection_var}), "
                        f"left_on='{merge_params['left_on']}', "
                        f"right_on='{merge_params['right_on']}', "
                        f"how='{how}')"
                    )
                else:
                    # Complex join condition
                    join_methods.append(
                        f".merge(pd.read_sql_table('{table_name}', {self.connection_var}), "
                        f"how='{how}')  # Complex join: {join_condition}"
                    )
        
        return join_methods
    
    def _parse_join_condition(self, condition: str) -> Dict[str, str]:
        """Parse join condition to extract merge parameters"""
        # Simple equality join
        eq_pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        match = re.search(eq_pattern, condition)
        
        if match:
            left_table = match.group(1)
            left_col = match.group(2)
            right_table = match.group(3)
            right_col = match.group(4)
            
            return {
                'left_on': self.clean_identifier(left_col),
                'right_on': self.clean_identifier(right_col)
            }
        
        # Simple column names
        simple_pattern = r'(\w+)\s*=\s*(\w+)'
        match = re.search(simple_pattern, condition)
        
        if match:
            return {
                'left_on': self.clean_identifier(match.group(1)),
                'right_on': self.clean_identifier(match.group(2))
            }
        
        return {'left_on': None, 'right_on': None}
    
    def _convert_where_enhanced(self, where_tokens: List[str]) -> str:
        """Enhanced WHERE clause conversion with function support"""
        where_clause = ' '.join(where_tokens).strip()
        
        # Handle subqueries
        where_clause = self._convert_subqueries_in_condition(where_clause)
        
        # Convert SQL functions
        where_clause = self._convert_sql_functions(where_clause)
        
        # Basic conversions
        conversions = [
            (r'\bAND\b', '&'),
            (r'\bOR\b', '|'),
            (r'\bNOT\b', '~'),
            (r'\bIN\s*\(([^)]+)\)', r'.isin([\1])'),
            (r'\bNOT\s+IN\s*\(([^)]+)\)', r'~\1.isin([\2])'),
            (r'\bLIKE\s+\'%([^\']+)%\'', r'.str.contains("\1", case=False)'),
            (r'\bLIKE\s+\'([^\']+)%\'', r'.str.startswith("\1")'),
            (r'\bLIKE\s+\'%([^\']+)\'', r'.str.endswith("\1")'),
            (r'\bIS\s+NULL\b', '.isna()'),
            (r'\bIS\s+NOT\s+NULL\b', '.notna()'),
            (r'\bBETWEEN\s+(\S+)\s+AND\s+(\S+)', r'.between(\1, \2)'),
            (r'<>', '!='),
            (r'=', '==')
        ]
        
        pandas_where = where_clause
        for sql_pattern, pandas_pattern in conversions:
            pandas_where = re.sub(sql_pattern, pandas_pattern, pandas_where, flags=re.IGNORECASE)
        
        return pandas_where
    
    def _convert_subqueries_in_condition(self, condition: str) -> str:
        """Convert subqueries in WHERE/HAVING conditions"""
        # Handle IN (subquery)
        in_subquery_pattern = r'IN\s*\((SELECT.*?)\)'
        
        def replace_subquery(match):
            subquery = match.group(1)
            subquery_code = self.convert_select_query(subquery)
            return f".isin({subquery_code}['column'].tolist())"  # Adjust column name
        
        condition = re.sub(in_subquery_pattern, replace_subquery, condition, flags=re.IGNORECASE | re.DOTALL)
        
        return condition
    
    def _convert_select_enhanced(self, select_tokens: List[str]) -> str:
        """Enhanced SELECT conversion with expressions and aliases"""
        select_clause = ' '.join(select_tokens).strip()
        
        if select_clause == '*':
            return ""  # Select all columns
        
        # Parse select items with aliases and expressions
        select_items = self._parse_select_items_enhanced(select_clause)
        
        if not select_items:
            return ""
        
        # Check if we need to create computed columns
        has_expressions = any(item.get('is_expression', False) for item in select_items)
        
        if has_expressions:
            # Use assign for computed columns
            assignments = []
            selections = []
            
            for item in select_items:
                if item['is_expression']:
                    col_name = item['alias'] or f"expr_{len(assignments)}"
                    expr = self._convert_expression(item['expression'])
                    assignments.append(f"{col_name}={expr}")
                    selections.append(col_name)
                else:
                    selections.append(item['column'])
            
            code_parts = []
            if assignments:
                code_parts.append(f".assign({', '.join(assignments)})")
            if selections:
                code_parts.append(f"[[{', '.join([repr(s) for s in selections])}]]")
            
            return ''.join(code_parts)
        else:
            # Simple column selection
            columns = [item['alias'] or item['column'] for item in select_items]
            return f"[[{', '.join([repr(c) for c in columns])}]]"
    
    def _parse_select_items_enhanced(self, select_clause: str) -> List[Dict]:
        """Parse SELECT items with better expression handling"""
        items = []
        
        # Split by comma, but respect parentheses
        parts = self._split_respecting_parens(select_clause, ',')
        
        for part in parts:
            part = part.strip()
            
            # Check for alias
            alias_match = re.search(r'\s+AS\s+(\w+)$', part, re.IGNORECASE)
            if alias_match:
                alias = alias_match.group(1)
                expression = part[:alias_match.start()].strip()
            else:
                # Check for implicit alias (expression alias)
                space_split = part.rsplit(None, 1)
                if len(space_split) == 2 and not self._is_sql_keyword(space_split[1]):
                    expression, alias = space_split
                else:
                    expression = part
                    alias = None
            
            # Determine if it's a simple column or expression
            is_expression = (
                '(' in expression or
                any(op in expression for op in ['+', '-', '*', '/', '%']) or
                any(func in expression.upper() for func in self.sql_functions.keys())
            )
            
            items.append({
                'expression': expression,
                'column': self.clean_identifier(expression) if not is_expression else None,
                'alias': self.clean_identifier(alias) if alias else None,
                'is_expression': is_expression
            })
        
        return items
    
    def _split_respecting_parens(self, text: str, delimiter: str) -> List[str]:
        """Split text by delimiter, respecting parentheses"""
        parts = []
        current = []
        paren_depth = 0
        
        for char in text:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == delimiter and paren_depth == 0:
                parts.append(''.join(current))
                current = []
                continue
            
            current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _is_sql_keyword(self, word: str) -> bool:
        """Check if word is a SQL keyword"""
        keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'HAVING', 'ORDER',
            'LIMIT', 'JOIN', 'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
            'BETWEEN', 'LIKE', 'IS', 'NULL', 'ASC', 'DESC', 'DISTINCT',
            'UNION', 'ALL', 'AS', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'
        }
        return word.upper() in keywords
    
    def _convert_expression(self, expression: str) -> str:
        """Convert SQL expression to Pandas"""
        # Convert SQL functions
        expr = self._convert_sql_functions(expression)
        
        # Convert CASE WHEN
        expr = self._convert_case_when(expr)
        
        # Convert operators
        expr = expr.replace('||', '+')  # String concatenation
        
        return expr
    
    def _convert_sql_functions(self, text: str) -> str:
        """Convert SQL functions to Pandas equivalents"""
        result = text
        
        for sql_func, pandas_func in self.sql_functions.items():
            if callable(pandas_func):
                # Custom conversion function
                pattern = rf'\b{sql_func}\s*\(([^)]+)\)'
                matches = re.finditer(pattern, result, re.IGNORECASE)
                for match in reversed(list(matches)):
                    converted = pandas_func(match.group(1))
                    result = result[:match.start()] + converted + result[match.end():]
            else:
                # Simple replacement
                pattern = rf'\b{sql_func}\b'
                result = re.sub(pattern, pandas_func, result, flags=re.IGNORECASE)
        
        return result
    
    def _convert_case_when(self, expression: str) -> str:
        """Convert CASE WHEN to np.select or similar"""
        case_pattern = r'CASE\s+(.*?)\s+END'
        match = re.search(case_pattern, expression, re.IGNORECASE | re.DOTALL)
        
        if match:
            case_content = match.group(1)
            
            # Parse WHEN conditions
            when_pattern = r'WHEN\s+(.*?)\s+THEN\s+(.*?)(?=\s+WHEN|\s+ELSE|\s*$)'
            when_matches = re.finditer(when_pattern, case_content, re.IGNORECASE | re.DOTALL)
            
            conditions = []
            values = []
            
            for when_match in when_matches:
                condition = self._convert_where_enhanced([when_match.group(1)])
                value = when_match.group(2).strip()
                conditions.append(condition)
                values.append(value)
            
            # Parse ELSE
            else_match = re.search(r'ELSE\s+(.*)', case_content, re.IGNORECASE)
            default = else_match.group(1).strip() if else_match else 'None'
            
            # Generate np.select
            if conditions:
                cond_list = '[' + ', '.join(conditions) + ']'
                val_list = '[' + ', '.join(values) + ']'
                return f"np.select({cond_list}, {val_list}, default={default})"
            
        return expression
    
    def _convert_order_by_enhanced(self, order_by_tokens: List[str]) -> str:
        """Enhanced ORDER BY with expression support"""
        order_clause = ' '.join(order_by_tokens).strip()
        order_items = self._split_respecting_parens(order_clause, ',')
        
        sort_columns = []
        ascending = []
        
        for item in order_items:
            item = item.strip()
            
            # Check for DESC/ASC
            desc_match = re.search(r'\s+(DESC|ASC)$', item, re.IGNORECASE)
            if desc_match:
                direction = desc_match.group(1).upper()
                column_expr = item[:desc_match.start()].strip()
                ascending.append(direction != 'DESC')
            else:
                column_expr = item
                ascending.append(True)
            
            # Check if it's a position (ORDER BY 1, 2, etc.)
            if column_expr.isdigit():
                sort_columns.append(f"df.columns[{int(column_expr) - 1}]")
            else:
                sort_columns.append(repr(self.clean_identifier(column_expr)))
        
        if len(sort_columns) == 1:
            return f".sort_values({sort_columns[0]}, ascending={ascending[0]})"
        else:
            return f".sort_values([{', '.join(sort_columns)}], ascending={ascending})"
    
    def _convert_limit(self, limit_info: Dict) -> str:
        """Convert LIMIT/TOP clause"""
        if limit_info['type'] == 'top':
            return f".head({limit_info['value']})"
        else:
            # LIMIT clause
            return f".head({limit_info['value']})"
    
    def _convert_union_query(self, query: str) -> str:
        """Convert UNION queries"""
        # Split by UNION/UNION ALL
        union_parts = re.split(r'\bUNION\s+ALL\b|\bUNION\b', query, flags=re.IGNORECASE)
        
        # Check if UNION ALL is used
        use_union_all = 'UNION ALL' in query.upper()
        
        # Convert each part
        dfs = []
        for i, part in enumerate(union_parts):
            part_code = self.convert_select_query(part.strip())
            dfs.append(f"df_union_{i} = {part_code}")
        
        # Combine with concat
        code_lines = dfs
        df_list = [f"df_union_{i}" for i in range(len(union_parts))]
        
        if use_union_all:
            code_lines.append(f"pd.concat([{', '.join(df_list)}], ignore_index=True)")
        else:
            # UNION (distinct)
            code_lines.append(f"pd.concat([{', '.join(df_list)}], ignore_index=True).drop_duplicates()")
        
        return f"({'; '.join(code_lines)})[-1]"
    
    # Conversion helper methods for SQL functions
    def _convert_datepart(self, args: str) -> str:
        """Convert DATEPART function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            date_part = parts[0].strip().strip("'\"").lower()
            column = parts[1]
            
            part_map = {
                'year': 'dt.year',
                'month': 'dt.month',
                'day': 'dt.day',
                'hour': 'dt.hour',
                'minute': 'dt.minute',
                'second': 'dt.second',
                'week': 'dt.isocalendar().week',
                'dayofweek': 'dt.dayofweek',
                'quarter': 'dt.quarter'
            }
            
            return f"{column}.{part_map.get(date_part, 'dt.year')}"
        
        return f"# DATEPART({args})"
    
    def _convert_datediff(self, args: str) -> str:
        """Convert DATEDIFF function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 3:
            date_part = parts[0].strip().strip("'\"").lower()
            start_date = parts[1]
            end_date = parts[2]
            
            if date_part == 'day':
                return f"({end_date} - {start_date}).dt.days"
            elif date_part == 'hour':
                return f"({end_date} - {start_date}).dt.total_seconds() / 3600"
            elif date_part == 'minute':
                return f"({end_date} - {start_date}).dt.total_seconds() / 60"
            else:
                return f"# DATEDIFF({args})"
        
        return f"# DATEDIFF({args})"
    
    def _convert_dateadd(self, args: str) -> str:
        """Convert DATEADD function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 3:
            date_part = parts[0].strip().strip("'\"").lower()
            number = parts[1]
            date = parts[2]
            
            if date_part == 'day':
                return f"{date} + pd.Timedelta(days={number})"
            elif date_part == 'hour':
                return f"{date} + pd.Timedelta(hours={number})"
            elif date_part == 'minute':
                return f"{date} + pd.Timedelta(minutes={number})"
            elif date_part == 'month':
                return f"{date} + pd.DateOffset(months={number})"
            elif date_part == 'year':
                return f"{date} + pd.DateOffset(years={number})"
            else:
                return f"# DATEADD({args})"
        
        return f"# DATEADD({args})"
    
    def _convert_cast(self, args: str) -> str:
        """Convert CAST function"""
        # CAST(expression AS type)
        match = re.match(r'(.+?)\s+AS\s+(.+)', args, re.IGNORECASE)
        if match:
            expression = match.group(1).strip()
            target_type = match.group(2).strip().upper()
            
            type_map = {
                'INT': 'astype(int)',
                'INTEGER': 'astype(int)',
                'BIGINT': 'astype(int)',
                'FLOAT': 'astype(float)',
                'DECIMAL': 'astype(float)',
                'VARCHAR': 'astype(str)',
                'NVARCHAR': 'astype(str)',
                'CHAR': 'astype(str)',
                'DATE': 'pd.to_datetime',
                'DATETIME': 'pd.to_datetime'
            }
            
            for sql_type, pandas_convert in type_map.items():
                if target_type.startswith(sql_type):
                    if 'to_datetime' in pandas_convert:
                        return f"pd.to_datetime({expression})"
                    else:
                        return f"{expression}.{pandas_convert}"
            
        return f"# CAST({args})"
    
    def _convert_convert(self, args: str) -> str:
        """Convert CONVERT function"""
        # CONVERT(type, expression, style)
        parts = [p.strip() for p in args.split(',')]
        if len(parts) >= 2:
            target_type = parts[0].strip().upper()
            expression = parts[1]
            
            # Similar to CAST
            return self._convert_cast(f"{expression} AS {target_type}")
        
        return f"# CONVERT({args})"
    
    def _convert_isnull(self, args: str) -> str:
        """Convert ISNULL function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 2:
            check_value = parts[0]
            replacement = parts[1]
            return f"{check_value}.fillna({replacement})"
        
        return f"# ISNULL({args})"
    
    def _convert_coalesce(self, args: str) -> str:
        """Convert COALESCE function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) >= 2:
            # Use combine_first for multiple values
            result = parts[0]
            for part in parts[1:]:
                result = f"{result}.combine_first({part})"
            return result
        
        return f"# COALESCE({args})"
    
    def _convert_substring(self, args: str) -> str:
        """Convert SUBSTRING function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) == 3:
            string_col = parts[0]
            start = int(parts[1]) - 1  # SQL uses 1-based indexing
            length = int(parts[2])
            return f"{string_col}.str[{start}:{start + length}]"
        
        return f"# SUBSTRING({args})"
    
    def _convert_charindex(self, args: str) -> str:
        """Convert CHARINDEX function"""
        parts = [p.strip() for p in args.split(',')]
        if len(parts) >= 2:
            search_str = parts[0].strip("'\"")
            string_col = parts[1]
            return f"{string_col}.str.find('{search_str}') + 1"  # SQL uses 1-based indexing
        
        return f"# CHARINDEX({args})"
    
    def _convert_condition(self, condition: str) -> str:
        """Convert SQL condition to Python condition"""
        # Replace SQL operators with Python operators
        condition = condition.replace('=', '==')
        condition = condition.replace('<>', '!=')
        
        # Handle EXISTS
        condition = re.sub(r'EXISTS\s*\((.*?)\)', r'len(\1) > 0', condition, flags=re.IGNORECASE | re.DOTALL)
        
        # Convert variables
        for sql_var, py_var in self.variables.items():
            condition = condition.replace(sql_var, py_var)
        
        return condition
    
    def _convert_value(self, value: str) -> str:
        """Convert SQL value to Python value"""
        value = value.strip()
        
        # Handle NULL
        if value.upper() == 'NULL':
            return 'None'
        
        # Handle strings
        if value.startswith("'") and value.endswith("'"):
            return f'"{value[1:-1]}"'
        
        # Handle variables
        if value.startswith('@'):
            return self.variables.get(value, value)
        
        # Handle functions
        if '(' in value:
            return self._convert_sql_functions(value)
        
        return value
    
    def clean_identifier(self, identifier: str) -> str:
        """Clean SQL identifier (remove brackets, quotes, etc.)"""
        if not identifier:
            return identifier
        
        cleaned = identifier.strip()
        
        # Remove brackets
        cleaned = re.sub(r'[\[\]]', '', cleaned)
        
        # Remove quotes
        cleaned = re.sub(r'["`\']', '', cleaned)
        
        # Remove schema prefix if present
        if '.' in cleaned:
            parts = cleaned.split('.')
            cleaned = parts[-1]  # Take the last part (table/column name)
        
        return cleaned
    
    def handle_create_temp_table(self, parsed_statement, original_statement: str) -> str:
        """Handle CREATE temporary table"""
        code_lines = []
        
        # Extract table name
        temp_table_pattern = r'CREATE.*?(?:TABLE|#)?\s+(#?\w+)\s*\((.*?)\)'
        match = re.search(temp_table_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            table_name = match.group(1).replace('#', 'temp_')
            columns_def = match.group(2)
            
            # Store temp table
            self.temp_tables[table_name] = columns_def
            
            code_lines.append(f"# Create temporary table: {table_name}")
            
            # Parse column definitions
            columns = self.parse_column_definitions(columns_def)
            
            if columns:
                code_lines.append(f"# Define {table_name} structure")
                code_lines.append(f"{table_name} = pd.DataFrame({{")
                for col_name, col_type in columns.items():
                    pandas_type = self.sql_type_to_pandas(col_type)
                    code_lines.append(f"    '{col_name}': pd.Series([], dtype='{pandas_type}'),")
                code_lines.append("})")
            else:
                code_lines.append(f"{table_name} = pd.DataFrame()")
        else:
            # Handle CREATE TABLE AS SELECT
            create_as_pattern = r'CREATE.*?(?:TABLE|#)?\s+(#?\w+)\s+AS\s+(SELECT.*)'
            as_match = re.search(create_as_pattern, original_statement, re.IGNORECASE | re.DOTALL)
            
            if as_match:
                table_name = as_match.group(1).replace('#', 'temp_')
                select_query = as_match.group(2)
                
                code_lines.append(f"# Create temporary table from SELECT: {table_name}")
                select_code = self.convert_select_query(select_query)
                code_lines.append(f"{table_name} = {select_code}")
                
                self.temp_tables[table_name] = "from_select"
            else:
                code_lines.append(f"# Could not parse CREATE statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_drop_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle DROP statement"""
        code_lines = []
        
        # Extract what's being dropped
        drop_pattern = r'DROP\s+(TABLE|INDEX|VIEW|PROCEDURE)\s+(?:IF\s+EXISTS\s+)?(.+)'
        match = re.search(drop_pattern, original_statement, re.IGNORECASE)
        
        if match:
            object_type = match.group(1).upper()
            object_name = match.group(2).strip()
            
            if object_type == 'TABLE':
                # Clean table name
                clean_name = self.clean_identifier(object_name).replace('#', 'temp_')
                
                code_lines.append(f"# Drop table: {object_name}")
                code_lines.append(f"# Remove DataFrame from memory")
                code_lines.append(f"try:")
                code_lines.append(f"    del {clean_name}")
                code_lines.append(f"except NameError:")
                code_lines.append(f"    pass  # Table doesn't exist")
                
                # Remove from temp tables tracking
                if clean_name in self.temp_tables:
                    del self.temp_tables[clean_name]
                    
            else:
                code_lines.append(f"# Drop {object_type}: {object_name}")
                code_lines.append(f"# Note: {object_type} operations not directly applicable to Pandas")
        else:
            code_lines.append(f"# Could not parse DROP statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_truncate_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle TRUNCATE statement"""
        code_lines = []
        
        # Extract table name
        truncate_pattern = r'TRUNCATE\s+TABLE\s+(.+)'
        match = re.search(truncate_pattern, original_statement, re.IGNORECASE)
        
        if match:
            table_name = self.clean_identifier(match.group(1)).replace('#', 'temp_')
            
            code_lines.append(f"# Truncate table: {table_name}")
            code_lines.append(f"# Clear all data but keep structure")
            code_lines.append(f"if '{table_name}' in locals():")
            code_lines.append(f"    {table_name} = {table_name}.iloc[0:0].copy()  # Keep structure, remove data")
            code_lines.append(f"else:")
            code_lines.append(f"    print('Table {table_name} does not exist')")
        else:
            code_lines.append(f"# Could not parse TRUNCATE statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_insert_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle INSERT statement"""
        code_lines = []
        
        # INSERT INTO table VALUES
        values_pattern = r'INSERT\s+INTO\s+(.+?)\s*(?:\(([^)]+)\))?\s*VALUES\s*\((.+)\)'
        values_match = re.search(values_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        # INSERT INTO table SELECT
        select_pattern = r'INSERT\s+INTO\s+(.+?)\s*(?:\(([^)]+)\))?\s*(SELECT.*)'
        select_match = re.search(select_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if values_match:
            table_name = self.clean_identifier(values_match.group(1)).replace('#', 'temp_')
            columns = values_match.group(2)
            values = values_match.group(3)
            
            code_lines.append(f"# Insert values into {table_name}")
            
            if columns:
                col_list = [self.clean_identifier(c.strip()) for c in columns.split(',')]
                val_list = [v.strip().strip("'\"") for v in values.split(',')]
                
                code_lines.append(f"new_row = pd.DataFrame({{")
                for col, val in zip(col_list, val_list):
                    code_lines.append(f"    '{col}': [{val}],")
                code_lines.append(f"}})")
            else:
                code_lines.append(f"# Insert into all columns")
                val_list = [v.strip().strip("'\"") for v in values.split(',')]
                code_lines.append(f"new_row = pd.DataFrame([{val_list}])")
            
            code_lines.append(f"{table_name} = pd.concat([{table_name}, new_row], ignore_index=True)")
            
        elif select_match:
            table_name = self.clean_identifier(select_match.group(1)).replace('#', 'temp_')
            columns = select_match.group(2)
            select_query = select_match.group(3)
            
            code_lines.append(f"# Insert SELECT results into {table_name}")
            select_code = self.convert_select_query(select_query)
            code_lines.append(f"insert_data = {select_code}")
            code_lines.append(f"{table_name} = pd.concat([{table_name}, insert_data], ignore_index=True)")
        else:
            code_lines.append(f"# Could not parse INSERT statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_update_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle UPDATE statement"""
        code_lines = []
        
        # Basic UPDATE pattern
        update_pattern = r'UPDATE\s+(.+?)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?'
        match = re.search(update_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            table_name = self.clean_identifier(match.group(1)).replace('#', 'temp_')
            set_clause = match.group(2)
            where_clause = match.group(3) if match.group(3) else None
            
            code_lines.append(f"# Update {table_name}")
            
            # Parse SET clause
            set_pairs = [pair.strip() for pair in set_clause.split(',')]
            
            if where_clause:
                where_pandas = self._convert_where_enhanced([where_clause])
                code_lines.append(f"# Apply WHERE condition: {where_clause}")
                code_lines.append(f"mask = {table_name}.eval('{where_pandas}')")
            else:
                code_lines.append(f"# Update all rows")
                code_lines.append(f"mask = True")
            
            for set_pair in set_pairs:
                if '=' in set_pair:
                    column, value = set_pair.split('=', 1)
                    column = self.clean_identifier(column.strip())
                    value = value.strip().strip("'\"")
                    code_lines.append(f"{table_name}.loc[mask, '{column}'] = {value}")
        else:
            code_lines.append(f"# Could not parse UPDATE statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_delete_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle DELETE statement"""
        code_lines = []
        
        # DELETE pattern
        delete_pattern = r'DELETE\s+FROM\s+(.+?)(?:\s+WHERE\s+(.+))?'
        match = re.search(delete_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            table_name = self.clean_identifier(match.group(1)).replace('#', 'temp_')
            where_clause = match.group(2) if match.group(2) else None
            
            code_lines.append(f"# Delete from {table_name}")
            
            if where_clause:
                where_pandas = self._convert_where_enhanced([where_clause])
                code_lines.append(f"# Apply WHERE condition: {where_clause}")
                code_lines.append(f"mask = ~{table_name}.eval('{where_pandas}')  # Keep rows that DON'T match")
                code_lines.append(f"{table_name} = {table_name}[mask].reset_index(drop=True)")
            else:
                code_lines.append(f"# Delete all rows")
                code_lines.append(f"{table_name} = {table_name}.iloc[0:0].copy()  # Keep structure, remove data")
        else:
            code_lines.append(f"# Could not parse DELETE statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_declare_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle DECLARE statement for variables"""
        code_lines = []
        
        # DECLARE pattern
        declare_pattern = r'DECLARE\s+(@\w+)\s+(\w+)(?:\s*=\s*(.+))?'
        match = re.search(declare_pattern, original_statement, re.IGNORECASE)
        
        if match:
            var_name = match.group(1).replace('@', 'var_')
            var_type = match.group(2)
            var_value = match.group(3) if match.group(3) else None
            
            code_lines.append(f"# Declare variable: {match.group(1)}")
            
            if var_value:
                # Clean the value
                clean_value = var_value.strip().strip("'\"")
                if var_type.upper() in ['INT', 'INTEGER', 'BIGINT']:
                    code_lines.append(f"{var_name} = {clean_value}")
                elif var_type.upper() in ['VARCHAR', 'NVARCHAR', 'CHAR', 'TEXT']:
                    code_lines.append(f"{var_name} = '{clean_value}'")
                elif var_type.upper() in ['DECIMAL', 'FLOAT', 'REAL']:
                    code_lines.append(f"{var_name} = {clean_value}")
                else:
                    code_lines.append(f"{var_name} = '{clean_value}'  # {var_type}")
            else:
                code_lines.append(f"{var_name} = None  # {var_type}")
            
            self.variables[match.group(1)] = var_name
        else:
            code_lines.append(f"# Could not parse DECLARE statement: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_set_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle SET statement"""
        code_lines = []
        
        # SET variable pattern
        set_pattern = r'SET\s+(@\w+)\s*=\s*(.+)'
        match = re.search(set_pattern, original_statement, re.IGNORECASE)
        
        if match:
            var_name = match.group(1)
            var_value = match.group(2).strip()
            
            if var_name in self.variables:
                py_var = self.variables[var_name]
                code_lines.append(f"# Set variable: {var_name}")
                code_lines.append(f"{py_var} = {self._convert_value(var_value)}")
            else:
                # SET options (like SET NOCOUNT ON)
                code_lines.append(f"# SQL Server option: {original_statement}")
                code_lines.append("# No Pandas equivalent needed")
        
        return '\n'.join(code_lines)
    
    def handle_if_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle IF statement"""
        code_lines = []
        code_lines.append("# IF statement")
        
        # Basic IF pattern
        if_pattern = r'IF\s+(.*?)\s+BEGIN\s+(.*?)\s+END'
        match = re.search(if_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            condition = match.group(1)
            body = match.group(2)
            
            pandas_condition = self._convert_condition(condition)
            code_lines.append(f"if {pandas_condition}:")
            
            # Process body statements
            body_statements = self._split_statements(body)
            for stmt in body_statements:
                if stmt.strip():
                    code_lines.append(f"    # {stmt.strip()}")
            code_lines.append("    pass  # Add implementation")
        else:
            # Simple IF without BEGIN/END
            code_lines.append(f"# Simple IF: {original_statement}")
        
        return '\n'.join(code_lines)
    
    def handle_while_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle WHILE statement"""
        code_lines = []
        code_lines.append("# WHILE loop")
        
        while_pattern = r'WHILE\s+(.*?)\s+BEGIN\s+(.*?)\s+END'
        match = re.search(while_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            condition = match.group(1)
            body = match.group(2)
            
            pandas_condition = self._convert_condition(condition)
            code_lines.append(f"while {pandas_condition}:")
            code_lines.append("    # Loop body")
            code_lines.append("    # Add implementation")
            code_lines.append("    # Don't forget to update loop condition!")
            code_lines.append("    break  # Remove this when implementing")
        
        return '\n'.join(code_lines)
    
    def handle_print_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle PRINT statement"""
        code_lines = []
        
        print_pattern = r'PRINT\s+(.*)'
        match = re.search(print_pattern, original_statement, re.IGNORECASE)
        
        if match:
            print_content = match.group(1).strip()
            # Convert variables
            for var, py_var in self.variables.items():
                print_content = print_content.replace(var, py_var)
            
            code_lines.append(f"print({print_content})")
        
        return '\n'.join(code_lines)
    
    def handle_exec_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle EXEC/EXECUTE statement"""
        code_lines = []
        code_lines.append("# EXEC/EXECUTE statement")
        code_lines.append(f"# Original: {original_statement}")
        code_lines.append("# Stored procedures require custom implementation")
        code_lines.append("# Consider creating a Python function for the procedure logic")
        
        return '\n'.join(code_lines)
    
    def handle_merge_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle MERGE statement"""
        code_lines = []
        code_lines.append("# MERGE statement")
        code_lines.append("# MERGE operations in Pandas require custom logic")
        
        # Basic MERGE pattern parsing
        merge_pattern = r'MERGE\s+(\w+)\s+.*?USING\s+(\w+)\s+.*?ON\s+(.*?)\s+WHEN'
        match = re.search(merge_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            target_table = self.clean_identifier(match.group(1))
            source_table = self.clean_identifier(match.group(2))
            join_condition = match.group(3)
            
            code_lines.append(f"# Target: {target_table}, Source: {source_table}")
            code_lines.append(f"# Join condition: {join_condition}")
            code_lines.append("")
            code_lines.append("# Basic MERGE implementation:")
            code_lines.append(f"# 1. Identify matching records")
            code_lines.append(f"merged_df = pd.merge({target_table}, {source_table}, ")
            code_lines.append(f"                     on=[...], how='outer', indicator=True)")
            code_lines.append("")
            code_lines.append("# 2. Handle MATCHED records (UPDATE)")
            code_lines.append("matched_mask = merged_df['_merge'] == 'both'")
            code_lines.append("# Update logic here")
            code_lines.append("")
            code_lines.append("# 3. Handle NOT MATCHED BY TARGET (INSERT)")
            code_lines.append("insert_mask = merged_df['_merge'] == 'right_only'")
            code_lines.append("# Insert logic here")
            code_lines.append("")
            code_lines.append("# 4. Handle NOT MATCHED BY SOURCE (DELETE)")
            code_lines.append("delete_mask = merged_df['_merge'] == 'left_only'")
            code_lines.append("# Delete logic here")
        
        return '\n'.join(code_lines)
    
    def handle_unknown_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle unknown statement types"""
        return f"# Unknown statement type\n# Original: {original_statement}\n# Manual conversion required"
    
    def handle_create_view(self, parsed_statement, original_statement: str) -> str:
        """Handle CREATE VIEW statement"""
        code_lines = []
        
        view_pattern = r'CREATE\s+VIEW\s+(\w+)\s+AS\s+(SELECT.*)'
        match = re.search(view_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            view_name = match.group(1)
            select_query = match.group(2)
            
            code_lines.append(f"# Create view: {view_name}")
            code_lines.append(f"# Views in Pandas are typically implemented as functions")
            code_lines.append(f"def view_{view_name.lower()}():")
            
            select_code = self.convert_select_query(select_query)
            code_lines.append(f"    return {select_code}")
            code_lines.append("")
            code_lines.append(f"# Usage: df = view_{view_name.lower()}()")
        
        return '\n'.join(code_lines)
    
    def handle_cte_statement(self, parsed_statement, original_statement: str) -> str:
        """Handle Common Table Expression (WITH clause)"""
        code_lines = []
        code_lines.append("# Common Table Expression (CTE)")
        
        # Parse CTEs
        cte_pattern = r'WITH\s+(.*?)\s+SELECT'
        match = re.search(cte_pattern, original_statement, re.IGNORECASE | re.DOTALL)
        
        if match:
            cte_definitions = match.group(1)
            
            # Extract individual CTEs
            cte_list = []
            current_cte = []
            paren_depth = 0
            
            for char in cte_definitions:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    cte_list.append(''.join(current_cte).strip())
                    current_cte = []
                    continue
                current_cte.append(char)
            
            if current_cte:
                cte_list.append(''.join(current_cte).strip())
            
            # Process each CTE
            for cte in cte_list:
                cte_name_pattern = r'^(\w+)\s+AS\s*\((.*)\)
                cte_match = re.search(cte_name_pattern, cte, re.IGNORECASE | re.DOTALL)
                
                if cte_match:
                    cte_name = cte_match.group(1)
                    cte_query = cte_match.group(2)
                    
                    code_lines.append(f"# CTE: {cte_name}")
                    cte_code = self.convert_select_query(cte_query)
                    code_lines.append(f"{cte_name.lower()} = {cte_code}")
                    
                    # Store CTE for reference
                    self.cte_definitions[cte_name] = cte_name.lower()
            
            # Process main query
            main_query = re.sub(r'WITH\s+.*?\s+(?=SELECT)', '', original_statement, flags=re.IGNORECASE | re.DOTALL)
            code_lines.append("")
            code_lines.append("# Main query")
            main_code = self.convert_select_query(main_query, self.cte_definitions)
            code_lines.append(f"result = {main_code}")
        
        return '\n'.join(code_lines)
    
    def parse_column_definitions(self, columns_def: str) -> Dict[str, str]:
        """Parse column definitions from CREATE TABLE"""
        columns = {}
        
        # Simple parsing - split by comma and extract name/type
        col_defs = [col.strip() for col in columns_def.split(',')]
        
        for col_def in col_defs:
            parts = col_def.split()
            if len(parts) >= 2:
                col_name = self.clean_identifier(parts[0])
                col_type = parts[1]
                columns[col_name] = col_type
        
        return columns
    
    def sql_type_to_pandas(self, sql_type: str) -> str:
        """Convert SQL data type to Pandas dtype"""
        sql_type_upper = sql_type.upper()
        
        if any(t in sql_type_upper for t in ['INT', 'BIGINT', 'SMALLINT']):
            return 'int64'
        elif any(t in sql_type_upper for t in ['DECIMAL', 'FLOAT', 'REAL', 'NUMERIC']):
            return 'float64'
        elif any(t in sql_type_upper for t in ['VARCHAR', 'CHAR', 'TEXT', 'NVARCHAR']):
            return 'object'
        elif any(t in sql_type_upper for t in ['DATE', 'DATETIME', 'TIMESTAMP']):
            return 'datetime64[ns]'
        elif 'BIT' in sql_type_upper:
            return 'bool'
        else:
            return 'object'


# Enhanced test function with JSON and STRING_AGG examples
def test_enhanced_converter():
    """Test the enhanced converter with JSON and STRING_AGG features"""
    converter = TSQLToPandasConverter()
    
    test_queries = [
        # JSON operations
        """
        SELECT 
            OrderID,
            CustomerID,
            JSON_VALUE(OrderDetails, '$.customer.name') as CustomerName,
            JSON_VALUE(OrderDetails, '$.items[0].product') as FirstProduct,
            JSON_QUERY(OrderDetails, '$.items') as AllItems,
            ISJSON(OrderDetails) as IsValidJSON,
            JSON_VALUE(OrderDetails, '$.total') as OrderTotal
        FROM Orders
        WHERE ISJSON(OrderDetails) = 1
            AND JSON_VALUE(OrderDetails, '$.status') = 'completed';
        """,
        
        # STRING_AGG with GROUP BY
        """
        SELECT 
            CategoryID,
            CategoryName,
            COUNT(*) as ProductCount,
            STRING_AGG(ProductName, ', ') WITHIN GROUP (ORDER BY ProductName) as ProductList,
            STRING_AGG(CAST(Price AS VARCHAR), ' | ') as PriceList
        FROM Products p
        INNER JOIN Categories c ON p.CategoryID = c.CategoryID
        WHERE p.Active = 1
        GROUP BY CategoryID, CategoryName
        HAVING COUNT(*) > 5
        ORDER BY CategoryID;
        """,
        
        # Complex JSON with nested paths
        """
        SELECT 
            UserID,
            Username,
            JSON_VALUE(UserProfile, '$.address.city') as City,
            JSON_VALUE(UserProfile, '$.address.state') as State,
            JSON_VALUE(UserProfile, '$.preferences.notifications.email') as EmailNotifications,
            JSON_QUERY(UserProfile, '$.skills') as UserSkills,
            JSON_MODIFY(UserProfile, '$.lastLogin', GETDATE()) as UpdatedProfile
        FROM Users
        WHERE JSON_VALUE(UserProfile, '$.account.type') = 'premium'
            AND ISJSON(UserProfile) = 1;
        """,
        
        # FOR JSON query
        """
        SELECT 
            o.OrderID,
            o.OrderDate,
            c.CustomerName,
            c.Email,
            (
                SELECT 
                    ProductID,
                    ProductName,
                    Quantity,
                    Price
                FROM OrderDetails od
                INNER JOIN Products p ON od.ProductID = p.ProductID
                WHERE od.OrderID = o.OrderID
                FOR JSON PATH
            ) as OrderItems
        FROM Orders o
        INNER JOIN Customers c ON o.CustomerID = c.CustomerID
        WHERE o.OrderDate >= '2023-01-01'
        FOR JSON PATH, ROOT('Orders'), INCLUDE_NULL_VALUES;
        """,
        
        # OPENJSON with schema
        """
        SELECT 
            j.OrderID,
            j.CustomerName,
            j.OrderTotal,
            j.OrderDate
        FROM OPENJSON(@jsonData)
        WITH (
            OrderID INT '$.id',
            CustomerName VARCHAR(100) '$.customer.name',
            OrderTotal DECIMAL(10,2) '$.total',
            OrderDate DATETIME '$.date'
        ) AS j
        WHERE j.OrderTotal > 100;
        """,
        
        # Combined JSON and STRING_AGG
        """
        WITH OrderSummary AS (
            SELECT 
                CustomerID,
                JSON_VALUE(OrderData, '$.region') as Region,
                COUNT(*) as OrderCount,
                SUM(CAST(JSON_VALUE(OrderData, '$.total') AS DECIMAL(10,2))) as TotalSpent
            FROM Orders
            WHERE ISJSON(OrderData) = 1
            GROUP BY CustomerID, JSON_VALUE(OrderData, '$.region')
        )
        SELECT 
            Region,
            COUNT(DISTINCT CustomerID) as UniqueCustomers,
            STRING_AGG(
                CONCAT(CustomerID, ':', CAST(TotalSpent AS VARCHAR)), 
                '; '
            ) WITHIN GROUP (ORDER BY TotalSpent DESC) as CustomerSpending,
            SUM(TotalSpent) as RegionTotal
        FROM OrderSummary
        GROUP BY Region
        HAVING COUNT(DISTINCT CustomerID) > 10
        ORDER BY RegionTotal DESC;
        """,
        
        # Multiple STRING_AGG with different separators
        """
        SELECT 
            DepartmentID,
            DepartmentName,
            STRING_AGG(EmployeeName, ', ') as EmployeeList,
            STRING_AGG(
                CONCAT(EmployeeName, ' (', JobTitle, ')'), 
                CHAR(10)
            ) WITHIN GROUP (ORDER BY HireDate) as EmployeeDetails,
            STRING_AGG(CAST(Salary AS VARCHAR), ' - ') WITHIN GROUP (ORDER BY Salary DESC) as SalaryRange
        FROM Employees e
        INNER JOIN Departments d ON e.DepartmentID = d.DepartmentID
        WHERE e.Active = 1
        GROUP BY DepartmentID, DepartmentName;
        """,
        
        # JSON_MODIFY operations
        """
        UPDATE Users
        SET UserProfile = JSON_MODIFY(
            JSON_MODIFY(
                JSON_MODIFY(
                    UserProfile,
                    '$.lastActivity',
                    GETDATE()
                ),
                '$.loginCount',
                CAST(JSON_VALUE(UserProfile, '$.loginCount') AS INT) + 1
            ),
            '$.preferences.theme',
            'dark'
        )
        WHERE UserID = 123
            AND ISJSON(UserProfile) = 1;
        """,
        
        # Nested JSON queries
        """
        SELECT 
            p.ProductID,
            p.ProductName,
            JSON_QUERY(p.Specifications, '$.dimensions') as Dimensions,
            JSON_VALUE(p.Specifications, '$.dimensions.weight') as Weight,
            JSON_VALUE(p.Specifications, '$.dimensions.unit') as WeightUnit,
            (
                SELECT 
                    ReviewID,
                    Rating,
                    Comment
                FROM ProductReviews
                WHERE ProductID = p.ProductID
                FOR JSON PATH
            ) as Reviews
        FROM Products p
        WHERE JSON_VALUE(p.Specifications, '$.category') = 'Electronics'
        FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER;
        """
    ]
    
    print("Enhanced T-SQL to Pandas Converter - JSON and STRING_AGG Test")
    print("=" * 60)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n--- Test Query {i} ---")
        print("Original SQL:")
        print(query.strip())
        print("\nConverted Pandas Code:")
        print(converter.convert_sql_to_pandas(query))
        print("-" * 40)

if __name__ == "__main__":
    test_enhanced_converter()
