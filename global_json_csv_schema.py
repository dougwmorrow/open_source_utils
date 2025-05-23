import json
import csv
import os
from pathlib import Path
from typing import Dict, Set, Any, List
from collections import defaultdict

class SchemaGenerator:
    """Generate schema definitions from data files."""
    
    def __init__(self, project_root: Path = None):
        self.root = Path(project_root) if project_root else Path.cwd()
        self.schemas = {}
    
    def infer_type(self, value: Any) -> str:
        """Infer Python type from value."""
        if value is None:
            return 'Optional[Any]'
        elif isinstance(value, bool):
            return 'bool'
        elif isinstance(value, int):
            return 'int'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'str'
        elif isinstance(value, list):
            if value:
                inner_type = self.infer_type(value[0])
                return f'List[{inner_type}]'
            return 'List[Any]'
        elif isinstance(value, dict):
            return 'Dict[str, Any]'
        else:
            return 'Any'
    
    def analyze_json_schema(self, file_path: Path) -> Dict:
        """Extract schema from JSON file."""
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list) and data:
            # For lists, analyze first item
            sample = data[0]
        else:
            sample = data
        
        schema = {}
        if isinstance(sample, dict):
            for key, value in sample.items():
                schema[key] = self.infer_type(value)
        
        return schema
    
    def analyze_csv_schema(self, file_path: Path) -> Dict:
        """Extract schema from CSV file."""
        schema = defaultdict(set)
        
        with open(file_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key, value in row.items():
                    if value:  # Skip empty values
                        try:
                            int(value)
                            schema[key].add('int')
                        except ValueError:
                            try:
                                float(value)
                                schema[key].add('float')
                            except ValueError:
                                schema[key].add('str')
        
        # Determine final types
        final_schema = {}
        for key, types in schema.items():
            if len(types) == 1:
                final_schema[key] = types.pop()
            elif 'float' in types:
                final_schema[key] = 'float'
            else:
                final_schema[key] = 'str'
        
        return final_schema
    
    def generate_schemas_py(self) -> str:
        """Generate schemas.py with type definitions."""
        json_schemas = {}
        csv_schemas = {}
        
        # Scan for data files
        for root, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for file in files:
                file_path = Path(root) / file
                schema_name = file_path.stem.title().replace('_', '')
                
                if file.endswith('.json'):
                    try:
                        schema = self.analyze_json_schema(file_path)
                        if schema:
                            json_schemas[schema_name] = schema
                    except:
                        pass
                
                elif file.endswith('.csv'):
                    try:
                        schema = self.analyze_csv_schema(file_path)
                        if schema:
                            csv_schemas[schema_name] = schema
                    except:
                        pass
        
        # Generate content
        content = '''"""
Auto-generated schema definitions from data files.
"""

from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
import json


'''
        
        # Add JSON schemas
        if json_schemas:
            content += '# JSON File Schemas\n\n'
            for name, schema in json_schemas.items():
                content += f'@dataclass\nclass {name}Schema:\n'
                content += '    """Auto-generated from JSON file."""\n'
                for field, field_type in schema.items():
                    content += f'    {field}: {field_type}\n'
                content += '\n'
        
        # Add CSV schemas  
        if csv_schemas:
            content += '# CSV File Schemas\n\n'
            for name, schema in csv_schemas.items():
                content += f'@dataclass\nclass {name}CSV:\n'
                content += '    """Auto-generated from CSV file."""\n'
                for field, field_type in schema.items():
                    safe_field = field.replace(' ', '_').replace('-', '_').lower()
                    content += f'    {safe_field}: {field_type}\n'
                content += '\n'
        
        return content