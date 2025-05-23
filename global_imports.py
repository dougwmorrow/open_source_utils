import ast
import os
from pathlib import Path
from collections import Counter
from typing import Set, Dict, List

class ImportsAnalyzer:
    """Analyze and consolidate common imports across the project."""
    
    def __init__(self, project_root: Path = None):
        self.root = Path(project_root) if project_root else Path.cwd()
        self.imports_counter = Counter()
        self.from_imports_counter = Counter()
    
    def analyze_file(self, file_path: Path):
        """Extract imports from a Python file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                tree = ast.parse(f.read())
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            self.imports_counter[alias.name] += 1
                    
                    elif isinstance(node, ast.ImportFrom):
                        module = node.module or ''
                        for alias in node.names:
                            import_str = f"from {module} import {alias.name}"
                            self.from_imports_counter[import_str] += 1
                            
            except SyntaxError:
                pass
    
    def generate_imports_py(self) -> str:
        """Generate imports.py with common imports."""
        # Analyze all Python files
        for root, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
            
            for file in files:
                if file.endswith('.py'):
                    file_path = Path(root) / file
                    self.analyze_file(file_path)
        
        # Generate content
        content = '''"""
Common imports used across the project.
Usage: from imports import *
"""

# Standard library imports (used 3+ times)
'''
        
        # Add common standard library imports
        stdlib_modules = {'os', 'sys', 'json', 'csv', 'datetime', 'pathlib', 
                         're', 'typing', 'collections', 'itertools'}
        
        for module, count in self.imports_counter.most_common():
            if count >= 3 and any(module.startswith(std) for std in stdlib_modules):
                content += f'import {module}  # Used {count} times\n'
        
        content += '\n# Common from imports\n'
        for import_str, count in self.from_imports_counter.most_common(20):
            if count >= 2:
                content += f'{import_str}  # Used {count} times\n'
        
        content += '''

# Convenience imports
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Union, Any
from datetime import datetime, timedelta

# Project-specific imports (update as needed)
# from paths import paths
# from config import config
'''
        
        return content