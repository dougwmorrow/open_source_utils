import ast
import os
from pathlib import Path
from typing import Set, Dict, List

class ConstantsExtractor:
    """Extract all constants from Python files and generate constants.py"""
    
    def __init__(self, project_root: Path = None):
        self.root = Path(project_root) if project_root else Path.cwd()
        self.constants = {}
        self.ignore_values = {None, True, False, '', 0, 1, -1}
    
    def extract_constants_from_file(self, file_path: Path) -> Dict:
        """Extract string/number literals from a Python file."""
        constants = {
            'strings': set(),
            'numbers': set(),
            'assignments': {}
        }
        
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                tree = ast.parse(f.read())
                
                for node in ast.walk(tree):
                    # Extract string literals
                    if isinstance(node, ast.Str):
                        if len(node.s) > 2 and node.s not in self.ignore_values:
                            constants['strings'].add(node.s)
                    
                    # Extract number literals
                    elif isinstance(node, ast.Num):
                        if node.n not in self.ignore_values and abs(node.n) > 1:
                            constants['numbers'].add(node.n)
                    
                    # Extract uppercase assignments (existing constants)
                    elif isinstance(node, ast.Assign):
                        for target in node.targets:
                            if isinstance(target, ast.Name) and target.id.isupper():
                                if isinstance(node.value, (ast.Str, ast.Num)):
                                    value = node.value.s if isinstance(node.value, ast.Str) else node.value.n
                                    constants['assignments'][target.id] = value
                                    
            except SyntaxError:
                print(f"Syntax error in {file_path}")
                
        return constants
    
    def generate_constants_py(self) -> str:
        """Generate constants.py from extracted constants."""
        all_strings = set()
        all_numbers = set()
        all_assignments = {}
        
        # Scan all Python files
        for root, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
            
            for file in files:
                if file.endswith('.py') and file not in ['constants.py', 'update_constants.py']:
                    file_path = Path(root) / file
                    constants = self.extract_constants_from_file(file_path)
                    
                    all_strings.update(constants['strings'])
                    all_numbers.update(constants['numbers'])
                    all_assignments.update(constants['assignments'])
        
        # Generate content
        content = '''"""
Auto-generated constants from project analysis.
Review and rename as needed.
"""

# Existing constants found in code
'''
        
        # Add existing constants
        for name, value in sorted(all_assignments.items()):
            if isinstance(value, str):
                content += f'{name} = "{value}"\n'
            else:
                content += f'{name} = {value}\n'
        
        content += '\n# Frequently used strings (rename as needed)\n'
        for i, string in enumerate(sorted(all_strings)[:20]):  # Top 20
            const_name = self._suggest_constant_name(string)
            content += f'# {const_name} = "{string}"\n'
        
        content += '\n# Frequently used numbers (rename as needed)\n'
        for number in sorted(all_numbers)[:20]:  # Top 20
            content += f'# NUMBER_{int(number)} = {number}\n'
        
        return content
    
    def _suggest_constant_name(self, value: str) -> str:
        """Suggest a constant name based on the value."""
        # Simple heuristic for naming
        name = value.upper()[:30]
        name = ''.join(c if c.isalnum() else '_' for c in name)
        name = name.strip('_')
        return name if name else 'CONSTANT'