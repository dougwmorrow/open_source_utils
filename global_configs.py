import json
import configparser
import os
from pathlib import Path
from typing import Any, Dict
import re

class ConfigGenerator:
    """Scan for all config files and create a unified config.py"""
    
    def __init__(self, project_root: Path = None):
        self.root = Path(project_root) if project_root else Path.cwd()
        self.config_extensions = {
            '.json': self._parse_json,
            '.ini': self._parse_ini,
            '.cfg': self._parse_ini,
            '.env': self._parse_env,
            '.py': self._parse_py_config
        }
    
    def _parse_json(self, file_path: Path) -> Dict:
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def _parse_ini(self, file_path: Path) -> Dict:
        config = configparser.ConfigParser()
        config.read(file_path)
        return {section: dict(config[section]) for section in config.sections()}
    
    def _parse_env(self, file_path: Path) -> Dict:
        env_vars = {}
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip().strip('"\'')
        return env_vars
    
    def _parse_py_config(self, file_path: Path) -> Dict:
        # Extract uppercase variables from Python files
        config_vars = {}
        with open(file_path, 'r') as f:
            content = f.read()
            # Find all UPPERCASE = value patterns
            pattern = r'^([A-Z_]+)\s*=\s*(.+)$'
            for match in re.finditer(pattern, content, re.MULTILINE):
                var_name = match.group(1)
                var_value = match.group(2).strip()
                try:
                    # Safely evaluate simple literals
                    config_vars[var_name] = eval(var_value, {"__builtins__": {}})
                except:
                    config_vars[var_name] = var_value
        return config_vars
    
    def generate_config_py(self) -> str:
        """Generate config.py content"""
        configs = {}
        
        # Scan for all config files
        for root, dirs, files in os.walk(self.root):
            # Skip hidden and cache directories
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
            
            for file in files:
                file_path = Path(root) / file
                ext = file_path.suffix
                
                if ext in self.config_extensions:
                    rel_path = file_path.relative_to(self.root)
                    config_name = file_path.stem.upper().replace('-', '_').replace('.', '_')
                    
                    try:
                        config_data = self.config_extensions[ext](file_path)
                        configs[config_name] = {
                            'path': str(rel_path),
                            'data': config_data
                        }
                    except Exception as e:
                        print(f"Warning: Could not parse {file_path}: {e}")
        
        # Generate Python code
        content = '''"""
Auto-generated configuration management.
Generated from all config files in the project.
"""

import os
from pathlib import Path
from typing import Any, Dict

class Config:
    """Unified configuration from all config files."""
    
    def __init__(self):
        self._base = Path(__file__).parent
        self._configs = {}
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration data."""
        self._configs = {
'''
        
        # Add all config data
        for name, config in configs.items():
            content += f'            "{name}": {repr(config["data"])},\n'
        
        content += '''        }
    
    def get(self, config_name: str, key: str = None, default: Any = None) -> Any:
        """Get configuration value."""
        config_name = config_name.upper()
        if config_name not in self._configs:
            return default
            
        if key is None:
            return self._configs[config_name]
        
        return self._configs[config_name].get(key, default)
    
    def __getattr__(self, name: str) -> Any:
        """Access configs as attributes."""
        return self._configs.get(name.upper(), {})


# Singleton instance
config = Config()
'''
        
        return content