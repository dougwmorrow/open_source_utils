import os
from pathlib import Path
import re
from typing import Set, Dict, List, Tuple
from datetime import datetime

class PathsGenerator:
    """Automatically generate paths.py based on project structure."""
    
    def __init__(self, project_root: Path = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent
        self.ignore_dirs = {
            '.git', '.vscode', '__pycache__', '.pytest_cache', 
            'venv', '.venv', 'env', '.env', 'node_modules',
            '.idea', '.mypy_cache', 'dist', 'build', '*.egg-info',
            '.tox', 'htmlcov', '.coverage', '.DS_Store'
        }
        self.ignore_patterns = [
            re.compile(r'\..*'),  # Hidden files/folders
            re.compile(r'.*\.pyc$'),  # Python compiled files
            re.compile(r'.*\.egg-info$'),  # Egg info directories
        ]
        self.tracked_extensions = {
            '.json', '.yaml', '.yml', '.ini', '.cfg', '.conf',
            '.txt', '.csv', '.xlsx', '.db', '.sqlite'
        }
        
    def should_ignore(self, path: Path) -> bool:
        """Check if path should be ignored."""
        name = path.name
        
        # Check exact matches
        if name in self.ignore_dirs:
            return True
            
        # Check patterns
        for pattern in self.ignore_patterns:
            if pattern.match(name):
                return True
                
        return False
    
    def to_property_name(self, path_str: str) -> str:
        """Convert path to valid Python property name."""
        # Remove special characters and convert to snake_case
        name = re.sub(r'[^\w\s]', '', path_str)
        name = re.sub(r'\s+', '_', name)
        name = name.lower()
        
        # Ensure it doesn't start with a number
        if name and name[0].isdigit():
            name = f"dir_{name}"
            
        # Avoid Python keywords
        keywords = {'class', 'def', 'import', 'from', 'as', 'is', 'in', 'for', 'if', 'else'}
        if name in keywords:
            name = f"{name}_dir"
            
        return name
    
    def scan_directories(self) -> Dict[str, Path]:
        """Scan project and return all directories."""
        directories = {}
        
        for root, dirs, files in os.walk(self.project_root):
            # Filter out ignored directories
            dirs[:] = [d for d in dirs if not self.should_ignore(Path(root) / d)]
            
            root_path = Path(root)
            relative_path = root_path.relative_to(self.project_root)
            
            # Skip if it's the root directory
            if relative_path == Path('.'):
                continue
                
            # Create property name based on relative path
            path_parts = str(relative_path).replace(os.sep, '_')
            prop_name = self.to_property_name(path_parts)
            
            if prop_name and prop_name not in directories:
                directories[prop_name] = relative_path
        
        return directories
    
    def scan_important_files(self) -> Dict[str, Path]:
        """Scan for important configuration and data files."""
        important_files = {}
        
        for root, dirs, files in os.walk(self.project_root):
            # Filter out ignored directories
            dirs[:] = [d for d in dirs if not self.should_ignore(Path(root) / d)]
            
            root_path = Path(root)
            
            for file in files:
                file_path = root_path / file
                
                # Check if file has tracked extension
                if any(file.endswith(ext) for ext in self.tracked_extensions):
                    relative_path = file_path.relative_to(self.project_root)
                    
                    # Create property name
                    file_prop = file.replace('.', '_').replace('-', '_')
                    parent_parts = str(relative_path.parent).replace(os.sep, '_')
                    
                    if parent_parts == '.':
                        prop_name = f"file_{self.to_property_name(file_prop)}"
                    else:
                        parent_name = self.to_property_name(parent_parts)
                        prop_name = f"file_{parent_name}_{self.to_property_name(file_prop)}"
                    
                    if prop_name not in important_files:
                        important_files[prop_name] = relative_path
        
        return important_files
    
    def generate_paths_py(self, include_files: bool = True) -> str:
        """Generate the content for paths.py."""
        directories = self.scan_directories()
        files = self.scan_important_files() if include_files else {}
        
        # Sort for consistent output
        sorted_dirs = sorted(directories.items())
        sorted_files = sorted(files.items())
        
        content = f'''"""
Auto-generated project paths management.
Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

This file is automatically generated by update_paths.py
Do not modify manually - changes will be overwritten!
"""

from pathlib import Path
import os
from typing import Union, Optional

class ProjectPaths:
    """Centralized path management for the entire project."""
    
    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        """
        Initialize project paths.
        
        Args:
            base_path: Custom base path. If None, uses the directory containing this file.
        """
        if base_path is None:
            self._base = Path(__file__).parent.absolute()
        else:
            self._base = Path(base_path).absolute()
    
    @property
    def base(self) -> Path:
        """Get the base project directory."""
        return self._base
    
    # === DIRECTORIES ===
'''
        
        # Add directory properties
        for prop_name, rel_path in sorted_dirs:
            content += f'''    
    @property
    def {prop_name}(self) -> Path:
        """Path to {rel_path}"""
        return self._base / "{str(rel_path).replace(os.sep, '/')}"
'''
        
        if files:
            content += '\n    # === FILES ===\n'
            
            # Add file properties
            for prop_name, rel_path in sorted_files:
                content += f'''    
    @property
    def {prop_name}(self) -> Path:
        """Path to {rel_path}"""
        return self._base / "{str(rel_path).replace(os.sep, '/')}"
'''
        
        # Add utility methods
        content += '''
    
    # === UTILITY METHODS ===
    
    def create_all_directories(self):
        """Create all project directories if they don't exist."""
        directories = [
'''
        
        # List all directories for creation
        for prop_name, _ in sorted_dirs:
            content += f'            self.{prop_name},\n'
        
        content += '''        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_file_in(self, directory: str, filename: str) -> Path:
        """
        Get a file path in a specific directory.
        
        Args:
            directory: Name of the directory property
            filename: Name of the file
            
        Returns:
            Path to the file
        """
        if hasattr(self, directory):
            return getattr(self, directory) / filename
        else:
            raise AttributeError(f"Directory '{directory}' not found")
    
    def __str__(self) -> str:
        """String representation of all paths."""
        paths_info = [f"Project Base: {self._base}"]
        
        # Add all directory paths
        for attr_name in sorted(dir(self)):
            if not attr_name.startswith('_') and attr_name not in ['base', 'create_all_directories', 'get_file_in']:
                attr = getattr(self, attr_name)
                if isinstance(attr, Path):
                    paths_info.append(f"{attr_name}: {attr}")
        
        return "\\n".join(paths_info)


# Create a singleton instance
paths = ProjectPaths()

# === CONVENIENCE FUNCTIONS ===

def get_path(directory: str, filename: str = None) -> Path:
    """
    Get a path from the project structure.
    
    Args:
        directory: Directory property name
        filename: Optional filename to append
        
    Returns:
        Path object
    """
    if hasattr(paths, directory):
        dir_path = getattr(paths, directory)
        return dir_path / filename if filename else dir_path
    else:
        raise AttributeError(f"Path '{directory}' not found")
'''
        
        return content
    
    def update_paths_file(self, output_file: str = "paths.py", include_files: bool = True):
        """Generate and write the paths.py file."""
        content = self.generate_paths_py(include_files)
        
        output_path = self.project_root / output_file
        
        # Backup existing file if it exists
        if output_path.exists():
            backup_path = output_path.with_suffix('.py.backup')
            backup_path.write_text(output_path.read_text())
            print(f"Backed up existing file to: {backup_path}")
        
        # Write new content
        output_path.write_text(content)
        print(f"Generated paths.py with {len(self.scan_directories())} directories")
        if include_files:
            print(f"and {len(self.scan_important_files())} tracked files")
        
        return output_path


def main():
    """Run the paths generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate paths.py from project structure')
    parser.add_argument('--root', type=str, help='Project root directory (default: current directory)')
    parser.add_argument('--no-files', action='store_true', help='Exclude file paths, only include directories')
    parser.add_argument('--output', type=str, default='paths.py', help='Output filename (default: paths.py)')
    
    args = parser.parse_args()
    
    generator = PathsGenerator(args.root)
    
    print("Scanning project structure...")
    print(f"Project root: {generator.project_root}")
    
    # Show what will be included
    dirs = generator.scan_directories()
    print(f"\nFound {len(dirs)} directories:")
    for name, path in sorted(dirs.items())[:10]:
        print(f"  {name} -> {path}")
    if len(dirs) > 10:
        print(f"  ... and {len(dirs) - 10} more")
    
    if not args.no_files:
        files = generator.scan_important_files()
        if files:
            print(f"\nFound {len(files)} tracked files:")
            for name, path in sorted(files.items())[:10]:
                print(f"  {name} -> {path}")
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more")
    
    # Generate file
    print(f"\nGenerating {args.output}...")
    output_path = generator.update_paths_file(args.output, not args.no_files)
    print(f"Successfully generated: {output_path}")


if __name__ == "__main__":
    main()