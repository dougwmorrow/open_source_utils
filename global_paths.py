import os
from pathlib import Path
import re
from typing import Set, Dict, List, Tuple, Optional
from datetime import datetime
import fnmatch

class GitignoreParser:
    """Parse and apply .gitignore rules."""
    
    def __init__(self, root_path: Path):
        self.root_path = root_path
        self.ignore_patterns = self._load_gitignore_patterns()
        
    def _load_gitignore_patterns(self) -> Dict[Path, List[str]]:
        """Load all .gitignore files in the project."""
        patterns = {}
        
        for root, dirs, files in os.walk(self.root_path):
            if '.gitignore' in files:
                gitignore_path = Path(root) / '.gitignore'
                patterns[Path(root)] = self._parse_gitignore(gitignore_path)
                
        return patterns
    
    def _parse_gitignore(self, gitignore_path: Path) -> List[str]:
        """Parse a single .gitignore file."""
        patterns = []
        
        try:
            with open(gitignore_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if line and not line.startswith('#'):
                        patterns.append(line)
        except Exception:
            pass
            
        return patterns
    
    def should_ignore(self, path: Path) -> bool:
        """Check if a path should be ignored based on .gitignore rules."""
        # Check each .gitignore file's rules
        for gitignore_dir, patterns in self.ignore_patterns.items():
            # Only apply rules if the path is within the gitignore's directory
            try:
                relative_to_gitignore = path.relative_to(gitignore_dir)
                
                for pattern in patterns:
                    # Handle directory patterns (ending with /)
                    if pattern.endswith('/'):
                        if path.is_dir() and self._matches_pattern(path.name, pattern[:-1]):
                            return True
                    # Handle negation patterns (starting with !)
                    elif pattern.startswith('!'):
                        if self._matches_pattern(str(relative_to_gitignore), pattern[1:]):
                            return False
                    # Regular patterns
                    else:
                        if self._matches_pattern(str(relative_to_gitignore), pattern):
                            return True
                        # Also check just the filename
                        if self._matches_pattern(path.name, pattern):
                            return True
                            
            except ValueError:
                # Path is not relative to this gitignore directory
                continue
                
        return False
    
    def _matches_pattern(self, path_str: str, pattern: str) -> bool:
        """Check if a path matches a gitignore pattern."""
        # Convert gitignore pattern to fnmatch pattern
        if pattern.startswith('/'):
            # Absolute path from gitignore location
            pattern = pattern[1:]
            
        # Handle ** wildcards
        pattern = pattern.replace('**/', '*')
        pattern = pattern.replace('/**', '*')
        
        return fnmatch.fnmatch(path_str, pattern)


class PathsGenerator:
    """Automatically generate paths.py based on project structure."""
    
    def __init__(self, project_root: Path = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent
        self.gitignore_parser = GitignoreParser(self.project_root)
        
        # Built-in ignore patterns (in addition to .gitignore)
        self.always_ignore = {
            '__pycache__', '.pytest_cache', '.mypy_cache', '.tox',
            'htmlcov', '.coverage', '.DS_Store', 'Thumbs.db'
        }
        self.ignore_patterns = [
            re.compile(r'.*\.pyc$'),  # Python compiled files
            re.compile(r'.*\.pyo$'),  # Python optimized files
            re.compile(r'.*\.egg-info$'),  # Egg info directories
            re.compile(r'.*\.dist-info$'),  # Distribution info
        ]
        self.tracked_extensions = {
            '.json', '.yaml', '.yml', '.ini', '.cfg', '.conf',
            '.txt', '.csv', '.xlsx', '.db', '.sqlite', '.toml'
        }
        
    def should_ignore(self, path: Path) -> bool:
        """Check if path should be ignored."""
        name = path.name
        
        # Check built-in ignore list
        if name in self.always_ignore:
            return True
            
        # Check built-in patterns
        for pattern in self.ignore_patterns:
            if pattern.match(name):
                return True
        
        # Check gitignore rules
        if self.gitignore_parser.should_ignore(path):
            return True
                
        return False
    
    def determine_output_location(self) -> Path:
        """Determine the best location for paths.py file."""
        # Look for common Python project structures
        possible_locations = []
        
        # Check for src directory
        src_dir = self.project_root / 'src'
        if src_dir.exists() and src_dir.is_dir():
            # Look for main package in src
            for item in src_dir.iterdir():
                if item.is_dir() and not item.name.startswith('.') and not item.name.startswith('_'):
                    config_dir = item / 'config'
                    utils_dir = item / 'utils'
                    possible_locations.extend([
                        (config_dir, 90),  # Highest priority
                        (utils_dir, 80),
                        (item, 70)  # Package root
                    ])
        
        # Check for package in root (common structure)
        for item in self.project_root.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Check if it's likely a package (has __init__.py or looks like one)
                if (item / '__init__.py').exists() or item.name.lower() == self.project_root.name.lower():
                    config_dir = item / 'config'
                    utils_dir = item / 'utils' 
                    common_dir = item / 'common'
                    possible_locations.extend([
                        (config_dir, 85),
                        (utils_dir, 75),
                        (common_dir, 70),
                        (item, 65)
                    ])
        
        # Fallback locations
        possible_locations.extend([
            (self.project_root / 'config', 60),
            (self.project_root / 'utils', 55),
            (self.project_root, 50)  # Last resort
        ])
        
        # Sort by priority and return the first existing or creatable location
        possible_locations.sort(key=lambda x: x[1], reverse=True)
        
        for location, priority in possible_locations:
            # If location exists or we can create it (for config/utils dirs)
            if location.exists() or 'config' in location.name or 'utils' in location.name:
                return location
        
        # Default to project root
        return self.project_root
    
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
        keywords = {'class', 'def', 'import', 'from', 'as', 'is', 'in', 'for', 'if', 'else',
                   'return', 'yield', 'lambda', 'with', 'try', 'except', 'finally', 'raise'}
        if name in keywords:
            name = f"{name}_dir"
            
        return name
    
    def scan_directories(self) -> Dict[str, Path]:
        """Scan project and return all directories."""
        directories = {}
        
        for root, dirs, files in os.walk(self.project_root):
            root_path = Path(root)
            
            # Filter out ignored directories before recursion
            original_dirs = dirs[:]
            dirs[:] = []
            
            for d in original_dirs:
                dir_path = root_path / d
                if not self.should_ignore(dir_path):
                    dirs.append(d)
            
            relative_path = root_path.relative_to(self.project_root)
            
            # Skip if it's the root directory
            if relative_path == Path('.'):
                continue
                
            # Skip if this directory should be ignored
            if self.should_ignore(root_path):
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
            root_path = Path(root)
            
            # Filter out ignored directories
            original_dirs = dirs[:]
            dirs[:] = []
            
            for d in original_dirs:
                dir_path = root_path / d
                if not self.should_ignore(dir_path):
                    dirs.append(d)
            
            for file in files:
                file_path = root_path / file
                
                # Skip if file should be ignored
                if self.should_ignore(file_path):
                    continue
                
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
            base_path: Custom base path. If None, uses the project root directory.
        """
        if base_path is None:
            # Navigate up from this file's location to find project root
            current = Path(__file__).parent.absolute()
            
            # Look for common project root indicators
            while current != current.parent:
                if any((current / indicator).exists() for indicator in 
                       ['.git', 'setup.py', 'pyproject.toml', 'requirements.txt']):
                    self._base = current
                    break
                current = current.parent
            else:
                # Fallback to file's parent directory
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
    
    def ensure_dir_exists(self, directory: str) -> Path:
        """
        Ensure a directory exists, creating it if necessary.
        
        Args:
            directory: Name of the directory property
            
        Returns:
            Path to the directory
        """
        if hasattr(self, directory):
            dir_path = getattr(self, directory)
            dir_path.mkdir(parents=True, exist_ok=True)
            return dir_path
        else:
            raise AttributeError(f"Directory '{directory}' not found")
    
    def __str__(self) -> str:
        """String representation of all paths."""
        paths_info = [f"Project Base: {self._base}"]
        
        # Add all directory paths
        for attr_name in sorted(dir(self)):
            if not attr_name.startswith('_') and attr_name not in [
                'base', 'create_all_directories', 'get_file_in', 'ensure_dir_exists'
            ]:
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

def ensure_path_exists(directory: str) -> Path:
    """
    Ensure a directory path exists.
    
    Args:
        directory: Directory property name
        
    Returns:
        Path object
    """
    return paths.ensure_dir_exists(directory)
'''
        
        return content
    
    def update_paths_file(self, output_file: str = None, include_files: bool = True):
        """Generate and write the paths.py file."""
        # Determine output location if not specified
        if output_file is None:
            output_dir = self.determine_output_location()
            
            # Create the directory if it doesn't exist and it's a config/utils directory
            if not output_dir.exists() and ('config' in output_dir.name or 'utils' in output_dir.name):
                output_dir.mkdir(parents=True, exist_ok=True)
                print(f"Created directory: {output_dir}")
                
                # Create __init__.py if it doesn't exist
                init_file = output_dir / '__init__.py'
                if not init_file.exists():
                    init_file.write_text('"""Configuration module."""\n')
            
            output_path = output_dir / 'paths.py'
        else:
            output_path = self.project_root / output_file
        
        content = self.generate_paths_py(include_files)
        
        # Backup existing file if it exists
        if output_path.exists():
            backup_path = output_path.with_suffix('.py.backup')
            backup_path.write_text(output_path.read_text())
            print(f"Backed up existing file to: {backup_path}")
        
        # Write new content
        output_path.write_text(content)
        print(f"Generated {output_path.relative_to(self.project_root)} with {len(self.scan_directories())} directories")
        if include_files:
            print(f"and {len(self.scan_important_files())} tracked files")
        
        return output_path


def main():
    """Run the paths generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate paths.py from project structure')
    parser.add_argument('--root', type=str, help='Project root directory (default: current directory)')
    parser.add_argument('--no-files', action='store_true', help='Exclude file paths, only include directories')
    parser.add_argument('--output', type=str, help='Output file path (default: auto-determined based on project structure)')
    
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
    
    # Show where the file will be created
    if args.output is None:
        output_location = generator.determine_output_location()
        print(f"\nDetermined output location: {output_location.relative_to(generator.project_root)}/paths.py")
    
    # Generate file
    print(f"\nGenerating paths.py...")
    output_path = generator.update_paths_file(args.output, not args.no_files)
    print(f"Successfully generated: {output_path}")


if __name__ == "__main__":
    main()
