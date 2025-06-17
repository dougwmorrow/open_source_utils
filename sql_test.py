#!/usr/bin/env python3
"""
libtdsodbc.so Library Finder for Red Hat Linux Server
Searches the entire filesystem for FreeTDS ODBC driver library
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from typing import List, Optional

def print_banner():
    """Print script banner"""
    print("=" * 60)
    print("libtdsodbc.so Library Finder - Red Hat Linux")
    print("=" * 60)
    print("🔍 Searching for FreeTDS ODBC driver library")
    print()

def check_system_info():
    """Display system information"""
    try:
        # Get OS information
        with open('/etc/redhat-release', 'r') as f:
            os_info = f.read().strip()
        print(f"🖥️  System: {os_info}")
    except:
        print("🖥️  System: Linux (Red Hat compatible)")
    
    try:
        # Get architecture
        arch = subprocess.run(['uname', '-m'], capture_output=True, text=True)
        if arch.returncode == 0:
            print(f"🏗️  Architecture: {arch.stdout.strip()}")
    except:
        pass
    
    print()

def check_common_locations() -> List[Path]:
    """Check common library locations first"""
    print("🔍 Checking common library locations...")
    
    common_paths = [
        # Standard library directories
        "/usr/lib64/libtdsodbc.so",
        "/usr/lib/libtdsodbc.so",
        "/usr/lib/x86_64-linux-gnu/libtdsodbc.so",
        "/lib64/libtdsodbc.so",
        "/lib/libtdsodbc.so",
        
        # FreeTDS specific locations
        "/usr/lib64/freetds/libtdsodbc.so",
        "/usr/lib/freetds/libtdsodbc.so",
        "/usr/local/lib64/libtdsodbc.so",
        "/usr/local/lib/libtdsodbc.so",
        "/usr/local/freetds/lib/libtdsodbc.so",
        
        # ODBC specific locations
        "/usr/lib64/odbc/libtdsodbc.so",
        "/usr/lib/odbc/libtdsodbc.so",
        "/usr/local/lib/odbc/libtdsodbc.so",
        
        # Alternative package manager locations
        "/opt/freetds/lib/libtdsodbc.so",
        "/opt/mssql-tools/lib/libtdsodbc.so",
        
        # Versioned variants
        "/usr/lib64/libtdsodbc.so.0",
        "/usr/lib/libtdsodbc.so.0",
        "/usr/lib64/libtdsodbc.so.1",
        "/usr/lib/libtdsodbc.so.1",
    ]
    
    found_libraries = []
    
    for path_str in common_paths:
        path = Path(path_str)
        if path.exists() and path.is_file():
            print(f"✅ Found: {path}")
            found_libraries.append(path)
        else:
            print(f"❌ Not found: {path}")
    
    return found_libraries

def search_using_find_command() -> List[Path]:
    """Use Linux 'find' command to locate libtdsodbc.so"""
    print("\n🔍 Using 'find' command to search filesystem...")
    
    found_libraries = []
    
    try:
        # Search for exact filename
        result = subprocess.run(
            ['find', '/', '-name', 'libtdsodbc.so', '-type', 'f', '2>/dev/null'],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            shell=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            paths = result.stdout.strip().split('\n')
            for path_str in paths:
                path_str = path_str.strip()
                if path_str:
                    path = Path(path_str)
                    if path.exists():
                        print(f"✅ Found: {path}")
                        found_libraries.append(path)
        
        # Also search for versioned variants
        print("\n🔍 Searching for versioned variants...")
        result2 = subprocess.run(
            ['find', '/', '-name', 'libtdsodbc.so.*', '-type', 'f', '2>/dev/null'],
            capture_output=True,
            text=True,
            timeout=180,
            shell=True
        )
        
        if result2.returncode == 0 and result2.stdout.strip():
            paths = result2.stdout.strip().split('\n')
            for path_str in paths:
                path_str = path_str.strip()
                if path_str:
                    path = Path(path_str)
                    if path.exists():
                        print(f"✅ Found versioned: {path}")
                        found_libraries.append(path)
        
        if not found_libraries:
            print("❌ No libtdsodbc.so files found using 'find' command")
            
    except subprocess.TimeoutExpired:
        print("⚠️ Search timeout - 'find' command took too long")
    except Exception as e:
        print(f"⚠️ Error using 'find' command: {e}")
    
    return found_libraries

def search_using_locate() -> List[Path]:
    """Use 'locate' command if available"""
    print("\n🔍 Trying 'locate' command...")
    
    found_libraries = []
    
    try:
        # Update locate database first (may require sudo)
        print("📊 Updating locate database...")
        try:
            subprocess.run(['updatedb'], check=False, capture_output=True, timeout=60)
        except:
            print("⚠️ Could not update locate database (may need sudo)")
        
        # Search using locate
        result = subprocess.run(
            ['locate', 'libtdsodbc.so'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0 and result.stdout.strip():
            paths = result.stdout.strip().split('\n')
            for path_str in paths:
                path_str = path_str.strip()
                if path_str:
                    path = Path(path_str)
                    if path.exists() and path.is_file():
                        print(f"✅ Found: {path}")
                        found_libraries.append(path)
        else:
            print("❌ No results from 'locate' command")
            
    except FileNotFoundError:
        print("⚠️ 'locate' command not available")
    except Exception as e:
        print(f"⚠️ Error using 'locate' command: {e}")
    
    return found_libraries

def manual_filesystem_search(mount_points: List[str] = None) -> List[Path]:
    """Perform manual filesystem search"""
    if mount_points is None:
        mount_points = ['/']
    
    print(f"\n🔍 Manual filesystem search on: {', '.join(mount_points)}")
    print("⚠️ This may take several minutes...")
    
    found_libraries = []
    searched_dirs = 0
    start_time = time.time()
    
    for mount_point in mount_points:
        print(f"\n📁 Searching: {mount_point}")
        
        try:
            for root, dirs, files in os.walk(mount_point):
                searched_dirs += 1
                
                # Print progress every 2000 directories
                if searched_dirs % 2000 == 0:
                    elapsed = time.time() - start_time
                    print(f"⏳ Searched {searched_dirs} directories... ({elapsed:.1f}s)")
                
                # Skip certain directories to speed up search
                dirs_to_skip = [
                    'proc', 'sys', 'dev', 'run', 'tmp', 'var/cache',
                    'var/log', 'var/tmp', '.git', '__pycache__', 'node_modules'
                ]
                
                # Filter out directories to skip
                dirs[:] = [d for d in dirs if d not in dirs_to_skip and not d.startswith('.')]
                
                # Look for libtdsodbc.so files
                for file in files:
                    if file == 'libtdsodbc.so' or file.startswith('libtdsodbc.so.'):
                        lib_path = Path(root) / file
                        if lib_path.exists() and lib_path.is_file():
                            print(f"✅ Found: {lib_path}")
                            found_libraries.append(lib_path)
                
        except PermissionError:
            print(f"⚠️ Permission denied accessing some directories in {mount_point}")
        except Exception as e:
            print(f"⚠️ Error searching {mount_point}: {e}")
    
    elapsed = time.time() - start_time
    print(f"\n📊 Manual search completed in {elapsed:.1f} seconds")
    print(f"📊 Searched {searched_dirs} directories")
    
    return found_libraries

def get_library_info(lib_path: Path) -> dict:
    """Get detailed information about the library file"""
    info = {}
    
    try:
        # File size
        stat = lib_path.stat()
        info['size'] = f"{stat.st_size / 1024:.1f} KB"
        info['modified'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stat.st_mtime))
        info['permissions'] = oct(stat.st_mode)[-3:]
        
        # Try to get version information using strings command
        try:
            result = subprocess.run(
                ['strings', str(lib_path)],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'freetds' in line.lower() or 'version' in line.lower():
                        info['version_info'] = line.strip()[:100]
                        break
        except:
            pass
        
        # Check if it's a symbolic link
        if lib_path.is_symlink():
            info['symlink_target'] = str(lib_path.resolve())
        
    except Exception as e:
        info['error'] = str(e)
    
    return info

def display_results(libraries: List[Path]):
    """Display found libraries with detailed information"""
    if not libraries:
        print("\n❌ libtdsodbc.so library not found!")
        print("\n💡 Installation suggestions:")
        print("   Red Hat/CentOS/RHEL 7/8:")
        print("     sudo yum install freetds freetds-devel")
        print("   Red Hat/CentOS/RHEL 9+ / Fedora:")
        print("     sudo dnf install freetds freetds-devel")
        print("   From source:")
        print("     wget http://www.freetds.org/files/stable/freetds-X.X.tar.gz")
        print("     tar -xzf freetds-X.X.tar.gz && cd freetds-X.X")
        print("     ./configure --with-unixodbc && make && sudo make install")
        return
    
    print(f"\n✅ Found {len(libraries)} libtdsodbc.so library/libraries:")
    print("=" * 60)
    
    for i, lib_path in enumerate(libraries, 1):
        print(f"\n{i}. {lib_path}")
        
        info = get_library_info(lib_path)
        
        if 'size' in info:
            print(f"   📏 Size: {info['size']}")
        if 'modified' in info:
            print(f"   📅 Modified: {info['modified']}")
        if 'permissions' in info:
            print(f"   🔒 Permissions: {info['permissions']}")
        if 'symlink_target' in info:
            print(f"   🔗 Symlink to: {info['symlink_target']}")
        if 'version_info' in info:
            print(f"   📋 Version info: {info['version_info']}")
        if 'error' in info:
            print(f"   ⚠️ Info error: {info['error']}")

def get_mount_points() -> List[str]:
    """Get available mount points for search"""
    mount_points = ['/']
    
    try:
        result = subprocess.run(['mount'], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            mounts = []
            for line in lines:
                if ' on ' in line and ' type ' in line:
                    parts = line.split(' on ')
                    if len(parts) > 1:
                        mount_point = parts[1].split(' type ')[0]
                        if mount_point.startswith('/') and mount_point not in ['/', '/boot', '/proc', '/sys', '/dev']:
                            mounts.append(mount_point)
            
            if mounts:
                print(f"📁 Additional mount points found: {', '.join(mounts)}")
                mount_points.extend(mounts)
    except:
        pass
    
    return mount_points

def main():
    """Main function"""
    print_banner()
    check_system_info()
    
    # Check if running on Linux
    if sys.platform != 'linux':
        print("❌ This script is designed for Linux systems only!")
        sys.exit(1)
    
    all_libraries = []
    
    # Step 1: Check common locations
    common_libraries = check_common_locations()
    all_libraries.extend(common_libraries)
    
    # Step 2: Use 'find' command
    if not all_libraries:
        find_libraries = search_using_find_command()
        all_libraries.extend(find_libraries)
    
    # Step 3: Try 'locate' command
    if not all_libraries:
        locate_libraries = search_using_locate()
        all_libraries.extend(locate_libraries)
    
    # Step 4: Manual filesystem search if still nothing found
    if not all_libraries:
        print("\n❓ libtdsodbc.so not found using quick methods.")
        response = input("Perform manual filesystem search? This may take 10+ minutes (y/N): ")
        
        if response.lower() in ['y', 'yes']:
            mount_points = get_mount_points()
            manual_libraries = manual_filesystem_search(mount_points)
            all_libraries.extend(manual_libraries)
    
    # Remove duplicates while preserving order
    unique_libraries = []
    seen = set()
    for lib in all_libraries:
        if lib not in seen:
            unique_libraries.append(lib)
            seen.add(lib)
    
    # Display results
    display_results(unique_libraries)
    
    # Show configuration suggestions if libraries found
    if unique_libraries:
        print(f"\n🔧 Configuration suggestions:")
        print("   1. Check ODBC configuration: /etc/odbcinst.ini")
        print("   2. Verify FreeTDS configuration: /etc/freetds/freetds.conf")
        print("   3. Test ODBC connection: isql -v")
        print("   4. Set LD_LIBRARY_PATH if needed:")
        for lib in unique_libraries:
            print(f"      export LD_LIBRARY_PATH={lib.parent}:$LD_LIBRARY_PATH")

if __name__ == "__main__":
    main()