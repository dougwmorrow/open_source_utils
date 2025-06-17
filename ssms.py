#!/usr/bin/env python3
"""
SQL Server Management Studio (SSMS) Locator for Windows 11
Searches for SSMS installation on the system.
"""

import os
import sys
import time
from pathlib import Path
import subprocess
from typing import List, Optional

def print_banner():
    """Print script banner"""
    print("=" * 60)
    print("SQL Server Management Studio (SSMS) Locator")
    print("=" * 60)
    print()

def check_common_locations() -> List[Path]:
    """Check common SSMS installation locations first"""
    print("🔍 Checking common SSMS installation locations...")
    
    common_paths = [
        # SSMS 18.x locations
        r"C:\Program Files (x86)\Microsoft SQL Server Management Studio 18\Common7\IDE\Ssms.exe",
        r"C:\Program Files\Microsoft SQL Server Management Studio 18\Common7\IDE\Ssms.exe",
        
        # SSMS 19.x locations  
        r"C:\Program Files (x86)\Microsoft SQL Server Management Studio 19\Common7\IDE\Ssms.exe",
        r"C:\Program Files\Microsoft SQL Server Management Studio 19\Common7\IDE\Ssms.exe",
        
        # SSMS 20.x locations
        r"C:\Program Files (x86)\Microsoft SQL Server Management Studio 20\Common7\IDE\Ssms.exe",
        r"C:\Program Files\Microsoft SQL Server Management Studio 20\Common7\IDE\Ssms.exe",
        
        # Legacy locations (older versions)
        r"C:\Program Files (x86)\Microsoft SQL Server\140\Tools\Binn\ManagementStudio\Ssms.exe",
        r"C:\Program Files\Microsoft SQL Server\140\Tools\Binn\ManagementStudio\Ssms.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\130\Tools\Binn\ManagementStudio\Ssms.exe",
        r"C:\Program Files\Microsoft SQL Server\130\Tools\Binn\ManagementStudio\Ssms.exe",
    ]
    
    found_installations = []
    
    for path_str in common_paths:
        path = Path(path_str)
        if path.exists() and path.is_file():
            print(f"✅ Found: {path}")
            found_installations.append(path)
        else:
            print(f"❌ Not found: {path}")
    
    return found_installations

def search_using_where_command() -> List[Path]:
    """Use Windows 'where' command to locate SSMS"""
    print("\n🔍 Using Windows 'where' command to locate SSMS...")
    
    found_installations = []
    
    try:
        # Search for ssms.exe using where command
        result = subprocess.run(
            ['where', '/R', 'C:\\', 'ssms.exe'],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0 and result.stdout.strip():
            paths = result.stdout.strip().split('\n')
            for path_str in paths:
                path_str = path_str.strip()
                if path_str:
                    path = Path(path_str)
                    if path.exists() and path.is_file():
                        print(f"✅ Found: {path}")
                        found_installations.append(path)
        else:
            print("❌ No SSMS installations found using 'where' command")
            
    except subprocess.TimeoutExpired:
        print("⚠️ Search timeout - Windows 'where' command took too long")
    except Exception as e:
        print(f"⚠️ Error using 'where' command: {e}")
    
    return found_installations

def search_filesystem(drives: List[str] = None) -> List[Path]:
    """Perform deep filesystem search for SSMS"""
    if drives is None:
        drives = ['C:\\']
    
    print(f"\n🔍 Performing deep filesystem search on drives: {', '.join(drives)}")
    print("⚠️ This may take several minutes...")
    
    found_installations = []
    searched_dirs = 0
    start_time = time.time()
    
    for drive in drives:
        print(f"\n📁 Searching drive: {drive}")
        
        try:
            for root, dirs, files in os.walk(drive):
                searched_dirs += 1
                
                # Print progress every 1000 directories
                if searched_dirs % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(f"⏳ Searched {searched_dirs} directories... ({elapsed:.1f}s)")
                
                # Skip system directories that are unlikely to contain SSMS
                dirs_to_skip = [
                    'Windows', 'System Volume Information', '$Recycle.Bin',
                    'Recovery', 'PerfLogs', 'MSOCache', 'Intel', 'AMD'
                ]
                
                # Filter out directories to skip
                dirs[:] = [d for d in dirs if d not in dirs_to_skip]
                
                # Look for ssms.exe in current directory
                if 'ssms.exe' in [f.lower() for f in files]:
                    ssms_path = Path(root) / 'Ssms.exe'
                    if ssms_path.exists():
                        print(f"✅ Found: {ssms_path}")
                        found_installations.append(ssms_path)
                
        except PermissionError:
            print(f"⚠️ Permission denied accessing some directories in {drive}")
        except Exception as e:
            print(f"⚠️ Error searching {drive}: {e}")
    
    elapsed = time.time() - start_time
    print(f"\n📊 Search completed in {elapsed:.1f} seconds")
    print(f"📊 Searched {searched_dirs} directories")
    
    return found_installations

def get_ssms_version(ssms_path: Path) -> Optional[str]:
    """Get SSMS version information"""
    try:
        result = subprocess.run(
            [str(ssms_path), '/?'],
            capture_output=True,
            text=True,
            timeout=10
        )
        # Parse version from output if available
        # This might not work for all versions, so we'll also try file properties
        return "Version info available via file properties"
    except:
        pass
    
    try:
        # Try to get file version using PowerShell
        ps_command = f'(Get-ItemProperty "{ssms_path}").VersionInfo.FileVersion'
        result = subprocess.run(
            ['powershell', '-Command', ps_command],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except:
        pass
    
    return "Unknown"

def display_results(installations: List[Path]):
    """Display found SSMS installations with details"""
    if not installations:
        print("\n❌ No SQL Server Management Studio installations found!")
        print("\n💡 Possible reasons:")
        print("   • SSMS is not installed")
        print("   • SSMS is installed in a non-standard location")
        print("   • Insufficient permissions to access installation directory")
        print("\n🔗 Download SSMS from: https://docs.microsoft.com/sql/ssms/download-sql-server-management-studio-ssms")
        return
    
    print(f"\n✅ Found {len(installations)} SSMS installation(s):")
    print("=" * 60)
    
    for i, ssms_path in enumerate(installations, 1):
        print(f"\n{i}. {ssms_path}")
        
        # Get file size
        try:
            size_mb = ssms_path.stat().st_size / (1024 * 1024)
            print(f"   📏 Size: {size_mb:.1f} MB")
        except:
            print(f"   📏 Size: Unknown")
        
        # Get version
        version = get_ssms_version(ssms_path)
        print(f"   📋 Version: {version}")
        
        # Get last modified
        try:
            mtime = ssms_path.stat().st_mtime
            mtime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
            print(f"   📅 Modified: {mtime_str}")
        except:
            print(f"   📅 Modified: Unknown")

def main():
    """Main function"""
    print_banner()
    
    # Check if running on Windows
    if sys.platform != 'win32':
        print("❌ This script is designed for Windows systems only!")
        sys.exit(1)
    
    all_installations = []
    
    # Step 1: Check common locations
    common_installations = check_common_locations()
    all_installations.extend(common_installations)
    
    # Step 2: Use Windows 'where' command if nothing found in common locations
    if not all_installations:
        where_installations = search_using_where_command()
        all_installations.extend(where_installations)
    
    # Step 3: Deep filesystem search if still nothing found
    if not all_installations:
        print("\n❓ SSMS not found in common locations.")
        response = input("Perform deep filesystem search? This may take several minutes (y/N): ")
        
        if response.lower() in ['y', 'yes']:
            # Get available drives
            drives = []
            for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                drive = f"{letter}:\\"
                if os.path.exists(drive):
                    drives.append(drive)
            
            print(f"📁 Available drives: {', '.join(drives)}")
            
            # Allow user to select drives
            drive_input = input(f"Enter drives to search (comma-separated) or press Enter for all [{', '.join(drives)}]: ")
            if drive_input.strip():
                selected_drives = [d.strip().upper() + ':\\' for d in drive_input.split(',')]
                drives = [d for d in selected_drives if d in drives]
            
            filesystem_installations = search_filesystem(drives)
            all_installations.extend(filesystem_installations)
    
    # Remove duplicates while preserving order
    unique_installations = []
    seen = set()
    for installation in all_installations:
        if installation not in seen:
            unique_installations.append(installation)
            seen.add(installation)
    
    # Display results
    display_results(unique_installations)
    
    # Offer to launch SSMS
    if unique_installations:
        print(f"\n🚀 Launch SSMS?")
        if len(unique_installations) == 1:
            response = input("Launch SSMS? (y/N): ")
            if response.lower() in ['y', 'yes']:
                try:
                    subprocess.Popen([str(unique_installations[0])])
                    print("✅ SSMS launched successfully!")
                except Exception as e:
                    print(f"❌ Error launching SSMS: {e}")
        else:
            print("Multiple installations found. Select one to launch:")
            for i, path in enumerate(unique_installations, 1):
                print(f"  {i}. {path}")
            
            try:
                choice = input("Enter choice (1-{}) or 'N' to skip: ".format(len(unique_installations)))
                if choice.isdigit() and 1 <= int(choice) <= len(unique_installations):
                    selected_path = unique_installations[int(choice) - 1]
                    subprocess.Popen([str(selected_path)])
                    print("✅ SSMS launched successfully!")
            except Exception as e:
                print(f"❌ Error launching SSMS: {e}")

if __name__ == "__main__":
    main()