#!/usr/bin/env python3
"""
SQL Server Management Studio (SSMS) Locator for Windows 11
Searches for SSMS installation on local and network drives.
Enhanced with network drive support.
"""

import os
import sys
import time
from pathlib import Path
import subprocess
from typing import List, Optional, Dict, Tuple
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import socket

def print_banner():
    """Print script banner"""
    print("=" * 70)
    print("SQL Server Management Studio (SSMS) Locator - Enhanced Edition")
    print("Now with Network Drive Support for Windows 11")
    print("=" * 70)
    print()

def check_network_connectivity(host: str = "8.8.8.8", port: int = 53, timeout: int = 3) -> bool:
    """Check if network connectivity is available"""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False

def get_drive_info() -> Dict[str, Dict]:
    """Get comprehensive drive information including network drives"""
    print("🔍 Detecting available drives (local and network)...")
    
    drives_info = {}
    
    # Method 1: Use WMIC to get detailed drive information
    try:
        cmd = ['wmic', 'logicaldisk', 'get', 'DeviceID,DriveType,FileSystem,FreeSpace,Size,VolumeName,Description', '/format:csv']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            headers = None
            
            for line in lines:
                if not line.strip():
                    continue
                    
                fields = [f.strip() for f in line.split(',')]
                
                if headers is None:
                    headers = fields
                    continue
                
                if len(fields) >= len(headers) and fields[1]:  # DeviceID exists
                    drive_data = dict(zip(headers, fields))
                    device_id = drive_data.get('DeviceID', '').strip()
                    
                    if device_id and len(device_id) >= 2:
                        drive_type_code = drive_data.get('DriveType', '0')
                        
                        # Drive types: 0=Unknown, 1=No Root, 2=Removable, 3=Local, 4=Network, 5=CD-ROM, 6=RAM
                        drive_types = {
                            '0': 'Unknown',
                            '1': 'No Root Directory',
                            '2': 'Removable',
                            '3': 'Local Fixed',
                            '4': 'Network',
                            '5': 'CD-ROM',
                            '6': 'RAM Disk'
                        }
                        
                        drives_info[device_id] = {
                            'type': drive_types.get(drive_type_code, 'Unknown'),
                            'type_code': int(drive_type_code) if drive_type_code.isdigit() else 0,
                            'filesystem': drive_data.get('FileSystem', '').strip(),
                            'volume_name': drive_data.get('VolumeName', '').strip(),
                            'description': drive_data.get('Description', '').strip(),
                            'free_space': drive_data.get('FreeSpace', '0'),
                            'total_size': drive_data.get('Size', '0'),
                            'accessible': True
                        }
                        
    except Exception as e:
        print(f"⚠️ Warning: Could not get detailed drive info via WMIC: {e}")
    
    # Method 2: Fallback - Basic drive detection
    if not drives_info:
        print("📁 Using fallback drive detection method...")
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            drive = f"{letter}:"
            drive_path = f"{letter}:\\"
            
            if os.path.exists(drive_path):
                drives_info[drive] = {
                    'type': 'Unknown',
                    'type_code': 0,
                    'filesystem': 'Unknown',
                    'volume_name': '',
                    'description': 'Basic Detection',
                    'free_space': '0',
                    'total_size': '0',
                    'accessible': True
                }
    
    # Method 3: Get network drives using net use command
    try:
        result = subprocess.run(['net', 'use'], capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            for line in lines:
                line = line.strip()
                if ':' in line and '\\\\' in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        drive_letter = parts[1] if parts[1].endswith(':') else parts[0]
                        if drive_letter.endswith(':') and len(drive_letter) == 2:
                            if drive_letter not in drives_info:
                                drives_info[drive_letter] = {
                                    'type': 'Network',
                                    'type_code': 4,
                                    'filesystem': 'Network',
                                    'volume_name': '',
                                    'description': 'Network Drive (net use)',
                                    'free_space': '0',
                                    'total_size': '0',
                                    'accessible': True
                                }
                            else:
                                # Update existing entry to mark as network
                                drives_info[drive_letter]['type'] = 'Network'
                                drives_info[drive_letter]['type_code'] = 4
                                
    except Exception as e:
        print(f"⚠️ Warning: Could not enumerate network drives via 'net use': {e}")
    
    # Test accessibility of drives
    for drive, info in drives_info.items():
        drive_path = f"{drive}\\" if not drive.endswith('\\') else drive
        try:
            # Quick accessibility test
            test_result = os.access(drive_path, os.R_OK)
            info['accessible'] = test_result
            if not test_result:
                print(f"⚠️ Drive {drive} detected but not accessible")
        except:
            info['accessible'] = False
    
    return drives_info

def categorize_drives(drives_info: Dict[str, Dict]) -> Tuple[List[str], List[str], List[str]]:
    """Categorize drives into local, network, and other"""
    local_drives = []
    network_drives = []
    other_drives = []
    
    for drive, info in drives_info.items():
        if not info['accessible']:
            continue
            
        drive_path = f"{drive}\\" if not drive.endswith('\\') else drive
        
        if info['type_code'] == 4 or info['type'] == 'Network':
            network_drives.append(drive_path)
        elif info['type_code'] == 3 or info['type'] == 'Local Fixed':
            local_drives.append(drive_path)
        elif info['type_code'] in [2, 5]:  # Removable, CD-ROM
            other_drives.append(drive_path)
        else:
            # Default to local for unknown types if they're accessible
            local_drives.append(drive_path)
    
    return local_drives, network_drives, other_drives

def display_drive_info(drives_info: Dict[str, Dict]):
    """Display detailed drive information"""
    local_drives, network_drives, other_drives = categorize_drives(drives_info)
    
    print("\n📊 Drive Summary:")
    print("-" * 50)
    
    if local_drives:
        print(f"🖥️  Local Drives ({len(local_drives)}): {', '.join(local_drives)}")
    
    if network_drives:
        print(f"🌐 Network Drives ({len(network_drives)}): {', '.join(network_drives)}")
        
        # Show network connectivity status
        if check_network_connectivity():
            print("   ✅ Network connectivity confirmed")
        else:
            print("   ⚠️ Limited network connectivity detected")
    
    if other_drives:
        print(f"💾 Other Drives ({len(other_drives)}): {', '.join(other_drives)}")
    
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

def search_using_where_command(include_network: bool = False) -> List[Path]:
    """Use Windows 'where' command to locate SSMS"""
    print(f"\n🔍 Using Windows 'where' command to locate SSMS...")
    if include_network:
        print("   Including network drives in search...")
    
    found_installations = []
    
    try:
        # Get drives to search
        if include_network:
            drives_info = get_drive_info()
            local_drives, network_drives, _ = categorize_drives(drives_info)
            search_drives = local_drives + network_drives
        else:
            search_drives = ['C:\\']
        
        for drive in search_drives:
            try:
                print(f"   Searching {drive}...")
                result = subprocess.run(
                    ['where', '/R', drive, 'ssms.exe'],
                    capture_output=True,
                    text=True,
                    timeout=120 if drive in network_drives else 60
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
                                
            except subprocess.TimeoutExpired:
                print(f"⚠️ Search timeout on {drive}")
            except Exception as e:
                print(f"⚠️ Error searching {drive}: {e}")
                
    except Exception as e:
        print(f"⚠️ Error using 'where' command: {e}")
    
    if not found_installations:
        print("❌ No SSMS installations found using 'where' command")
    
    return found_installations

def search_drive_worker(drive: str, is_network: bool = False) -> List[Path]:
    """Worker function for searching a single drive"""
    found_installations = []
    searched_dirs = 0
    start_time = time.time()
    timeout = 600 if is_network else 300  # 10 min for network, 5 min for local
    
    try:
        for root, dirs, files in os.walk(drive):
            # Check timeout
            if time.time() - start_time > timeout:
                print(f"⏰ Timeout reached for {drive} after {timeout}s")
                break
                
            searched_dirs += 1
            
            # Skip system directories that are unlikely to contain SSMS
            dirs_to_skip = [
                'Windows', 'System Volume Information', '$Recycle.Bin',
                'Recovery', 'PerfLogs', 'MSOCache', 'Intel', 'AMD',
                'Windows.old', 'ProgramData', 'AppData', 'Temp'
            ]
            
            # Filter out directories to skip
            dirs[:] = [d for d in dirs if d not in dirs_to_skip]
            
            # Look for ssms.exe in current directory
            if 'ssms.exe' in [f.lower() for f in files]:
                ssms_path = Path(root) / 'Ssms.exe'
                if ssms_path.exists():
                    found_installations.append(ssms_path)
                    
    except PermissionError:
        pass  # Silent skip for permission errors
    except Exception as e:
        if is_network:
            pass  # Silent skip for network errors
    
    return found_installations

def search_filesystem_parallel(drives: List[str] = None, include_network: bool = False) -> List[Path]:
    """Perform parallel filesystem search for SSMS"""
    if drives is None:
        drives_info = get_drive_info()
        local_drives, network_drives, _ = categorize_drives(drives_info)
        drives = local_drives
        if include_network:
            drives.extend(network_drives)
    
    print(f"\n🔍 Performing parallel filesystem search on {len(drives)} drive(s)")
    if include_network:
        print("⚠️ Network drives included - this may take significantly longer...")
    print("⚠️ This operation may take several minutes...")
    
    found_installations = []
    
    # Use ThreadPoolExecutor for parallel searching
    max_workers = min(len(drives), 4)  # Limit concurrent operations
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_drive = {}
        
        for drive in drives:
            is_network = any('\\\\' in drive or drive.startswith('\\') for net_drive in drives if net_drive == drive)
            future = executor.submit(search_drive_worker, drive, is_network)
            future_to_drive[future] = drive
        
        for future in as_completed(future_to_drive):
            drive = future_to_drive[future]
            try:
                drive_results = future.result(timeout=900)  # 15 minute timeout per drive
                if drive_results:
                    print(f"✅ Found {len(drive_results)} installation(s) on {drive}")
                    for result in drive_results:
                        print(f"   📁 {result}")
                    found_installations.extend(drive_results)
                else:
                    print(f"❌ No installations found on {drive}")
            except Exception as e:
                print(f"⚠️ Error searching {drive}: {e}")
    
    return found_installations

def search_filesystem_legacy(drives: List[str] = None, include_network: bool = False) -> List[Path]:
    """Legacy sequential filesystem search (fallback)"""
    if drives is None:
        drives_info = get_drive_info()
        local_drives, network_drives, _ = categorize_drives(drives_info)
        drives = local_drives
        if include_network:
            drives.extend(network_drives)
    
    print(f"\n🔍 Performing sequential filesystem search on drives: {', '.join(drives)}")
    print("⚠️ This may take several minutes...")
    
    found_installations = []
    
    for drive in drives:
        print(f"\n📁 Searching drive: {drive}")
        drive_results = search_drive_worker(drive, '\\\\' in drive)
        found_installations.extend(drive_results)
    
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
        print("   • Network drives are disconnected or inaccessible")
        print("\n🔗 Download SSMS from: https://docs.microsoft.com/sql/ssms/download-sql-server-management-studio-ssms")
        return
    
    print(f"\n✅ Found {len(installations)} SSMS installation(s):")
    print("=" * 70)
    
    for i, ssms_path in enumerate(installations, 1):
        # Determine if this is on a network drive
        drive_letter = ssms_path.parts[0] if ssms_path.parts else ""
        is_network = '\\\\' in str(ssms_path) or any(char in str(ssms_path) for char in ['$', '@'])
        
        print(f"\n{i}. {ssms_path}")
        if is_network:
            print(f"   🌐 Location: Network Drive")
        else:
            print(f"   🖥️ Location: Local Drive ({drive_letter})")
        
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
    
    # Get drive information
    drives_info = get_drive_info()
    display_drive_info(drives_info)
    
    local_drives, network_drives, other_drives = categorize_drives(drives_info)
    
    all_installations = []
    
    # Step 1: Check common locations
    common_installations = check_common_locations()
    all_installations.extend(common_installations)
    
    # Step 2: Use Windows 'where' command if nothing found in common locations
    if not all_installations:
        include_network_where = False
        if network_drives:
            response = input(f"\n🌐 Include network drives in 'where' command search? (y/N): ")
            include_network_where = response.lower() in ['y', 'yes']
        
        where_installations = search_using_where_command(include_network_where)
        all_installations.extend(where_installations)
    
    # Step 3: Deep filesystem search if still nothing found
    if not all_installations:
        print("\n❓ SSMS not found using quick search methods.")
        
        # Ask about deep search
        response = input("Perform deep filesystem search? This may take several minutes (y/N): ")
        
        if response.lower() in ['y', 'yes']:
            all_drives = local_drives + other_drives
            selected_drives = all_drives.copy()
            include_network_deep = False
            
            # Network drive options
            if network_drives:
                print(f"\n🌐 Network drives detected: {', '.join(network_drives)}")
                net_response = input("Include network drives in deep search? (y/N): ")
                include_network_deep = net_response.lower() in ['y', 'yes']
                if include_network_deep:
                    selected_drives.extend(network_drives)
            
            # Drive selection
            print(f"\n📁 Available drives for search: {', '.join(selected_drives)}")
            drive_input = input(f"Enter drives to search (comma-separated) or press Enter for all: ")
            
            if drive_input.strip():
                input_drives = [d.strip().upper() for d in drive_input.split(',')]
                # Ensure drives end with backslash
                input_drives = [d if d.endswith('\\') else d + '\\' for d in input_drives]
                selected_drives = [d for d in input_drives if d in selected_drives]
            
            print(f"\n🎯 Selected drives: {', '.join(selected_drives)}")
            
            # Choose search method
            if len(selected_drives) > 1:
                search_method = input("Use parallel search for faster results? (Y/n): ")
                use_parallel = search_method.lower() not in ['n', 'no']
            else:
                use_parallel = False
            
            # Perform search
            if use_parallel:
                filesystem_installations = search_filesystem_parallel(selected_drives, include_network_deep)
            else:
                filesystem_installations = search_filesystem_legacy(selected_drives, include_network_deep)
                
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
    
    print(f"\n🏁 Search completed. Thank you for using SSMS Locator Enhanced!")

if __name__ == "__main__":
    main()