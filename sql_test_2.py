#!/usr/bin/env python3
"""
SQL Server Connection Script - Minimum Viable Product
Connects to SQL Server using Windows Authentication
"""

import pyodbc
import sys

def connect_to_sql_server(server_name, database_name=None):
    """
    Connect to SQL Server using Windows Authentication
    
    Args:
        server_name (str): SQL Server instance name (e.g., 'localhost', 'DESKTOP-123\\SQLEXPRESS')
        database_name (str, optional): Database name to connect to
    
    Returns:
        pyodbc.Connection: Database connection object or None if failed
    """
    
    try:
        # Build connection string for Windows Authentication
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};Trusted_Connection=yes;"
        
        if database_name:
            connection_string += f"DATABASE={database_name};"
        
        print(f"🔗 Connecting to SQL Server: {server_name}")
        if database_name:
            print(f"📊 Database: {database_name}")
        
        # Establish connection
        connection = pyodbc.connect(connection_string)
        print("✅ Connected successfully!")
        return connection
        
    except pyodbc.Error as e:
        print(f"❌ Connection failed: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def execute_test_query(connection):
    """
    Execute a simple test query to verify connection
    
    Args:
        connection: pyodbc connection object
    """
    
    try:
        cursor = connection.cursor()
        
        # Test query - get SQL Server version and current database
        query = """
        SELECT 
            @@VERSION as SqlServerVersion,
            DB_NAME() as CurrentDatabase,
            SYSTEM_USER as CurrentUser,
            GETDATE() as CurrentDateTime
        """
        
        print("\n🔍 Executing test query...")
        cursor.execute(query)
        
        # Fetch results
        row = cursor.fetchone()
        
        if row:
            print("\n📋 Connection Details:")
            print(f"   SQL Server Version: {row.SqlServerVersion.split()[3]} {row.SqlServerVersion.split()[4]}")
            print(f"   Current Database: {row.CurrentDatabase}")
            print(f"   Current User: {row.CurrentUser}")
            print(f"   Server Time: {row.CurrentDateTime}")
        
        cursor.close()
        
    except pyodbc.Error as e:
        print(f"❌ Query execution failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error during query: {e}")

def list_databases(connection):
    """
    List all databases on the SQL Server instance
    
    Args:
        connection: pyodbc connection object
    """
    
    try:
        cursor = connection.cursor()
        
        # Query to get all databases
        query = """
        SELECT 
            name as DatabaseName,
            database_id as DatabaseID,
            create_date as CreatedDate
        FROM sys.databases 
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        ORDER BY name
        """
        
        print("\n📚 Available User Databases:")
        cursor.execute(query)
        
        databases = cursor.fetchall()
        
        if databases:
            for db in databases:
                print(f"   • {db.DatabaseName} (ID: {db.DatabaseID}, Created: {db.CreatedDate.strftime('%Y-%m-%d')})")
        else:
            print("   No user databases found")
        
        cursor.close()
        
    except pyodbc.Error as e:
        print(f"❌ Failed to list databases: {e}")
    except Exception as e:
        print(f"❌ Unexpected error listing databases: {e}")

def main():
    """
    Main function - MVP SQL Server connection demo
    """
    
    print("=" * 50)
    print("SQL Server Windows Auth Connector - MVP")
    print("=" * 50)
    
    # Configuration - Update these values for your environment
    SERVER_NAME = "localhost"  # Change to your SQL Server instance
    DATABASE_NAME = None       # Optional: specify a database name
    
    # You can also try these common server names:
    # "localhost\\SQLEXPRESS"     # For SQL Server Express
    # "DESKTOP-YOURPC\\SQLEXPRESS" # For named instance
    # "."                         # Local default instance
    # "127.0.0.1"                # Local IP
    
    print(f"🎯 Target Server: {SERVER_NAME}")
    
    # Connect to SQL Server
    connection = connect_to_sql_server(SERVER_NAME, DATABASE_NAME)
    
    if connection:
        try:
            # Execute test query
            execute_test_query(connection)
            
            # List available databases
            list_databases(connection)
            
            print(f"\n✅ Connection test completed successfully!")
            
        except Exception as e:
            print(f"❌ Error during testing: {e}")
        
        finally:
            # Clean up
            try:
                connection.close()
                print("🔌 Connection closed")
            except:
                pass
    
    else:
        print("\n💡 Troubleshooting Tips:")
        print("   1. Ensure SQL Server is running")
        print("   2. Check if Windows Authentication is enabled")
        print("   3. Verify your Windows user has SQL Server access")
        print("   4. Try different server names (localhost, .\\SQLEXPRESS, etc.)")
        print("   5. Install ODBC Driver 17 for SQL Server if not present")
        sys.exit(1)

if __name__ == "__main__":
    # Check if pyodbc is installed
    try:
        import pyodbc
    except ImportError:
        print("❌ pyodbc library not found!")
        print("📦 Install it using: pip install pyodbc")
        sys.exit(1)
    
    main()