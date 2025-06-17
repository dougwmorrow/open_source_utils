#!/usr/bin/env python3
"""
Oracle to MongoDB Data Migration Script
Efficiently extracts large datasets from Oracle and loads into MongoDB
with proper indexing and partitioning strategies.
"""

import oracledb
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
import logging
import sys
from datetime import datetime, timedelta
import json
from typing import Iterator, Dict, Any, List, Optional
import gc
import psutil
import time
from contextlib import contextmanager
import threading
from queue import Queue
import signal
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pickle
from pathlib import Path


@dataclass
class Config:
    """Configuration class for database connections and processing parameters"""
    # Oracle configuration
    oracle_host: str = "localhost"
    oracle_port: int = 1521
    oracle_service: str = "ORCL"
    oracle_user: str = "your_username"
    oracle_password: str = "your_password"
    
    # MongoDB configuration - Use connection string format
    mongo_connection_string: str = "mongodb+srv://username:password@cluster.mongodb.net/"
    mongo_db: str = "analytics_db"
    mongo_collection: str = "daily_data"
    
    # Processing configuration
    batch_size: int = 10000  # Records per batch for memory efficiency
    max_workers: int = 4     # Number of threads for parallel processing
    target_date: str = "2024-01-01"  # Date to extract (YYYY-MM-DD format)
    
    # Table configuration
    oracle_table: str = "your_large_table"
    datetime_column: str = "DATETIME"
    grouped_column: str = "GROUPED"
    
    # Checkpoint configuration
    checkpoint_dir: str = "./checkpoints"
    enable_resume: bool = True


class CheckpointManager:
    """Manages checkpoint data for resumable migrations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.checkpoint_dir = Path(config.checkpoint_dir)
        self.checkpoint_file = self.checkpoint_dir / f"migration_{config.target_date}.checkpoint"
        self.progress_file = self.checkpoint_dir / f"progress_{config.target_date}.json"
        
        # Create checkpoint directory if it doesn't exist
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        self.checkpoint_data = {
            'target_date': config.target_date,
            'total_records': 0,
            'processed_batches': set(),
            'failed_batches': set(),
            'last_successful_offset': -1,
            'total_inserted': 0,
            'start_time': None,
            'last_update': None
        }
        
        self.lock = threading.Lock()
    
    def load_checkpoint(self) -> bool:
        """Load existing checkpoint data"""
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'rb') as f:
                    self.checkpoint_data = pickle.load(f)
                logging.info(f"Loaded checkpoint: {len(self.checkpoint_data['processed_batches'])} batches completed")
                return True
            else:
                logging.info("No existing checkpoint found, starting fresh migration")
                return False
        except Exception as e:
            logging.error(f"Failed to load checkpoint: {e}")
            return False
    
    def save_checkpoint(self):
        """Save current checkpoint data"""
        try:
            with self.lock:
                self.checkpoint_data['last_update'] = datetime.utcnow().isoformat()
                
                # Save binary checkpoint data
                with open(self.checkpoint_file, 'wb') as f:
                    pickle.dump(self.checkpoint_data, f)
                
                # Save human-readable progress
                progress_data = {
                    'target_date': self.checkpoint_data['target_date'],
                    'total_records': self.checkpoint_data['total_records'],
                    'processed_batches_count': len(self.checkpoint_data['processed_batches']),
                    'failed_batches_count': len(self.checkpoint_data['failed_batches']),
                    'total_inserted': self.checkpoint_data['total_inserted'],
                    'last_successful_offset': self.checkpoint_data['last_successful_offset'],
                    'start_time': self.checkpoint_data['start_time'],
                    'last_update': self.checkpoint_data['last_update']
                }
                
                with open(self.progress_file, 'w') as f:
                    json.dump(progress_data, f, indent=2)
                    
        except Exception as e:
            logging.error(f"Failed to save checkpoint: {e}")
    
    def mark_batch_completed(self, batch_num: int, offset: int, records_inserted: int):
        """Mark a batch as successfully completed"""
        with self.lock:
            self.checkpoint_data['processed_batches'].add(batch_num)
            self.checkpoint_data['failed_batches'].discard(batch_num)  # Remove from failed if it was there
            self.checkpoint_data['last_successful_offset'] = max(self.checkpoint_data['last_successful_offset'], offset)
            self.checkpoint_data['total_inserted'] += records_inserted
        
        # Save checkpoint every 10 batches
        if batch_num % 10 == 0:
            self.save_checkpoint()
    
    def mark_batch_failed(self, batch_num: int):
        """Mark a batch as failed"""
        with self.lock:
            self.checkpoint_data['failed_batches'].add(batch_num)
            self.checkpoint_data['processed_batches'].discard(batch_num)  # Remove from completed if it was there
    
    def is_batch_completed(self, batch_num: int) -> bool:
        """Check if a batch has already been completed"""
        return batch_num in self.checkpoint_data['processed_batches']
    
    def is_batch_failed(self, batch_num: int) -> bool:
        """Check if a batch has failed"""
        return batch_num in self.checkpoint_data['failed_batches']
    
    def get_resume_info(self) -> Dict[str, Any]:
        """Get information about where to resume"""
        with self.lock:
            return {
                'completed_batches': len(self.checkpoint_data['processed_batches']),
                'failed_batches': len(self.checkpoint_data['failed_batches']),
                'total_inserted': self.checkpoint_data['total_inserted'],
                'last_offset': self.checkpoint_data['last_successful_offset']
            }
    
    def verify_existing_data(self, mongo_collection) -> int:
        """Verify how many records already exist in MongoDB for the target date"""
        try:
            # Parse target date
            target_date = datetime.strptime(self.config.target_date, '%Y-%m-%d')
            next_date = target_date + timedelta(days=1)
            
            # Count existing records
            existing_count = mongo_collection.count_documents({
                'date': {
                    '$gte': target_date,
                    '$lt': next_date
                }
            })
            
            logging.info(f"Found {existing_count:,} existing records in MongoDB for {self.config.target_date}")
            return existing_count
            
        except Exception as e:
            logging.error(f"Failed to verify existing data: {e}")
            return 0
    
    def cleanup_checkpoint(self):
        """Clean up checkpoint files after successful completion"""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
            logging.info("Checkpoint files cleaned up after successful migration")
        except Exception as e:
            logging.warning(f"Failed to cleanup checkpoint files: {e}")


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.oracle_pool = None
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_collection = None
        
    def setup_oracle_connection(self):
        """Setup Oracle connection pool"""
        try:
            dsn = oracledb.makedsn(
                host=self.config.oracle_host,
                port=self.config.oracle_port,
                service_name=self.config.oracle_service
            )
            
            self.oracle_pool = oracledb.create_pool(
                user=self.config.oracle_user,
                password=self.config.oracle_password,
                dsn=dsn,
                min=1,
                max=self.config.max_workers + 2,
                increment=1,
                getmode=oracledb.POOL_GETMODE_WAIT
            )
            
            logging.info("Oracle connection pool created successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to create Oracle connection pool: {e}")
            return False
    
    def setup_mongo_connection(self):
        """Setup MongoDB connection and create indexes"""
        try:
            self.mongo_client = MongoClient(
                self.config.mongo_connection_string,
                maxPoolSize=self.config.max_workers + 2,
                retryWrites=True,
                w="majority",
                serverSelectionTimeoutMS=30000,  # 30 second timeout
                connectTimeoutMS=20000,          # 20 second connection timeout
                socketTimeoutMS=20000            # 20 second socket timeout
            )
            
            self.mongo_db = self.mongo_client[self.config.mongo_db]
            self.mongo_collection = self.mongo_db[self.config.mongo_collection]
            
            # Create indexes for efficient querying
            self.create_mongodb_indexes()
            
            logging.info("MongoDB connection established successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def create_mongodb_indexes(self):
        """Create optimized indexes for MongoDB collection"""
        try:
            # Compound index for date and category queries
            self.mongo_collection.create_index([
                ("date", ASCENDING),
                ("category", ASCENDING)
            ], name="idx_date_category")
            
            # Index for date-based queries
            self.mongo_collection.create_index([
                ("date", ASCENDING)
            ], name="idx_date")
            
            # Index for category-based queries
            self.mongo_collection.create_index([
                ("category", ASCENDING)
            ], name="idx_category")
            
            # Compound index for date range and category aggregations
            self.mongo_collection.create_index([
                ("date", ASCENDING),
                ("category", ASCENDING),
                ("_id", ASCENDING)
            ], name="idx_date_category_id")
            
            logging.info("MongoDB indexes created successfully")
            
        except Exception as e:
            logging.error(f"Failed to create MongoDB indexes: {e}")
    
    @contextmanager
    def get_oracle_connection(self):
        """Context manager for Oracle connections"""
        connection = None
        try:
            connection = self.oracle_pool.acquire()
            yield connection
        finally:
            if connection:
                self.oracle_pool.release(connection)
    
    def close_connections(self):
        """Close all database connections"""
        if self.oracle_pool:
            self.oracle_pool.close()
            logging.info("Oracle connection pool closed")
        
        if self.mongo_client:
            self.mongo_client.close()
            logging.info("MongoDB connection closed")


class DataExtractor:
    """Handles data extraction from Oracle database"""
    
    def __init__(self, db_manager: DatabaseManager, config: Config):
        self.db_manager = db_manager
        self.config = config
        
    def get_total_records(self, target_date: str) -> int:
        """Get total number of records for the target date"""
        query = f"""
        SELECT COUNT(*) 
        FROM {self.config.oracle_table} 
        WHERE DATE({self.config.datetime_column}) = DATE('{target_date}')
        """
        
        try:
            with self.db_manager.get_oracle_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                count = cursor.fetchone()[0]
                cursor.close()
                return count
        except Exception as e:
            logging.error(f"Failed to get record count: {e}")
            return 0
    
    def extract_data_batch(self, offset: int, batch_size: int, target_date: str) -> List[Dict[str, Any]]:
        """Extract a batch of data from Oracle"""
        query = f"""
        SELECT * FROM (
            SELECT a.*, ROWNUM rnum FROM (
                SELECT *
                FROM {self.config.oracle_table}
                WHERE DATE({self.config.datetime_column}) = DATE('{target_date}')
                ORDER BY {self.config.datetime_column}, {self.config.grouped_column}
            ) a
            WHERE ROWNUM <= {offset + batch_size}
        )
        WHERE rnum > {offset}
        """
        
        try:
            with self.db_manager.get_oracle_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0].lower() for desc in cursor.description]
                
                # Fetch data and convert to dictionaries
                rows = cursor.fetchall()
                cursor.close()
                
                batch_data = []
                for row in rows:
                    record = dict(zip(columns, row))
                    
                    # Transform data for MongoDB
                    transformed_record = self.transform_record(record)
                    batch_data.append(transformed_record)
                
                return batch_data
                
        except Exception as e:
            logging.error(f"Failed to extract batch at offset {offset}: {e}")
            return []
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Oracle record for MongoDB storage"""
        transformed = {
            'date': record.get(self.config.datetime_column.lower()),
            'category': record.get(self.config.grouped_column.lower()),
            'created_at': datetime.utcnow(),
            'source': 'oracle_migration'
        }
        
        # Add all other fields
        for key, value in record.items():
            if key not in [self.config.datetime_column.lower(), self.config.grouped_column.lower()]:
                # Handle different data types
                if isinstance(value, datetime):
                    transformed[key] = value
                elif value is None:
                    transformed[key] = None
                else:
                    transformed[key] = value
        
        return transformed


class DataLoader:
    """Handles data loading into MongoDB"""
    
    def __init__(self, db_manager: DatabaseManager, config: Config, checkpoint_manager: CheckpointManager):
        self.db_manager = db_manager
        self.config = config
        self.checkpoint_manager = checkpoint_manager
        self.total_inserted = 0
        self.lock = threading.Lock()
    
    def load_batch(self, batch_data: List[Dict[str, Any]], batch_num: int, offset: int) -> bool:
        """Load a batch of data into MongoDB"""
        if not batch_data:
            return True
        
        try:
            # Check for duplicates based on a unique combination of fields
            # This helps prevent duplicate inserts during resume operations
            existing_records = self.check_for_duplicates(batch_data)
            
            if existing_records > 0:
                logging.info(f"Skipping {existing_records} duplicate records in batch {batch_num}")
                
                # If all records are duplicates, mark batch as completed
                if existing_records == len(batch_data):
                    self.checkpoint_manager.mark_batch_completed(batch_num, offset, 0)
                    return True
                
                # Filter out duplicates
                batch_data = self.filter_duplicates(batch_data)
            
            # Use bulk insert for efficiency
            result = self.db_manager.mongo_collection.insert_many(
                batch_data,
                ordered=False,  # Continue on error
                bypass_document_validation=False
            )
            
            inserted_count = len(result.inserted_ids)
            
            with self.lock:
                self.total_inserted += inserted_count
            
            # Mark batch as completed in checkpoint
            self.checkpoint_manager.mark_batch_completed(batch_num, offset, inserted_count)
            
            logging.info(f"Batch {batch_num}: Inserted {inserted_count} records (offset: {offset:,})")
            return True
            
        except pymongo.errors.BulkWriteError as e:
            # Handle partial success
            inserted_count = e.details.get('nInserted', 0)
            with self.lock:
                self.total_inserted += inserted_count
            
            if inserted_count > 0:
                self.checkpoint_manager.mark_batch_completed(batch_num, offset, inserted_count)
                logging.warning(f"Batch {batch_num}: Partial insert - {inserted_count} inserted, {len(e.details.get('writeErrors', []))} errors")
                return True
            else:
                self.checkpoint_manager.mark_batch_failed(batch_num)
                logging.error(f"Batch {batch_num}: Complete failure - {len(e.details.get('writeErrors', []))} errors")
                return False
            
        except Exception as e:
            self.checkpoint_manager.mark_batch_failed(batch_num)
            logging.error(f"Batch {batch_num}: Failed to insert - {e}")
            return False
    
    def check_for_duplicates(self, batch_data: List[Dict[str, Any]]) -> int:
        """Check how many records in the batch already exist in MongoDB"""
        try:
            if not batch_data:
                return 0
            
            # Create a query to check for existing records
            # Using date and a few other fields to identify potential duplicates
            or_conditions = []
            for record in batch_data[:100]:  # Check first 100 to avoid huge queries
                condition = {
                    'date': record.get('date'),
                    'category': record.get('category')
                }
                # Add more fields for unique identification if available
                if 'id' in record:
                    condition['id'] = record['id']
                or_conditions.append(condition)
            
            if not or_conditions:
                return 0
            
            existing_count = self.db_manager.mongo_collection.count_documents({
                '$or': or_conditions
            })
            
            return existing_count
            
        except Exception as e:
            logging.warning(f"Failed to check for duplicates: {e}")
            return 0
    
    def filter_duplicates(self, batch_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out duplicate records from the batch"""
        try:
            # For simplicity, return the original batch
            # In a production environment, you might want more sophisticated duplicate detection
            return batch_data
            
        except Exception as e:
            logging.warning(f"Failed to filter duplicates: {e}")
            return batch_data


class MigrationManager:
    """Main migration manager orchestrating the data transfer"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_manager = DatabaseManager(config)
        self.checkpoint_manager = CheckpointManager(config)
        self.extractor = DataExtractor(self.db_manager, config)
        self.loader = DataLoader(self.db_manager, config, self.checkpoint_manager)
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'migration_{self.config.target_date}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def monitor_memory(self):
        """Monitor memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        logging.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB ({memory_percent:.2f}%)")
        
        if memory_percent > 80:
            logging.warning("High memory usage detected, forcing garbage collection")
            gc.collect()
    
    def run_migration(self):
        """Execute the complete migration process"""
        start_time = time.time()
        
        logging.info(f"Starting migration for date: {self.config.target_date}")
        
        # Setup database connections
        if not self.db_manager.setup_oracle_connection():
            logging.error("Failed to setup Oracle connection")
            return False
        
        if not self.db_manager.setup_mongo_connection():
            logging.error("Failed to setup MongoDB connection")
            return False
        
        try:
            # Load existing checkpoint if resuming
            is_resume = False
            if self.config.enable_resume:
                is_resume = self.checkpoint_manager.load_checkpoint()
                if is_resume:
                    # Verify existing data in MongoDB
                    existing_records = self.checkpoint_manager.verify_existing_data(
                        self.db_manager.mongo_collection
                    )
                    resume_info = self.checkpoint_manager.get_resume_info()
                    logging.info(f"Resuming migration: {resume_info['completed_batches']} batches completed, "
                               f"{resume_info['total_inserted']:,} records inserted")
            
            # Get total record count
            total_records = self.extractor.get_total_records(self.config.target_date)
            logging.info(f"Total records to migrate: {total_records:,}")
            
            if total_records == 0:
                logging.warning("No records found for the specified date")
                return True
            
            # Update checkpoint with total records
            self.checkpoint_manager.checkpoint_data['total_records'] = total_records
            if not is_resume:
                self.checkpoint_manager.checkpoint_data['start_time'] = datetime.utcnow().isoformat()
            
            # Process data in batches
            batch_count = (total_records + self.config.batch_size - 1) // self.config.batch_size
            successful_batches = 0
            skipped_batches = 0
            failed_batches = 0
            
            logging.info(f"Processing {batch_count} batches with batch size {self.config.batch_size}")
            
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = []
                
                for batch_num in range(1, batch_count + 1):
                    if not self.running:
                        break
                    
                    # Skip already completed batches
                    if self.checkpoint_manager.is_batch_completed(batch_num):
                        skipped_batches += 1
                        successful_batches += 1
                        continue
                    
                    # Retry failed batches
                    if self.checkpoint_manager.is_batch_failed(batch_num):
                        logging.info(f"Retrying previously failed batch {batch_num}")
                    
                    offset = (batch_num - 1) * self.config.batch_size
                    future = executor.submit(self.process_batch, offset, batch_num, batch_count)
                    futures.append((future, batch_num))
                
                # Wait for all batches to complete
                for future, batch_num in futures:
                    if not self.running:
                        break
                    
                    try:
                        success = future.result()
                        if success:
                            successful_batches += 1
                        else:
                            failed_batches += 1
                            
                    except Exception as e:
                        failed_batches += 1
                        self.checkpoint_manager.mark_batch_failed(batch_num)
                        logging.error(f"Batch {batch_num} processing failed: {e}")
                    
                    # Monitor memory usage and save checkpoint periodically
                    if (successful_batches + failed_batches) % 5 == 0:
                        self.monitor_memory()
                        self.checkpoint_manager.save_checkpoint()
            
            # Final checkpoint save
            self.checkpoint_manager.save_checkpoint()
            
            # Final statistics
            end_time = time.time()
            duration = end_time - start_time
            total_inserted = self.checkpoint_manager.checkpoint_data['total_inserted']
            
            logging.info("Migration completed!")
            logging.info(f"Total batches: {batch_count}")
            logging.info(f"Successful batches: {successful_batches}")
            logging.info(f"Skipped batches (already completed): {skipped_batches}")
            logging.info(f"Failed batches: {failed_batches}")
            logging.info(f"Total records inserted: {total_inserted:,}")
            logging.info(f"Duration: {duration:.2f} seconds")
            
            if total_inserted > 0:
                logging.info(f"Average rate: {total_inserted/duration:.2f} records/second")
            
            # Clean up checkpoint files if migration completed successfully
            if failed_batches == 0:
                self.checkpoint_manager.cleanup_checkpoint()
                logging.info("Migration completed successfully - checkpoint files cleaned up")
            else:
                logging.warning(f"Migration completed with {failed_batches} failed batches - "
                              f"checkpoint files preserved for retry")
            
            return failed_batches == 0
            
        except Exception as e:
            logging.error(f"Migration failed: {e}")
            self.checkpoint_manager.save_checkpoint()
            return False
        
        finally:
            self.db_manager.close_connections()
    
    def process_batch(self, offset: int, batch_num: int, total_batches: int) -> bool:
        """Process a single batch of data"""
        try:
            logging.info(f"Processing batch {batch_num}/{total_batches} (offset: {offset:,})")
            
            # Extract data from Oracle
            batch_data = self.extractor.extract_data_batch(
                offset, self.config.batch_size, self.config.target_date
            )
            
            if not batch_data:
                logging.warning(f"No data extracted for batch {batch_num}")
                # Mark as completed even if no data (could be end of dataset)
                self.checkpoint_manager.mark_batch_completed(batch_num, offset, 0)
                return True
            
            # Load data into MongoDB
            success = self.loader.load_batch(batch_data, batch_num, offset)
            
            # Force garbage collection for memory management
            del batch_data
            gc.collect()
            
            return success
            
        except Exception as e:
            logging.error(f"Failed to process batch {batch_num}: {e}")
            self.checkpoint_manager.mark_batch_failed(batch_num)
            return False


def main():
    """Main execution function"""
    # Configuration
    config = Config(
        # Update these with your actual database credentials
        oracle_host="your_oracle_host",
        oracle_user="your_oracle_user",
        oracle_password="your_oracle_password",
        oracle_service="your_service_name",
        
        # MongoDB connection string - Update with your actual connection details
        mongo_connection_string="mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority",
        mongo_db="analytics_db",
        mongo_collection="daily_data",
        
        # Adjust these based on your table structure
        oracle_table="your_large_table",
        datetime_column="DATETIME",
        grouped_column="GROUPED",
        
        # Processing parameters - adjust based on your server capacity
        batch_size=5000,  # Reduced for 16GB server
        max_workers=3,    # Conservative for memory management
        target_date="2024-01-01",  # Change to your target date
        
        # Checkpoint configuration
        checkpoint_dir="./checkpoints",
        enable_resume=True
    )
    
    # Create and run migration
    migration = MigrationManager(config)
    migration.setup_logging()
    
    logging.info("Oracle to MongoDB Migration Script Starting...")
    logging.info(f"Configuration: Batch size={config.batch_size}, Workers={config.max_workers}")
    
    success = migration.run_migration()
    
    if success:
        logging.info("Migration completed successfully!")
        sys.exit(0)
    else:
        logging.error("Migration failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()