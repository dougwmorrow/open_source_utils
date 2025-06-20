#!/usr/bin/env python3
"""
Oracle to Data Lake ETL Script
Extracts data from Oracle database and stores in partitioned parquet format
"""

import os
import sys
import logging
import configparser
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import gc

import cx_Oracle
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oracle_etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class OracleDataLakeETL:
    """ETL pipeline for Oracle to Data Lake extraction"""
    
    def __init__(self, config_file: str = 'config.ini'):
        """Initialize ETL with configuration"""
        self.config = self._load_config(config_file)
        self.base_path = Path(self.config['storage']['base_path'])
        self.chunk_size = int(self.config['processing']['chunk_size'])
        self.connection_pool = None
        
    def _load_config(self, config_file: str) -> Dict[str, Dict[str, Any]]:
        """Load configuration from INI file"""
        if not os.path.exists(config_file):
            self._create_default_config(config_file)
            logger.info(f"Created default config file: {config_file}")
            logger.info("Please update the configuration and run again.")
            sys.exit(0)
            
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    
    def _create_default_config(self, config_file: str):
        """Create default configuration file"""
        config = configparser.ConfigParser()
        
        config['oracle'] = {
            'host': 'oracle_host',
            'port': '1521',
            'service_name': 'service_name',
            'username': 'username',
            'password': 'password',
            'encoding': 'UTF-8'
        }
        
        config['storage'] = {
            'base_path': '/data-lake',
            'file_format': 'parquet',
            'compression': 'gzip'
        }
        
        config['processing'] = {
            'chunk_size': '50000',
            'array_size': '10000',
            'prefetch_rows': '10000'
        }
        
        config['extraction'] = {
            'table_name': 'your_table_name',
            'datetime_column': 'DATETIME',
            'category_column': 'CATEGORY',
            'additional_columns': 'col1,col2,col3'
        }
        
        with open(config_file, 'w') as f:
            config.write(f)
    
    def _init_connection_pool(self):
        """Initialize Oracle connection pool"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config['oracle']['host'],
                self.config['oracle']['port'],
                service_name=self.config['oracle']['service_name']
            )
            
            self.connection_pool = cx_Oracle.SessionPool(
                user=self.config['oracle']['username'],
                password=self.config['oracle']['password'],
                dsn=dsn,
                min=2,
                max=5,
                increment=1,
                encoding=self.config['oracle']['encoding']
            )
            logger.info("Oracle connection pool initialized successfully")
        except cx_Oracle.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def _get_connection(self):
        """Get connection from pool"""
        if not self.connection_pool:
            self._init_connection_pool()
        return self.connection_pool.acquire()
    
    def _create_directory_structure(self, category: str, date: datetime) -> Path:
        """Create data lake directory structure"""
        dir_path = self.base_path / 'raw' / category / f'year={date.year}' / \
                   f'month={date.month:02d}' / f'day={date.day:02d}'
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path
    
    def _generate_filename(self, timestamp: datetime) -> str:
        """Generate filename with timestamp"""
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        return f"{timestamp_str}_data.parquet.gz"
    
    def _build_query(self, start_date: datetime, end_date: datetime) -> str:
        """Build extraction query with date range"""
        table_name = self.config['extraction']['table_name']
        datetime_col = self.config['extraction']['datetime_column']
        category_col = self.config['extraction']['category_column']
        
        # Get additional columns
        additional_cols = self.config['extraction'].get('additional_columns', '')
        if additional_cols:
            columns = f"{datetime_col}, {category_col}, {additional_cols}"
        else:
            columns = f"{datetime_col}, {category_col}, *"
        
        query = f"""
        SELECT {columns}
        FROM {table_name}
        WHERE {datetime_col} >= :start_date
          AND {datetime_col} < :end_date
        ORDER BY {datetime_col}, {category_col}
        """
        return query
    
    def _process_chunk(self, df_chunk: pd.DataFrame, processed_categories: Dict[str, Path]):
        """Process a chunk of data and write to appropriate files"""
        datetime_col = self.config['extraction']['datetime_column']
        category_col = self.config['extraction']['category_column']
        
        # Group by date and category
        for (date, category), group_df in df_chunk.groupby([
            pd.Grouper(key=datetime_col, freq='D'), category_col
        ]):
            # Create directory structure
            dir_path = self._create_directory_structure(category, date)
            
            # Generate filename
            key = f"{category}_{date.strftime('%Y%m%d')}"
            if key not in processed_categories:
                filename = self._generate_filename(date)
                file_path = dir_path / filename
                processed_categories[key] = file_path
            else:
                file_path = processed_categories[key]
            
            # Convert to Arrow Table for efficient writing
            table = pa.Table.from_pandas(group_df, preserve_index=False)
            
            # Write or append to parquet file
            if file_path.exists():
                # Read existing data and append
                existing_table = pq.read_table(file_path)
                combined_table = pa.concat_tables([existing_table, table])
                pq.write_table(
                    combined_table,
                    file_path,
                    compression='gzip',
                    use_dictionary=True,
                    compression_level=6
                )
            else:
                pq.write_table(
                    table,
                    file_path,
                    compression='gzip',
                    use_dictionary=True,
                    compression_level=6
                )
    
    def extract_date_range(self, start_date: datetime, end_date: datetime):
        """Extract data for a date range"""
        logger.info(f"Starting extraction from {start_date} to {end_date}")
        
        connection = None
        cursor = None
        processed_categories = {}
        total_rows = 0
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Set array size for better performance
            cursor.arraysize = int(self.config['processing']['array_size'])
            cursor.prefetchrows = int(self.config['processing']['prefetch_rows'])
            
            # Build and execute query
            query = self._build_query(start_date, end_date)
            logger.info(f"Executing query for date range: {start_date} to {end_date}")
            
            cursor.execute(query, start_date=start_date, end_date=end_date)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Process in chunks
            with tqdm(desc="Processing rows", unit="rows") as pbar:
                while True:
                    rows = cursor.fetchmany(self.chunk_size)
                    if not rows:
                        break
                    
                    # Convert to DataFrame
                    df_chunk = pd.DataFrame(rows, columns=columns)
                    
                    # Ensure datetime column is datetime type
                    datetime_col = self.config['extraction']['datetime_column']
                    df_chunk[datetime_col] = pd.to_datetime(df_chunk[datetime_col])
                    
                    # Process chunk
                    self._process_chunk(df_chunk, processed_categories)
                    
                    # Update progress
                    rows_processed = len(rows)
                    total_rows += rows_processed
                    pbar.update(rows_processed)
                    
                    # Force garbage collection to free memory
                    del df_chunk
                    gc.collect()
            
            logger.info(f"Successfully processed {total_rows:,} rows")
            
        except cx_Oracle.Error as e:
            logger.error(f"Oracle error: {e}")
            raise
        except Exception as e:
            logger.error(f"Processing error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.connection_pool.release(connection)
    
    def extract_daily_batch(self, num_days: int = 1, start_date: Optional[datetime] = None):
        """Extract data for specified number of days"""
        if start_date is None:
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"Starting daily batch extraction for {num_days} day(s)")
        
        for day_offset in range(num_days):
            current_date = start_date - timedelta(days=day_offset)
            next_date = current_date + timedelta(days=1)
            
            logger.info(f"Processing day: {current_date.strftime('%Y-%m-%d')}")
            
            try:
                self.extract_date_range(current_date, next_date)
                logger.info(f"Completed processing for {current_date.strftime('%Y-%m-%d')}")
            except Exception as e:
                logger.error(f"Failed to process {current_date.strftime('%Y-%m-%d')}: {e}")
                continue
    
    def extract_historical_data(self, start_date: datetime, end_date: datetime, 
                              batch_days: int = 7):
        """Extract historical data in batches"""
        logger.info(f"Starting historical extraction from {start_date} to {end_date}")
        
        current_date = start_date
        while current_date < end_date:
            batch_end = min(current_date + timedelta(days=batch_days), end_date)
            
            logger.info(f"Processing batch: {current_date} to {batch_end}")
            
            try:
                self.extract_date_range(current_date, batch_end)
            except Exception as e:
                logger.error(f"Failed to process batch: {e}")
                # Continue with next batch
            
            current_date = batch_end
            
            # Force garbage collection between batches
            gc.collect()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.connection_pool:
            self.connection_pool.close()
            logger.info("Connection pool closed")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Oracle to Data Lake ETL')
    parser.add_argument('--config', default='config.ini', help='Configuration file path')
    parser.add_argument('--mode', choices=['daily', 'historical', 'range'], 
                       default='daily', help='Extraction mode')
    parser.add_argument('--days', type=int, default=1, 
                       help='Number of days to extract (for daily mode)')
    parser.add_argument('--start-date', type=str, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, 
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--batch-days', type=int, default=7,
                       help='Batch size in days for historical mode')
    
    args = parser.parse_args()
    
    # Initialize ETL
    etl = OracleDataLakeETL(args.config)
    
    try:
        if args.mode == 'daily':
            start_date = None
            if args.start_date:
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
            etl.extract_daily_batch(args.days, start_date)
            
        elif args.mode == 'historical':
            if not args.start_date or not args.end_date:
                logger.error("Historical mode requires --start-date and --end-date")
                sys.exit(1)
            
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
            etl.extract_historical_data(start_date, end_date, args.batch_days)
            
        elif args.mode == 'range':
            if not args.start_date or not args.end_date:
                logger.error("Range mode requires --start-date and --end-date")
                sys.exit(1)
            
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
            etl.extract_date_range(start_date, end_date)
            
    except KeyboardInterrupt:
        logger.info("ETL interrupted by user")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        etl.cleanup()

if __name__ == "__main__":
    main()
