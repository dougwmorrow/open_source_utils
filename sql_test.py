#!/usr/bin/env python3
"""
Optimized Oracle to Parquet ETL Script for Billion-Row JOIN Operations
Enhanced with Oracle Parallel Processing capabilities
Designed for 16GB RAM constraint with streaming and memory management
Modified to read category values from a parquet file instead of database
Uses Oracle SID connection instead of DSN
"""

import oracledb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging
import sys
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set
import gc
import time
import psutil
import tempfile
import shutil
from queue import Queue
from threading import Thread
import signal
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oracle_etl_optimized.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class OptimizedOracleJoinETL:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize optimized ETL processor with memory-aware settings
        """
        self.config = config
        
        # Increased chunk size for parallel processing
        self.chunk_size = config.get('chunk_size', 50000)  # Increased from 10k
        self.max_workers = min(config.get('max_workers', 2), 2)  # Keep limited for memory
        
        self.data_lake_path = config['data_lake_path']
        self.output_table_name = config.get('output_table_name', 'joined_data')
        self.checkpoint_dir = config.get('checkpoint_dir', './etl_checkpoints')
        self.temp_dir = config.get('temp_dir', '/tmp/etl_temp')
        
        # Category parquet file path
        self.categories_parquet_path = config.get('categories_parquet_path')
        if not self.categories_parquet_path:
            raise ValueError("categories_parquet_path must be specified in config")
        
        # Query configuration
        self.date_column = config.get('date_column')
        self.partition_column = config.get('partition_column')
        self.excluded_categories = set(config.get('excluded_categories', []))
        self.table2_columns = config.get('table2_columns', [])
        
        # Date range
        self.start_date = config.get('start_date')
        self.end_date = config.get('end_date')
        
        # Enhanced performance options
        self.parallel_degree = config.get('parallel_degree', 8)  # Increased default
        self.parallel_degree_range = config.get('parallel_degree_range', (4, 16))
        self.memory_limit_percent = config.get('memory_limit_percent', 70)
        self.enable_parallel_monitoring = config.get('enable_parallel_monitoring', True)
        
        # Initialize Oracle connection pool using SID
        # Create DSN from host, port, and SID
        oracle_host = config['oracle_host']
        oracle_port = config['oracle_port']
        oracle_sid = config['oracle_sid']
        
        # Create DSN string for SID connection
        dsn = oracledb.makedsn(oracle_host, oracle_port, sid=oracle_sid)
        
        # Alternative: Direct connection string format for SID
        # dsn = f"{oracle_host}:{oracle_port}/{oracle_sid}"
        
        logger.info(f"Connecting to Oracle: {oracle_host}:{oracle_port}/{oracle_sid}")
        
        self.pool = oracledb.create_pool(
            user=config['oracle_user'],
            password=config['oracle_password'],
            dsn=dsn,
            min=1,
            max=self.max_workers + 1,
            increment=1,
            threaded=True,
            getmode=oracledb.POOL_GETMODE_WAIT
        )
        
        # Initialize Oracle client if needed
        if 'oracle_lib_dir' in config:
            oracledb.init_oracle_client(lib_dir=config['oracle_lib_dir'])
        
        # Create directories
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Category optimization: Load categories from parquet file
        self.valid_categories = None
        self._initialize_categories_from_parquet()
        
        # Initialize parallel monitoring
        self.parallel_stats = {
            'queries_executed': 0,
            'parallel_executions': 0,
            'avg_parallel_degree': 0,
            'max_parallel_degree': 0
        }
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown = False
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Shutdown signal received, finishing current batch...")
        self.shutdown = True
    
    def _initialize_categories_from_parquet(self):
        """Load valid categories from parquet file instead of database"""
        try:
            # Check if parquet file exists
            if not os.path.exists(self.categories_parquet_path):
                raise FileNotFoundError(f"Categories parquet file not found: {self.categories_parquet_path}")
            
            logger.info(f"Loading categories from parquet file: {self.categories_parquet_path}")
            
            # Read the parquet file
            df = pd.read_parquet(self.categories_parquet_path)
            
            # Check if the expected column exists
            expected_column = '999_distinct values'  # Note the space in the column name
            if expected_column not in df.columns:
                # Try alternative column names
                possible_columns = ['999_distinct_values', '999_categories', 'categories']
                found_column = None
                for col in possible_columns:
                    if col in df.columns:
                        found_column = col
                        logger.warning(f"Column '{expected_column}' not found, using '{found_column}' instead")
                        break
                
                if not found_column:
                    raise ValueError(f"Column '{expected_column}' not found in parquet file. Available columns: {list(df.columns)}")
                expected_column = found_column
            
            # Get all unique category values from the parquet file
            all_categories = set(df[expected_column].dropna().unique())
            logger.info(f"Found {len(all_categories)} total categories in parquet file")
            
            # Exclude specified categories
            if self.excluded_categories:
                self.valid_categories = all_categories - self.excluded_categories
                logger.info(f"Excluding {len(self.excluded_categories)} categories: {self.excluded_categories}")
            else:
                self.valid_categories = all_categories
            
            logger.info(f"Using {len(self.valid_categories)} valid categories out of {len(all_categories)} total")
            
            # Log a sample of categories for verification (first 5)
            sample_categories = list(self.valid_categories)[:5]
            logger.info(f"Sample categories: {sample_categories}")
            
        except Exception as e:
            logger.error(f"Error loading categories from parquet file: {str(e)}")
            raise
    
    def monitor_parallel_execution(self, connection):
        """Monitor Oracle parallel execution statistics"""
        if not self.enable_parallel_monitoring:
            return
        
        try:
            cursor = connection.cursor()
            
            # Check current parallel execution
            cursor.execute("""
                SELECT COUNT(*) as active_servers
                FROM v$px_process 
                WHERE status = 'IN USE'
            """)
            active_servers = cursor.fetchone()[0]
            
            # Get session parallel info
            cursor.execute("""
                SELECT 
                    s.sid,
                    s.serial#,
                    px.req_degree,
                    px.degree,
                    COUNT(DISTINCT p.server_name) as actual_servers
                FROM v$session s
                JOIN v$px_session px ON s.sid = px.sid
                LEFT JOIN v$px_process p ON px.sid = p.sid
                WHERE s.username = :username
                  AND s.status = 'ACTIVE'
                GROUP BY s.sid, s.serial#, px.req_degree, px.degree
            """, username=self.config['oracle_user'].upper())
            
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    sid, serial, req_degree, degree, actual_servers = row
                    logger.debug(f"Session {sid},{serial}: Requested={req_degree}, Granted={degree}, Active={actual_servers}")
                    
                    if degree and degree > 0:
                        self.parallel_stats['parallel_executions'] += 1
                        self.parallel_stats['max_parallel_degree'] = max(
                            self.parallel_stats['max_parallel_degree'], degree
                        )
            
            cursor.close()
            
            if active_servers > 0:
                logger.info(f"Parallel execution active: {active_servers} servers in use")
                
        except Exception as e:
            logger.debug(f"Parallel monitoring error: {str(e)}")
    
    def check_memory(self) -> bool:
        """Check if memory usage is within limits"""
        mem = psutil.virtual_memory()
        if mem.percent > self.memory_limit_percent:
            logger.warning(f"Memory usage at {mem.percent}%, triggering garbage collection")
            gc.collect()
            time.sleep(2)
            
            # Check again after GC
            mem = psutil.virtual_memory()
            if mem.percent > 85:
                logger.error(f"Memory critical at {mem.percent}%")
                return False
        return True
    
    def build_optimized_query(self, date_range: Optional[Tuple[datetime, datetime]] = None,
                            last_id: Optional[int] = None,
                            partition_value: Optional[str] = None,
                            use_parallel_pipelined: bool = False) -> Tuple[str, Dict]:
        """
        Build memory-optimized query with enhanced parallel processing
        """
        params = {}
        
        # Enhanced hints for parallel processing
        parallel_hints = [
            f"PARALLEL(tb1, {self.parallel_degree})",
            f"PARALLEL(tb2, {self.parallel_degree})",
            "USE_HASH(tb1 tb2)",
            "PQ_DISTRIBUTE(tb2 HASH HASH)",
            "NO_PARALLEL_INDEX(tb1)",  # Force full table scan for parallel
            "NO_PARALLEL_INDEX(tb2)"
        ]
        
        # Add statement-level parallel hint
        hints = f"/*+ {' '.join(parallel_hints)} */"
        
        # Build column list: all from table_1, specific 17 from table_2
        # Handle id column overlap by aliasing
        table2_column_list = ', '.join([f'tb2."{col}" as "tb2_{col}"' if col == 'id' else f'tb2."{col}"' 
                                       for col in self.table2_columns])
        
        # For parallel pipelined table function (if available in your Oracle version)
        if use_parallel_pipelined and hasattr(self, 'pipelined_function_name'):
            query = f"""
            SELECT {hints} *
            FROM TABLE({self.pipelined_function_name}(
                CURSOR(
                    SELECT tb1.*, {table2_column_list}
                    FROM table_1 tb1
                    JOIN table_2 tb2 ON tb1.id = tb2.id
                    WHERE 1=1
            """
        else:
            # Standard parallel query
            query = f"""
            SELECT {hints}
                tb1.*,
                {table2_column_list}
            FROM table_1 tb1
            JOIN table_2 tb2 ON tb1.id = tb2.id
            WHERE 1=1
            """
        
        # Category filter - use IN for small sets
        if len(self.valid_categories) < 30:
            query += f' AND tb2."999_categories" IN ({",".join([f":cat_{i}" for i in range(len(self.valid_categories))])})'
            for i, cat in enumerate(self.valid_categories):
                params[f'cat_{i}'] = cat
        elif self.excluded_categories:
            placeholders = ','.join([f':cat_{i}' for i in range(len(self.excluded_categories))])
            query += f' AND tb2."999_categories" NOT IN ({placeholders})'
            for i, cat in enumerate(self.excluded_categories):
                params[f'cat_{i}'] = cat
        
        # Add date filter
        if date_range and date_range[0] is not None and self.date_column:
            query += f" AND {self.date_column} >= :start_date AND {self.date_column} < :end_date"
            params['start_date'] = date_range[0]
            params['end_date'] = date_range[1]
        
        # Add partition filter
        if partition_value is not None and self.partition_column:
            query += f" AND {self.partition_column} = :partition_value"
            params['partition_value'] = partition_value
        
        # Use stable ID column for reliability
        if last_id is not None:
            query += " AND tb1.id > :last_id"
            params['last_id'] = last_id
        
        # Close pipelined function if used
        if use_parallel_pipelined and hasattr(self, 'pipelined_function_name'):
            query += "))) parallel_data"
        
        query += " ORDER BY tb1.id"
        
        # Use FETCH FIRST for Oracle 12c+ (more efficient than ROWNUM)
        query = f"""
        SELECT * FROM (
            {query}
            FETCH FIRST :chunk_size ROWS ONLY
        )
        """
        params['chunk_size'] = self.chunk_size
        
        return query, params
    
    def enable_parallel_session(self, connection):
        """Enable parallel execution for the session"""
        try:
            cursor = connection.cursor()
            
            # Enable parallel DML if needed
            cursor.execute("ALTER SESSION ENABLE PARALLEL DML")
            
            # Force parallel execution
            cursor.execute(f"ALTER SESSION FORCE PARALLEL QUERY PARALLEL {self.parallel_degree}")
            
            # Set parallel degree policy
            cursor.execute("ALTER SESSION SET parallel_degree_policy = 'MANUAL'")
            
            # Increase parallel execution memory
            cursor.execute("ALTER SESSION SET pga_aggregate_target = 2G")
            
            cursor.close()
            logger.info(f"Enabled parallel execution with degree {self.parallel_degree}")
            
        except Exception as e:
            logger.warning(f"Could not set all parallel options: {str(e)}")
    
    def stream_extract_to_parquet(self, query: str, params: Dict, 
                                 output_path: str) -> Tuple[int, int]:
        """
        Stream data directly from Oracle to Parquet with parallel execution monitoring
        """
        connection = self.pool.acquire()
        temp_file = None
        rows_written = 0
        last_id = None
        
        try:
            # Enable parallel execution for this session
            self.enable_parallel_session(connection)
            
            cursor = connection.cursor()
            cursor.arraysize = 5000  # Increased for parallel processing
            cursor.prefetchrows = cursor.arraysize + 1
            
            # Log query execution plan (optional)
            if logger.isEnabledFor(logging.DEBUG):
                explain_cursor = connection.cursor()
                explain_cursor.execute(f"EXPLAIN PLAN FOR {query}", params)
                explain_cursor.execute("SELECT * FROM table(DBMS_XPLAN.DISPLAY())")
                for row in explain_cursor:
                    logger.debug(f"Plan: {row[0]}")
                explain_cursor.close()
            
            # Execute query
            start_time = time.time()
            cursor.execute(query, params)
            
            # Monitor parallel execution
            self.monitor_parallel_execution(connection)
            self.parallel_stats['queries_executed'] += 1
            
            # Get column info
            columns = [desc[0] for desc in cursor.description]
            
            # Set up Parquet writer
            writer = None
            batch_rows = []
            batch_size = 5000  # Increased batch size
            
            # Use temp file to avoid network I/O during write
            temp_file = os.path.join(self.temp_dir, f"temp_{os.getpid()}_{time.time()}.parquet")
            
            for row in cursor:
                if self.shutdown:
                    break
                    
                batch_rows.append(row)
                last_id = row[0]  # Assuming first column is ID
                
                if len(batch_rows) >= batch_size:
                    # Check memory before processing
                    if not self.check_memory():
                        logger.error("Memory limit exceeded, stopping batch")
                        break
                    
                    # Convert to DataFrame with minimal memory footprint
                    df = pd.DataFrame(batch_rows, columns=columns)
                    
                    # Optimize dtypes immediately
                    df = self._optimize_dataframe_memory(df)
                    
                    if writer is None:
                        # Create schema from first batch
                        table = pa.Table.from_pandas(df)
                        writer = pq.ParquetWriter(
                            temp_file,
                            table.schema,
                            compression='snappy',
                            use_dictionary=['999_categories'],  # 999_categories has only 999 values
                            write_statistics=True,
                            row_group_size=10000  # Increased for larger chunks
                        )
                    
                    # Write batch
                    table = pa.Table.from_pandas(df)
                    writer.write_table(table)
                    rows_written += len(df)
                    
                    # Clear memory
                    del df, table
                    batch_rows = []
                    
                    if rows_written % 50000 == 0:
                        elapsed = time.time() - start_time
                        rate = rows_written / elapsed
                        logger.info(f"Streamed {rows_written:,} rows at {rate:,.0f} rows/sec")
            
            # Write final batch
            if batch_rows and not self.shutdown:
                df = pd.DataFrame(batch_rows, columns=columns)
                df = self._optimize_dataframe_memory(df)
                
                if writer is None:
                    table = pa.Table.from_pandas(df)
                    writer = pq.ParquetWriter(
                        temp_file,
                        table.schema,
                        compression='snappy',
                        use_dictionary=['999_categories'],
                        write_statistics=True,
                        row_group_size=10000
                    )
                
                table = pa.Table.from_pandas(df)
                writer.write_table(table)
                rows_written += len(df)
            
            cursor.close()
            
            # Close writer
            if writer:
                writer.close()
            
            # Move temp file to final location (network drive)
            if rows_written > 0 and os.path.exists(temp_file):
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                shutil.move(temp_file, output_path)
                
                elapsed = time.time() - start_time
                rate = rows_written / elapsed if elapsed > 0 else 0
                logger.info(f"Written {rows_written:,} rows to {output_path} at {rate:,.0f} rows/sec")
            
            return rows_written, last_id
            
        except Exception as e:
            logger.error(f"Error in stream extraction: {str(e)}")
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
            raise
        finally:
            self.pool.release(connection)
    
    def _optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame memory usage in-place
        """
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type != 'object':
                if col_type.name.startswith('int'):
                    # Determine optimal int type
                    c_min = df[col].min()
                    c_max = df[col].max()
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                
                elif col_type.name.startswith('float'):
                    df[col] = pd.to_numeric(df[col], downcast='float')
            
            else:
                # Convert 999_categories column to categorical
                if col == '999_categories':
                    df[col] = df[col].astype('category')
                
                # Convert other string columns with low cardinality
                elif df[col].nunique() < 50:
                    df[col] = df[col].astype('category')
        
        return df
    
    def get_adaptive_date_ranges(self) -> List[Tuple[datetime, datetime]]:
        """
        Split date range adaptively based on estimated data volume
        With parallel processing, we can handle larger chunks
        """
        if not self.start_date or not self.end_date:
            return [(None, None)]
        
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        date_ranges = []
        current = start
        
        # With parallel processing, use 12-hour chunks (increased from 4)
        # This should give ~650k rows per chunk at 1.3M rows/day
        chunk_hours = 12
        
        while current <= end:
            next_date = min(current + timedelta(hours=chunk_hours), end + timedelta(days=1))
            date_ranges.append((current, next_date))
            current = next_date
        
        return date_ranges
    
    def optimize_parallel_degree(self):
        """
        Dynamically adjust parallel degree based on performance
        """
        if self.parallel_stats['queries_executed'] > 0 and self.parallel_stats['parallel_executions'] > 0:
            success_rate = self.parallel_stats['parallel_executions'] / self.parallel_stats['queries_executed']
            
            if success_rate < 0.5 and self.parallel_degree > self.parallel_degree_range[0]:
                # Reduce parallel degree if not effective
                self.parallel_degree = max(self.parallel_degree - 2, self.parallel_degree_range[0])
                logger.info(f"Reduced parallel degree to {self.parallel_degree} (success rate: {success_rate:.1%})")
            elif success_rate > 0.8 and self.parallel_degree < self.parallel_degree_range[1]:
                # Increase parallel degree if working well
                self.parallel_degree = min(self.parallel_degree + 2, self.parallel_degree_range[1])
                logger.info(f"Increased parallel degree to {self.parallel_degree} (success rate: {success_rate:.1%})")
    
    def process_partition(self, partition_id: str, 
                         date_range: Tuple[datetime, datetime],
                         partition_value: Optional[Any] = None) -> Dict[str, Any]:
        """
        Process a partition with streaming and checkpointing
        """
        extraction_id = self.get_extraction_id()
        
        # Check if already completed
        checkpoint = self.load_checkpoint(extraction_id)
        if checkpoint and partition_id in checkpoint.get('completed_partitions', []):
            logger.info(f"Partition {partition_id} already completed, skipping")
            return checkpoint['partitions'].get(partition_id, {})
        
        # Determine output path
        partition_path = os.path.join(self.data_lake_path, self.output_table_name)
        
        if date_range[0]:
            date_str = date_range[0].strftime('%Y-%m-%d_%H')
            partition_path = os.path.join(partition_path, f"date={date_str}")
        
        if partition_value is not None:
            partition_path = os.path.join(partition_path, f"{self.partition_column}={partition_value}")
        
        os.makedirs(partition_path, exist_ok=True)
        
        # Get last processed ID from checkpoint
        last_id = None
        if checkpoint and partition_id in checkpoint.get('partitions', {}):
            last_id = checkpoint['partitions'][partition_id].get('last_id')
            logger.info(f"Resuming partition {partition_id} from ID: {last_id}")
        
        # Process data in chunks
        rows_processed = checkpoint.get('partitions', {}).get(partition_id, {}).get('rows_processed', 0)
        files_written = checkpoint.get('partitions', {}).get(partition_id, {}).get('files_written', [])
        chunk_num = len(files_written)
        
        logger.info(f"Processing partition {partition_id} with parallel degree {self.parallel_degree}")
        
        consecutive_empty = 0
        max_consecutive_empty = 2
        
        while consecutive_empty < max_consecutive_empty and not self.shutdown:
            try:
                # Optimize parallel degree periodically
                if chunk_num % 5 == 0:
                    self.optimize_parallel_degree()
                
                # Build query
                query, params = self.build_optimized_query(date_range, last_id, partition_value)
                
                # Generate output filename
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"part_{partition_id}_{timestamp}_{chunk_num:06d}.parquet"
                file_path = os.path.join(partition_path, filename)
                
                # Stream extract to parquet
                chunk_rows, new_last_id = self.stream_extract_to_parquet(query, params, file_path)
                
                if chunk_rows == 0:
                    consecutive_empty += 1
                    logger.info(f"Empty result {consecutive_empty}/{max_consecutive_empty}")
                    continue
                
                consecutive_empty = 0
                rows_processed += chunk_rows
                chunk_num += 1
                files_written.append(filename)
                last_id = new_last_id
                
                # Update checkpoint
                if not checkpoint:
                    checkpoint = {'completed_partitions': [], 'partitions': {}}
                
                checkpoint['partitions'][partition_id] = {
                    'rows_processed': rows_processed,
                    'files_written': files_written,
                    'last_id': last_id,
                    'last_update': datetime.now().isoformat()
                }
                
                self.save_checkpoint(extraction_id, checkpoint)
                
                logger.info(
                    f"Partition {partition_id}: {rows_processed:,} total rows "
                    f"(+{chunk_rows:,} in chunk {chunk_num})"
                )
                
                # Force garbage collection
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error in partition {partition_id}: {str(e)}")
                raise
        
        # Mark as completed
        if not self.shutdown:
            checkpoint['completed_partitions'].append(partition_id)
            self.save_checkpoint(extraction_id, checkpoint)
        
        return {
            'partition_id': partition_id,
            'rows_processed': rows_processed,
            'files_written': len(files_written),
            'path': partition_path
        }
    
    def get_extraction_id(self) -> str:
        """Generate unique extraction ID"""
        config_str = f"{self.output_table_name}_{self.start_date}_{self.end_date}_{','.join(sorted(self.excluded_categories))}"
        return hashlib.md5(config_str.encode()).hexdigest()[:8]
    
    def get_checkpoint_file(self, extraction_id: str) -> str:
        """Get checkpoint file path"""
        return os.path.join(self.checkpoint_dir, f"{extraction_id}_checkpoint.json")
    
    def load_checkpoint(self, extraction_id: str) -> Optional[Dict]:
        """Load checkpoint data if exists"""
        checkpoint_file = self.get_checkpoint_file(extraction_id)
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        return None
    
    def save_checkpoint(self, extraction_id: str, checkpoint_data: Dict):
        """Save checkpoint data atomically"""
        checkpoint_file = self.get_checkpoint_file(extraction_id)
        temp_file = f"{checkpoint_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        os.replace(temp_file, checkpoint_file)
    
    def run(self):
        """Execute the optimized ETL process with parallel processing"""
        start_time = datetime.now()
        extraction_id = self.get_extraction_id()
        
        logger.info("="*60)
        logger.info(f"Starting Optimized JOIN ETL Process with Parallel Execution")
        logger.info(f"Extraction ID: {extraction_id}")
        logger.info(f"Oracle Connection: {self.config['oracle_host']}:{self.config['oracle_port']}/{self.config['oracle_sid']} (SID)")
        logger.info(f"Categories loaded from: {self.categories_parquet_path}")
        logger.info(f"Valid categories: {len(self.valid_categories)} out of 999")
        logger.info(f"Memory limit: {self.memory_limit_percent}%")
        logger.info(f"Chunk size: {self.chunk_size:,} rows")
        logger.info(f"Parallel degree: {self.parallel_degree} (range: {self.parallel_degree_range})")
        logger.info(f"Available parallel servers: 16-160")
        logger.info("="*60)
        
        try:
            # Get date ranges (larger chunks with parallel processing)
            date_ranges = self.get_adaptive_date_ranges()
            logger.info(f"Processing {len(date_ranges)} time ranges")
            
            # Process each range sequentially to limit memory usage
            completed_partitions = []
            
            for date_range in date_ranges:
                if self.shutdown:
                    logger.info("Shutdown requested, stopping processing")
                    break
                
                # For billion-row tables, skip partition values to reduce complexity
                partition_id = f"{date_range[0].strftime('%Y%m%d_%H') if date_range[0] else 'all'}"
                
                try:
                    result = self.process_partition(partition_id, date_range, None)
                    completed_partitions.append(result)
                    logger.info(f"Completed partition: {partition_id} "
                              f"({len(completed_partitions)}/{len(date_ranges)})")
                    
                    # Check memory after each partition
                    if not self.check_memory():
                        logger.warning("Memory pressure detected, pausing...")
                        time.sleep(10)
                        gc.collect()
                    
                except Exception as e:
                    logger.error(f"Failed partition {partition_id}: {str(e)}")
                    if "ORA-01555" in str(e):  # Snapshot too old
                        logger.warning("Snapshot too old error - consider smaller chunks or undo retention")
                    raise
            
            # Summary
            total_rows = sum(p['rows_processed'] for p in completed_partitions)
            total_files = sum(p['files_written'] for p in completed_partitions)
            
            # Log parallel execution statistics
            if self.enable_parallel_monitoring:
                logger.info("="*60)
                logger.info("Parallel Execution Statistics:")
                logger.info(f"Total queries executed: {self.parallel_stats['queries_executed']}")
                logger.info(f"Queries with parallel execution: {self.parallel_stats['parallel_executions']}")
                if self.parallel_stats['queries_executed'] > 0:
                    success_rate = self.parallel_stats['parallel_executions'] / self.parallel_stats['queries_executed']
                    logger.info(f"Parallel success rate: {success_rate:.1%}")
                logger.info(f"Maximum parallel degree used: {self.parallel_stats['max_parallel_degree']}")
                logger.info("="*60)
            
            # Write metadata
            metadata = {
                'extraction_id': extraction_id,
                'query_type': 'optimized_join_parallel',
                'categories_source': self.categories_parquet_path,
                'excluded_categories': list(self.excluded_categories),
                'valid_categories': list(self.valid_categories),
                'total_rows': total_rows,
                'total_files': total_files,
                'total_partitions': len(completed_partitions),
                'date_range': f"{self.start_date} to {self.end_date}" if self.start_date else "all",
                'extraction_date': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds(),
                'completed': not self.shutdown,
                'parallel_stats': self.parallel_stats,
                'partitions': completed_partitions
            }
            
            metadata_path = os.path.join(
                self.data_lake_path,
                self.output_table_name,
                f'_metadata_{extraction_id}.json'
            )
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Clean up if completed
            if not self.shutdown:
                checkpoint_file = self.get_checkpoint_file(extraction_id)
                if os.path.exists(checkpoint_file):
                    os.remove(checkpoint_file)
            
            duration = datetime.now() - start_time
            logger.info("="*60)
            if self.shutdown:
                logger.info("ETL process stopped by user")
            else:
                logger.info("ETL process completed successfully!")
            logger.info(f"Total rows: {total_rows:,}")
            logger.info(f"Total files: {total_files}")
            logger.info(f"Duration: {duration}")
            logger.info(f"Average speed: {total_rows/duration.total_seconds():.0f} rows/second")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            logger.info(f"Resume using extraction ID: {extraction_id}")
            raise
        finally:
            # Clean up temp directory
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            
            # Close connection pool
            self.pool.close()


def main():
    """Main execution function with parallel-optimized configuration"""
    
    # Configuration optimized for parallel processing
    config = {
        # Oracle connection using SID
        'oracle_user': os.environ.get('ORACLE_USER', 'your_username'),
        'oracle_password': os.environ.get('ORACLE_PASSWORD', 'your_password'),
        'oracle_host': os.environ.get('ORACLE_HOST', 'hostname'),
        'oracle_port': os.environ.get('ORACLE_PORT', '1521'),
        'oracle_sid': os.environ.get('ORACLE_SID', 'ORCL'),
        
        # Note: If you need to use a service name instead of SID, you can modify the connection
        # in __init__ to use: dsn = oracledb.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
        
        # Output settings
        'output_table_name': 'joined_table1_table2_parallel',
        'data_lake_path': '/mnt/data_lake/raw',
        'checkpoint_dir': './etl_checkpoints',
        'temp_dir': '/tmp/etl_temp',  # Local SSD for temp files
        
        # Category configuration - UPDATED TO USE PARQUET FILE
        'categories_parquet_path': '/path/to/categories.parquet',  # Path to parquet file with 999_distinct values
        
        # Date configuration
        'date_column': 'tb1.transaction_date',
        'start_date': '2024-01-01',
        'end_date': '2024-01-07',  # Start with 1 week for testing
        
        # Category exclusions (with 999 total categories)
        'excluded_categories': ['cat1', 'cat2'],
        
        # Table 2 columns - specify the 17 columns you need from table_2
        'table2_columns': [
            'id',              # This will be aliased as tb2_id to avoid conflict
            '999_categories',   # The category column
            'col1',            # Replace with actual column names
            'col2',
            'col3',
            'col4',
            'col5',
            'col6',
            'col7',
            'col8',
            'col9',
            'col10',
            'col11',
            'col12',
            'col13',
            'col14',
            'col15'
        ],
        
        # Parallel-optimized settings
        'chunk_size': 50000,           # Increased from 10000
        'max_workers': 2,              # Keep limited for memory
        'parallel_degree': 8,          # Start with 8, can test 4-16
        'parallel_degree_range': (4, 16),  # Min and max for adaptive tuning
        'memory_limit_percent': 70,    # Leave 30% buffer
        'enable_parallel_monitoring': True,  # Monitor parallel execution
    }
    
    # Create and run ETL
    etl = OptimizedOracleJoinETL(config)
    
    try:
        etl.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted - run again to resume")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        logger.info("Run again to resume from checkpoint")
        sys.exit(1)


if __name__ == "__main__":
    main()
