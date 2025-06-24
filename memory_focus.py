#!/usr/bin/env python3
"""
Optimized Oracle to Parquet ETL Script for Billion-Row JOIN Operations
Enhanced with Oracle Parallel Processing capabilities and optimized Parquet compression
Designed for 16GB RAM constraint with streaming and memory management
Modified to read category values from a parquet file instead of database
Uses Oracle SID connection instead of DSN

MEMORY-AWARE VERSION: Enhanced memory management for 16GB constraint
- Adaptive chunk sizing based on memory pressure
- Memory semaphore to limit concurrent operations
- Async write queue with memory limits
- Periodic memory profiling and monitoring
- Emergency stop at 90% memory usage

Performance Tips:
1. Start with conservative settings and increase gradually
2. Monitor memory usage logs to find optimal chunk_size
3. Reduce max_workers if memory issues persist
4. Consider processing during off-peak hours
5. Close other applications to free memory
"""

import oracledb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
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
import signal
import numpy as np
import concurrent.futures
import threading
from queue import Queue, Empty
from dataclasses import dataclass
from functools import lru_cache

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

@dataclass
class ExtractionTask:
    """Data class for extraction tasks"""
    partition_id: str
    date_range: Tuple[Optional[datetime], Optional[datetime]]
    partition_value: Optional[Any]
    last_id: Optional[int] = None

class AsyncParquetWriter:
    """Asynchronous Parquet writer with memory-aware queue management"""
    def __init__(self, compression_args: Dict[str, Any], max_writers: int = 5, max_queue_gb: float = 1.0):
        self.compression_args = compression_args
        self.write_queue = Queue(maxsize=10)  # Reduced from 20
        self.writer_pool = {}
        self.max_writers = max_writers
        self.writer_thread = None
        self.shutdown = False
        self.write_lock = threading.Lock()
        self.max_queue_gb = max_queue_gb
        self.queue_memory_usage = 0
        self.memory_lock = threading.Lock()
        
    def estimate_dataframe_memory(self, df: pd.DataFrame) -> float:
        """Estimate DataFrame memory usage in GB"""
        return df.memory_usage(deep=True).sum() / (1024 ** 3)
    
    def submit(self, path: str, df: pd.DataFrame, schema: pa.Schema):
        """Submit write task with memory checking"""
        df_memory = self.estimate_dataframe_memory(df)
        
        # Wait if queue has too much data
        while self.queue_memory_usage > self.max_queue_gb:
            logger.debug(f"Write queue memory at {self.queue_memory_usage:.2f}GB, waiting...")
            time.sleep(0.1)
        
        with self.memory_lock:
            self.queue_memory_usage += df_memory
        
        self.write_queue.put((path, df, schema, df_memory))
    
    def start(self):
        """Start the async writer thread"""
        self.writer_thread = threading.Thread(target=self._writer_loop)
        self.writer_thread.daemon = True
        self.writer_thread.start()
        logger.info("Started async Parquet writer thread with memory limit")
        
    def _writer_loop(self):
        """Main writer loop with memory tracking"""
        while not self.shutdown:
            try:
                task = self.write_queue.get(timeout=1)
                if task is None:
                    break
                    
                path, df, schema, df_memory = task
                self._write_to_parquet(path, df, schema)
                
                # Update memory usage
                with self.memory_lock:
                    self.queue_memory_usage -= df_memory
                
                self.write_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in writer thread: {str(e)}")
    
    def shutdown_writer(self):
        """Shutdown the writer thread"""
        self.shutdown = True
        self.write_queue.put(None)
        if self.writer_thread:
            self.writer_thread.join()
        logger.info("Shut down async writer thread")

class OptimizedOracleJoinETL:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize optimized ETL processor with memory-aware settings
        """
        self.config = config
        
        # Memory-aware chunk sizing
        self.base_chunk_size = config.get('chunk_size', 50000)  # Reduced back to safer size
        self.chunk_size = self.base_chunk_size  # Current chunk size (can be adjusted)
        self.max_workers = config.get('max_workers', 2)  # Reduced to control memory
        
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
        self.parallel_degree = config.get('parallel_degree', 8)
        self.parallel_degree_range = config.get('parallel_degree_range', (4, 16))
        self.enable_parallel_monitoring = config.get('enable_parallel_monitoring', True)
        
        # Memory management
        self.memory_limit_percent = config.get('memory_limit_percent', 70)
        self.critical_memory_percent = config.get('critical_memory_percent', 80)
        self.memory_check_interval = 100000  # Check memory every N rows
        self.memory_semaphore = threading.Semaphore(self.max_workers)
        self.global_memory_lock = threading.Lock()
        self.active_memory_usage = 0
        
        # Track memory per worker
        self.worker_memory = {}
        self.worker_memory_lock = threading.Lock()
        
        # Network optimization settings (conservative for memory)
        self.oracle_fetch_size = config.get('oracle_fetch_size', 5000)
        self.oracle_prefetch_rows = config.get('oracle_prefetch_rows', 10000)
        self.use_connection_multiplexing = config.get('use_connection_multiplexing', True)
        
        # ENHANCED: Parquet compression settings
        self.parquet_compression = config.get('parquet_compression', 'zstd')
        self.parquet_compression_level = config.get('parquet_compression_level', 6)
        self.parquet_row_group_size = config.get('parquet_row_group_size', 671000)  # From your logs
        self.parquet_use_dictionary = config.get('parquet_use_dictionary', ['999_categories'])
        self.parquet_write_statistics = config.get('parquet_write_statistics', True)
        self.parquet_write_page_index = config.get('parquet_write_page_index', True)
        
        logger.info(f"Using row group size: {self.parquet_row_group_size:,} rows")
        
        # GC monitoring flag
        self.monitor_gc = config.get('monitor_gc', True)
        
        # Initialize Oracle connection pool with enhanced settings
        oracle_host = config['oracle_host']
        oracle_port = config['oracle_port']
        oracle_sid = config['oracle_sid']
        
        # Create DSN with network optimization
        dsn = oracledb.makedsn(
            oracle_host, 
            oracle_port, 
            sid=oracle_sid,
            tcp_connect_timeout=10,
            transport_connect_timeout=3,
            sdu=65535  # Max SDU for better network throughput
        )
        
        logger.info(f"Connecting to Oracle: {oracle_host}:{oracle_port}/{oracle_sid} with SDU=65535")
        
        # Enhanced connection pool settings (conservative for memory)
        self.pool = oracledb.create_pool(
            user=config['oracle_user'],
            password=config['oracle_password'],
            dsn=dsn,
            min=1,
            max=min(self.max_workers + 1, 3),  # Limit connections for memory
            increment=1,
            threaded=True,
            getmode=oracledb.POOL_GETMODE_WAIT,
            timeout=30,
            ping_interval=60  # Keep connections alive
        )
        
        # Initialize Oracle client if needed
        if 'oracle_lib_dir' in config:
            oracledb.init_oracle_client(lib_dir=config['oracle_lib_dir'])
        
        # Create directories
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Category optimization: Load categories from parquet file
        self.valid_categories = None
        self._initialize_categories_from_parquet_optimized()
        
        # Initialize parallel monitoring
        self.parallel_stats = {
            'queries_executed': 0,
            'parallel_executions': 0,
            'avg_parallel_degree': 0,
            'max_parallel_degree': 0
        }
        
        # Initialize async writer with memory limit
        compression_args = self._get_parquet_compression_args()
        self.async_writer = AsyncParquetWriter(compression_args, max_queue_gb=1.0)
        self.async_writer.start()
        
        # Initialize extraction executor with limited threads
        self.extraction_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix='oracle_extract'
        )
        
        # Memory pool for DataFrame creation
        self.df_memory_pool = []
        self.pool_lock = threading.Lock()
        
        # Optimize GC settings
        self._optimize_gc_settings()
        
        # Set up GC monitoring
        if self.monitor_gc:
            self._setup_gc_monitoring()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown = False
        
        # Log configuration
        logger.info("="*60)
        logger.info("ETL Configuration:")
        logger.info(f"Base chunk size: {self.base_chunk_size:,} rows (adaptive)")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Connection pool size: min={self.pool.min}, max={self.pool.max}")
        logger.info(f"Oracle fetch size: {self.oracle_fetch_size:,}")
        logger.info(f"Oracle prefetch rows: {self.oracle_prefetch_rows:,}")
        logger.info(f"Memory limit: {self.memory_limit_percent}%")
        logger.info(f"Critical memory: {self.critical_memory_percent}%")
        logger.info(f"Parquet compression: {self.parquet_compression} (level {self.parquet_compression_level})")
        logger.info(f"Row group size: {self.parquet_row_group_size:,} rows")
        logger.info(f"Async writer queue limit: 1.0 GB")
        logger.info("="*60)
    
    def _initialize_categories_from_parquet_optimized(self):
        """Load categories more efficiently using PyArrow"""
        try:
            if not os.path.exists(self.categories_parquet_path):
                raise FileNotFoundError(f"Categories parquet file not found: {self.categories_parquet_path}")
            
            logger.info(f"Loading categories from parquet file: {self.categories_parquet_path}")
            
            # Use PyArrow for faster loading
            table = pq.read_table(
                self.categories_parquet_path,
                columns=['999_distinct values'],  # Only read needed column
                use_threads=True
            )
            
            # Convert to set efficiently
            category_array = table.column('999_distinct values')
            self.valid_categories = set(
                val.as_py() for val in category_array if val.is_valid
            ) - self.excluded_categories
            
            logger.info(f"Loaded {len(self.valid_categories)} valid categories efficiently")
            
            # Log sample for verification
            sample_categories = list(self.valid_categories)[:5]
            logger.info(f"Sample categories: {sample_categories}")
            
        except Exception as e:
            logger.error(f"Error loading categories from parquet file: {str(e)}")
            raise
    
    def _optimize_gc_settings(self):
        """Optimize GC for billion-row processing workload"""
        self.original_gc_thresholds = gc.get_threshold()
        logger.info(f"Original GC thresholds: {self.original_gc_thresholds}")
        
        # Increase thresholds for large data processing but be conservative
        gc.set_threshold(500000, 15, 15)  # More conservative than 700000
        logger.info("Set GC threshold to (500000, 15, 15) for balanced performance")
        
        collected = gc.collect()
        logger.info(f"Initial GC collected {collected} objects")
        
        gc.freeze()
        logger.info("Froze initial objects to reduce GC overhead")
    
    def _setup_gc_monitoring(self):
        """Set up GC monitoring callbacks"""
        self.gc_stats = {
            'collections': [],
            'total_collected': 0,
            'total_uncollectable': 0,
            'total_time': 0.0,
            'collection_count': 0
        }
        
        def gc_callback(phase, info):
            if phase == "start":
                info['start_time'] = time.time()
            elif phase == "stop":
                duration = time.time() - info.get('start_time', time.time())
                
                self.gc_stats['collections'].append({
                    'generation': info['generation'],
                    'collected': info['collected'],
                    'uncollectable': info['uncollectable'],
                    'duration': duration,
                    'timestamp': time.time()
                })
                
                self.gc_stats['total_collected'] += info['collected']
                self.gc_stats['total_uncollectable'] += info['uncollectable']
                self.gc_stats['total_time'] += duration
                self.gc_stats['collection_count'] += 1
                
                if duration > 0.1 or info['collected'] > 10000:
                    logger.info(
                        f"GC Gen {info['generation']}: collected {info['collected']} objects "
                        f"in {duration:.3f}s, {info['uncollectable']} uncollectable"
                    )
        
        gc.callbacks.append(gc_callback)
        logger.info("GC monitoring enabled")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Shutdown signal received, finishing current batches...")
        self.shutdown = True
    
    def enable_parallel_session_enhanced(self, connection):
        """Enable parallel execution with enhanced settings"""
        try:
            cursor = connection.cursor()
            
            # Enable parallel DML
            cursor.execute("ALTER SESSION ENABLE PARALLEL DML")
            
            # Force parallel execution
            cursor.execute(f"ALTER SESSION FORCE PARALLEL QUERY PARALLEL {self.parallel_degree}")
            
            # Set parallel degree policy
            cursor.execute("ALTER SESSION SET parallel_degree_policy = 'MANUAL'")
            
            # Performance optimizations
            cursor.execute("ALTER SESSION SET workarea_size_policy = 'AUTO'")
            cursor.execute("ALTER SESSION SET STATISTICS_LEVEL = ALL")
            
            # Enable adaptive plans
            try:
                cursor.execute("ALTER SESSION SET OPTIMIZER_ADAPTIVE_PLANS = TRUE")
            except:
                pass  # Not all versions support this
            
            # Increase hash area for better join performance
            try:
                cursor.execute("ALTER SESSION SET \"_hash_join_enabled\" = TRUE")
                cursor.execute("ALTER SESSION SET \"_px_max_message_pool_pct\" = 80")
            except:
                pass
            
            cursor.close()
            logger.info(f"Enabled enhanced parallel execution with degree {self.parallel_degree}")
            
        except Exception as e:
            logger.warning(f"Could not set all parallel options: {str(e)}")
    
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
            "NO_PARALLEL_INDEX(tb1)",
            "NO_PARALLEL_INDEX(tb2)",
            f"FIRST_ROWS({self.chunk_size})",  # Optimize for streaming
            "RESULT_CACHE"  # Use result cache if available
        ]
        
        # Add partition hints if tables are partitioned
        if self.date_column and date_range and date_range[0]:
            date_str = date_range[0].strftime('%Y%m%d')
            # Uncomment if your tables are partitioned
            # parallel_hints.append(f"PARTITION(tb1 PARTITION_FOR({date_str}))")
        
        hints = f"/*+ {' '.join(parallel_hints)} */"
        
        # Build column list
        table2_column_list = []
        for col in self.table2_columns:
            table2_column_list.append(f'tb2."{col}" as "tb2_{col}"')
        
        table2_columns_str = ', '.join(table2_column_list)
        
        # Standard parallel query
        query = f"""
        SELECT {hints}
            tb1.*,
            {table2_columns_str}
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
        
        query += " ORDER BY tb1.id"
        
        # Use FETCH FIRST for Oracle 12c+
        query = f"""
        SELECT * FROM (
            {query}
            FETCH FIRST :chunk_size ROWS ONLY
        )
        """
        params['chunk_size'] = self.chunk_size if not params.get('chunk_size') else params['chunk_size']
        
        return query, params
    
    def adaptive_fetch_size(self, connection) -> int:
        """Dynamically adjust fetch size based on available memory"""
        mem = psutil.virtual_memory()
        available_mb = mem.available / (1024 * 1024)
        
        # Estimate memory per row
        bytes_per_row = self.config.get('estimated_row_size_bytes', 200)
        
        # Calculate optimal fetch size (max 20% of available memory)
        max_rows_in_memory = int((available_mb * 0.20 * 1024 * 1024) / bytes_per_row)
        
        # Bound between reasonable limits
        fetch_size = max(5000, min(max_rows_in_memory, 50000))
        
        logger.debug(f"Adaptive fetch size: {fetch_size:,} rows (available memory: {available_mb:.0f}MB)")
        return fetch_size
    
    def rows_to_dataframe_optimized(self, rows: List, columns: List) -> pd.DataFrame:
        """Convert rows to DataFrame with memory optimization"""
        if not rows:
            return pd.DataFrame(columns=columns)
        
        # Convert to numpy array first (more efficient)
        arr = np.array(rows, dtype='object')
        
        # Create DataFrame without copying
        df = pd.DataFrame(arr, columns=columns, copy=False)
        
        # Apply dtypes efficiently
        for col in df.columns:
            if col in ['id', 'tb2_id']:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
            elif df[col].dtype == 'object':
                # Check if numeric
                try:
                    sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                    if sample is not None and isinstance(sample, (int, float)):
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    pass
        
        return df
    
    def stream_extract_to_parquet_enhanced(self, query: str, params: Dict, 
                                          output_path: str) -> Tuple[int, int]:
        """
        Enhanced streaming with memory-aware processing
        """
        connection = self.pool.acquire()
        rows_written = 0
        last_id = None
        schema = None
        worker_id = threading.current_thread().name
        
        # Track memory for this worker
        with self.worker_memory_lock:
            self.worker_memory[worker_id] = 0
        
        try:
            # Enable enhanced parallel execution
            self.enable_parallel_session_enhanced(connection)
            
            cursor = connection.cursor()
            
            # Start with conservative fetch size
            initial_chunk_size = self.adaptive_chunk_size()
            cursor.arraysize = min(5000, initial_chunk_size // 10)
            cursor.prefetchrows = cursor.arraysize * 2
            
            logger.info(f"Worker {worker_id}: Starting with chunk_size={initial_chunk_size}, "
                       f"fetch_size={cursor.arraysize}")
            
            # Execute query
            start_time = time.time()
            cursor.execute(query, params)
            
            # Monitor parallel execution
            if self.enable_parallel_monitoring:
                self.monitor_parallel_execution(connection)
            self.parallel_stats['queries_executed'] += 1
            
            # Get column info
            columns = [desc[0] for desc in cursor.description]
            
            # Batch processing with memory monitoring
            batch_rows = []
            batch_size = min(5000, initial_chunk_size // 10)  # Smaller batches
            rows_since_memory_check = 0
            
            # Process rows
            while True:
                # Check memory periodically
                if rows_since_memory_check >= self.memory_check_interval:
                    if not self.wait_for_memory(timeout=60):
                        logger.error(f"Worker {worker_id}: Memory timeout, stopping extraction")
                        break
                    
                    # Adjust batch size based on current memory
                    new_chunk_size = self.adaptive_chunk_size()
                    if new_chunk_size != initial_chunk_size:
                        batch_size = min(5000, new_chunk_size // 10)
                        cursor.arraysize = batch_size
                        logger.info(f"Worker {worker_id}: Adjusted batch_size to {batch_size}")
                    
                    rows_since_memory_check = 0
                
                # Fetch batch
                try:
                    rows = cursor.fetchmany(batch_size)
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Fetch error: {str(e)}")
                    break
                
                if not rows:
                    break
                
                if self.shutdown:
                    logger.info(f"Worker {worker_id}: Shutdown requested")
                    break
                
                batch_rows.extend(rows)
                rows_since_memory_check += len(rows)
                
                # Update last_id
                if rows:
                    last_id = rows[-1][0]
                
                # Process when batch is ready
                if len(batch_rows) >= batch_size:
                    # Convert to DataFrame
                    df = self.rows_to_dataframe_optimized(batch_rows, columns)
                    
                    # Estimate memory usage
                    df_memory_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
                    
                    with self.worker_memory_lock:
                        self.worker_memory[worker_id] = df_memory_mb
                    
                    logger.debug(f"Worker {worker_id}: Processing batch of {len(df)} rows "
                                f"({df_memory_mb:.1f} MB)")
                    
                    # Clean and optimize
                    df = self._clean_numeric_data(df)
                    df = self._optimize_dataframe_memory(df)
                    
                    if schema is None:
                        # Create schema from first batch
                        schema = self._create_consistent_schema(df)
                        logger.info(f"Created schema with {len(schema)} fields")
                    
                    # Apply schema
                    df = self._apply_consistent_schema(df, schema)
                    
                    # Generate filename for this chunk
                    chunk_file = f"{output_path}.chunk_{rows_written//batch_size:04d}.tmp"
                    
                    # Submit to async writer (will wait if queue is full)
                    self.async_writer.submit(chunk_file, df, schema)
                    
                    rows_written += len(df)
                    
                    # Clear batch and update memory tracking
                    batch_rows = []
                    with self.worker_memory_lock:
                        self.worker_memory[worker_id] = 0
                    
                    # Explicitly delete DataFrame
                    del df
                    
                    # Log progress
                    if rows_written % 50000 == 0:
                        elapsed = time.time() - start_time
                        rate = rows_written / elapsed
                        mem_info = psutil.virtual_memory()
                        logger.info(f"Worker {worker_id}: Processed {rows_written:,} rows at "
                                  f"{rate:,.0f} rows/sec (Memory: {mem_info.percent:.1f}%)")
            
            # Process final batch
            if batch_rows and not self.shutdown:
                # Wait for memory if needed
                if not self.wait_for_memory(timeout=60):
                    logger.error(f"Worker {worker_id}: Cannot process final batch due to memory")
                else:
                    df = self.rows_to_dataframe_optimized(batch_rows, columns)
                    df = self._clean_numeric_data(df)
                    
                    if schema is None:
                        df = self._optimize_dataframe_memory(df)
                        schema = self._create_consistent_schema(df)
                    
                    df = self._apply_consistent_schema(df, schema)
                    
                    chunk_file = f"{output_path}.chunk_{rows_written//batch_size:04d}.tmp"
                    self.async_writer.submit(chunk_file, df, schema)
                    rows_written += len(df)
                    del df
            
            cursor.close()
            
            # Wait for async writes to complete
            logger.info(f"Worker {worker_id}: Waiting for async writes to complete...")
            self.async_writer.write_queue.join()
            
            # Merge chunk files into final output
            if rows_written > 0:
                self._merge_chunk_files(output_path, schema)
                
                elapsed = time.time() - start_time
                rate = rows_written / elapsed if elapsed > 0 else 0
                logger.info(f"Worker {worker_id}: Completed {rows_written:,} rows at {rate:,.0f} rows/sec")
            
            return rows_written, last_id
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error in stream extraction: {str(e)}")
            raise
        finally:
            # Clean up worker memory tracking
            with self.worker_memory_lock:
                self.worker_memory.pop(worker_id, None)
            
            self.pool.release(connection)
    
    def _merge_chunk_files(self, output_path: str, schema: pa.Schema):
        """Merge temporary chunk files into final output"""
        import glob
        
        chunk_pattern = f"{output_path}.chunk_*.tmp"
        chunk_files = sorted(glob.glob(chunk_pattern))
        
        if not chunk_files:
            return
        
        logger.info(f"Merging {len(chunk_files)} chunk files...")
        
        # Create final parquet file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write in smaller batches to control memory
        with pq.ParquetWriter(
            output_path,
            schema,
            **self._get_parquet_compression_args()
        ) as writer:
            for i, chunk_file in enumerate(chunk_files):
                try:
                    # Check memory before reading chunk
                    if not self.wait_for_memory(timeout=30):
                        logger.error("Cannot merge chunks due to memory constraints")
                        break
                    
                    # Write chunk
                    table = pq.read_table(chunk_file)
                    
                    # Adjust row group size if needed
                    row_group_size = min(self.parquet_row_group_size, len(table))
                    
                    # Write to final file
                    writer.write_table(table, row_group_size=row_group_size)
                    
                    # Remove chunk file immediately to free disk space
                    os.remove(chunk_file)
                    
                    # Clean up
                    del table
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"Merged {i + 1}/{len(chunk_files)} chunks")
                        gc.collect()
                        
                except Exception as e:
                    logger.error(f"Error merging chunk {chunk_file}: {str(e)}")
                    raise
        
        # Verify all chunks were removed
        remaining_chunks = glob.glob(chunk_pattern)
        if remaining_chunks:
            logger.warning(f"{len(remaining_chunks)} chunk files were not removed")
        
        # Log file size
        if os.path.exists(output_path):
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"Final Parquet file size: {file_size_mb:.2f} MB")
    
    def process_partition_parallel(self, task: ExtractionTask) -> Dict[str, Any]:
        """Process a partition with memory-controlled parallel extraction"""
        partition_id = task.partition_id
        
        # Acquire memory semaphore
        self.memory_semaphore.acquire()
        
        try:
            logger.info(f"Processing partition {partition_id} with memory control")
            
            # Check memory before starting
            if not self.wait_for_memory(timeout=30):
                raise RuntimeError("Cannot start partition processing due to memory constraints")
            
            # Determine output path
            partition_path = os.path.join(self.data_lake_path, self.output_table_name)
            
            if task.date_range[0]:
                date_str = task.date_range[0].strftime('%Y-%m-%d_%H')
                partition_path = os.path.join(partition_path, f"date={date_str}")
            
            if task.partition_value is not None:
                partition_path = os.path.join(partition_path, f"{self.partition_column}={task.partition_value}")
            
            os.makedirs(partition_path, exist_ok=True)
            
            # Process data
            rows_processed = 0
            files_written = []
            last_id = task.last_id
            chunk_num = 0
            
            consecutive_empty = 0
            max_consecutive_empty = 2
            
            while consecutive_empty < max_consecutive_empty and not self.shutdown:
                try:
                    # Optimize parallel degree periodically
                    if chunk_num % 5 == 0:
                        self.optimize_parallel_degree()
                    
                    # Adjust chunk size based on current memory
                    current_chunk_size = self.adaptive_chunk_size()
                    
                    # Build query
                    query, params = self.build_optimized_query(
                        task.date_range, last_id, task.partition_value
                    )
                    
                    # Override chunk_size in params with adaptive size
                    params['chunk_size'] = current_chunk_size
                    
                    logger.debug(f"Using adaptive chunk size: {current_chunk_size:,} rows")
                    
                    # Generate output filename
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"part_{partition_id}_{timestamp}_{chunk_num:06d}.parquet"
                    file_path = os.path.join(partition_path, filename)
                    
                    # Stream extract with enhanced method
                    chunk_rows, new_last_id = self.stream_extract_to_parquet_enhanced(
                        query, params, file_path
                    )
                    
                    if chunk_rows == 0:
                        consecutive_empty += 1
                        logger.debug(f"Empty result {consecutive_empty}/{max_consecutive_empty}")
                        continue
                    
                    consecutive_empty = 0
                    rows_processed += chunk_rows
                    chunk_num += 1
                    files_written.append(filename)
                    last_id = new_last_id
                    
                    # Log memory status
                    mem_info = psutil.virtual_memory()
                    logger.info(
                        f"Partition {partition_id}: {rows_processed:,} total rows "
                        f"(+{chunk_rows:,} in chunk {chunk_num}, Memory: {mem_info.percent:.1f}%)"
                    )
                    
                    # Force cleanup between chunks
                    gc.collect()
                    
                    # Small delay to allow system to breathe
                    if chunk_num % 3 == 0:
                        time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error in partition {partition_id}: {str(e)}")
                    raise
            
            return {
                'partition_id': partition_id,
                'rows_processed': rows_processed,
                'files_written': len(files_written),
                'path': partition_path
            }
            
        finally:
            # Always release semaphore
            self.memory_semaphore.release()
    
    def get_adaptive_date_ranges(self) -> List[Tuple[datetime, datetime]]:
        """
        Split date range adaptively based on estimated data volume
        """
        if not self.start_date or not self.end_date:
            return [(None, None)]
        
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        date_ranges = []
        current = start
        
        # Conservative: 12-hour chunks for memory management
        chunk_hours = 12
        
        while current <= end:
            next_date = min(current + timedelta(hours=chunk_hours), end + timedelta(days=1))
            date_ranges.append((current, next_date))
            current = next_date
        
        return date_ranges
    
    def run_parallel(self):
        """Execute ETL with parallel partition processing"""
        start_time = datetime.now()
        extraction_id = self.get_extraction_id()
        
        logger.info("="*60)
        logger.info(f"Starting Enhanced Parallel JOIN ETL Process")
        logger.info(f"Extraction ID: {extraction_id}")
        logger.info(f"Using {self.max_workers} parallel workers")
        logger.info("="*60)
        
        try:
            # Log initial memory state
            self.log_memory_profile("at startup")
            
            # Get date ranges
            date_ranges = self.get_adaptive_date_ranges()
            logger.info(f"Processing {len(date_ranges)} time ranges")
            
            # Create extraction tasks
            tasks = []
            for date_range in date_ranges:
                task = ExtractionTask(
                    partition_id=f"{date_range[0].strftime('%Y%m%d_%H') if date_range[0] else 'all'}",
                    date_range=date_range,
                    partition_value=None
                )
                tasks.append(task)
            
            # Process tasks with memory-aware parallelism
            completed_partitions = []
            future_to_task = {}
            
            # Determine safe number of concurrent tasks based on memory
            _, current_mem = self.check_memory_with_threshold()
            if current_mem < 50:
                concurrent_tasks = self.max_workers
            elif current_mem < 65:
                concurrent_tasks = max(1, self.max_workers // 2)
            else:
                concurrent_tasks = 1
            
            logger.info(f"Starting with {concurrent_tasks} concurrent tasks (memory at {current_mem:.1f}%)")
            
            # Submit initial batch of tasks
            for task in tasks[:concurrent_tasks]:
                future = self.extraction_executor.submit(self.process_partition_parallel, task)
                future_to_task[future] = task
            
            remaining_tasks = tasks[concurrent_tasks:]
            
            # Process completed tasks and submit new ones
            while future_to_task:
                # Wait for any task to complete
                done, pending = concurrent.futures.wait(
                    future_to_task.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED
                )
                
                for future in done:
                    task = future_to_task.pop(future)
                    
                    try:
                        result = future.result()
                        completed_partitions.append(result)
                        logger.info(
                            f"Completed partition: {task.partition_id} "
                            f"({len(completed_partitions)}/{len(tasks)})"
                        )
                        
                        # Log memory profile every 5 partitions
                        if len(completed_partitions) % 5 == 0:
                            self.log_memory_profile(f"after {len(completed_partitions)} partitions")
                        
                        # Submit next task if available and memory allows
                        if remaining_tasks and not self.shutdown:
                            # Check if we should continue with parallel processing
                            _, current_mem = self.check_memory_with_threshold()
                            if current_mem < self.critical_memory_percent:
                                next_task = remaining_tasks.pop(0)
                                new_future = self.extraction_executor.submit(
                                    self.process_partition_parallel, next_task
                                )
                                future_to_task[new_future] = next_task
                            else:
                                logger.warning(f"Memory at {current_mem:.1f}%, pausing parallel submission")
                                # Wait for memory to improve before submitting more tasks
                                time.sleep(5)
                            
                    except Exception as e:
                        logger.error(f"Failed partition {task.partition_id}: {str(e)}")
                        if "ORA-01555" in str(e):
                            logger.warning("Snapshot too old error - consider smaller chunks")
                        raise
                
                if self.shutdown:
                    logger.info("Shutdown requested, cancelling remaining tasks")
                    for future in future_to_task:
                        future.cancel()
                    break
            
            # Summary
            total_rows = sum(p['rows_processed'] for p in completed_partitions)
            total_files = sum(p['files_written'] for p in completed_partitions)
            
            # Log statistics
            self._log_final_statistics(
                extraction_id, total_rows, total_files, 
                completed_partitions, start_time
            )
            
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
            
            # Final memory profile
            self.log_memory_profile("at completion")
            
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            # Log memory state at failure
            self.log_memory_profile("at failure")
            raise
        finally:
            self.cleanup()
    
    def _log_final_statistics(self, extraction_id: str, total_rows: int, 
                            total_files: int, completed_partitions: List,
                            start_time: datetime):
        """Log final statistics and metadata"""
        # Parallel execution statistics
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
            'query_type': 'enhanced_parallel_join',
            'categories_source': self.categories_parquet_path,
            'excluded_categories': list(self.excluded_categories),
            'valid_categories_count': len(self.valid_categories),
            'total_rows': total_rows,
            'total_files': total_files,
            'total_partitions': len(completed_partitions),
            'date_range': f"{self.start_date} to {self.end_date}" if self.start_date else "all",
            'extraction_date': datetime.now().isoformat(),
            'duration_seconds': (datetime.now() - start_time).total_seconds(),
            'completed': not self.shutdown,
            'performance_settings': {
                'chunk_size': self.chunk_size,
                'max_workers': self.max_workers,
                'parallel_degree': self.parallel_degree,
                'oracle_fetch_size': self.oracle_fetch_size,
                'oracle_prefetch_rows': self.oracle_prefetch_rows,
                'connection_pool_size': self.pool.max
            },
            'parquet_settings': {
                'compression': self.parquet_compression,
                'compression_level': self.parquet_compression_level,
                'row_group_size': self.parquet_row_group_size,
                'dictionary_columns': self.parquet_use_dictionary,
                'write_statistics': self.parquet_write_statistics,
                'write_page_index': self.parquet_write_page_index
            },
            'parallel_stats': self.parallel_stats,
            'gc_stats_summary': {
                'total_collections': self.gc_stats.get('collection_count', 0),
                'total_gc_time': self.gc_stats.get('total_time', 0),
                'total_collected': self.gc_stats.get('total_collected', 0)
            } if self.monitor_gc and hasattr(self, 'gc_stats') else None,
            'partitions': completed_partitions
        }
        
        metadata_path = os.path.join(
            self.data_lake_path,
            self.output_table_name,
            f'_metadata_{extraction_id}.json'
        )
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up ETL resources...")
        
        # Shutdown async writer
        if hasattr(self, 'async_writer'):
            self.async_writer.shutdown_writer()
        
        # Shutdown extraction executor
        if hasattr(self, 'extraction_executor'):
            self.extraction_executor.shutdown(wait=True)
        
        # Restore GC settings
        if hasattr(self, 'original_gc_thresholds'):
            gc.set_threshold(*self.original_gc_thresholds)
            logger.info("Restored original GC thresholds")
        
        # Log GC statistics
        if self.monitor_gc and hasattr(self, 'gc_stats'):
            self._log_gc_summary()
        
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Close connection pool
        if hasattr(self, 'pool'):
            self.pool.close()
    
    def log_memory_profile(self, context: str = ""):
        """Log detailed memory profile"""
        mem = psutil.virtual_memory()
        process = psutil.Process()
        mem_info = process.memory_info()
        
        logger.info(f"{'='*50}")
    
    def _log_gc_summary(self):
        """Log summary of GC activity"""
        if not self.gc_stats['collection_count']:
            return
        
        logger.info("="*60)
        logger.info("Garbage Collection Summary:")
        logger.info(f"Total collections: {self.gc_stats['collection_count']}")
        logger.info(f"Total objects collected: {self.gc_stats['total_collected']:,}")
        logger.info(f"Total uncollectable: {self.gc_stats['total_uncollectable']}")
        logger.info(f"Total GC time: {self.gc_stats['total_time']:.3f}s")
        
        if self.gc_stats['collection_count'] > 0:
            avg_time = self.gc_stats['total_time'] / self.gc_stats['collection_count']
            logger.info(f"Average collection time: {avg_time:.3f}s")
        
        logger.info("="*60)
        logger.info(f"Memory Profile {context}:")
        logger.info(f"System Memory: {mem.percent:.1f}% used ({mem.used / (1024**3):.1f}GB / {mem.total / (1024**3):.1f}GB)")
        logger.info(f"Available: {mem.available / (1024**3):.1f}GB")
        logger.info(f"Process RSS: {mem_info.rss / (1024**2):.1f}MB")
        logger.info(f"Process VMS: {mem_info.vms / (1024**2):.1f}MB")
        
        # Worker memory usage
        if hasattr(self, 'worker_memory') and self.worker_memory:
            with self.worker_memory_lock:
                total_worker_mem = sum(self.worker_memory.values())
                logger.info(f"Active workers: {len(self.worker_memory)}")
                logger.info(f"Total worker memory: {total_worker_mem:.1f}MB")
                for worker, mem_mb in self.worker_memory.items():
                    logger.info(f"  {worker}: {mem_mb:.1f}MB")
        
        # Async writer queue
        if hasattr(self, 'async_writer'):
            logger.info(f"Write queue size: {self.async_writer.write_queue.qsize()}")
            logger.info(f"Write queue memory: {self.async_writer.queue_memory_usage:.2f}GB")
        
        logger.info(f"{'='*50}")
        """Log summary of GC activity"""
        if not self.gc_stats['collection_count']:
            return
        
        logger.info("="*60)
        logger.info("Garbage Collection Summary:")
        logger.info(f"Total collections: {self.gc_stats['collection_count']}")
        logger.info(f"Total objects collected: {self.gc_stats['total_collected']:,}")
        logger.info(f"Total uncollectable: {self.gc_stats['total_uncollectable']}")
        logger.info(f"Total GC time: {self.gc_stats['total_time']:.3f}s")
        
        if self.gc_stats['collection_count'] > 0:
            avg_time = self.gc_stats['total_time'] / self.gc_stats['collection_count']
            logger.info(f"Average collection time: {avg_time:.3f}s")
        
        logger.info("="*60)
    
    # Include other required methods from original script
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
        """Simple memory check for backward compatibility"""
        is_ok, mem_percent = self.check_memory_with_threshold()
        return is_ok
    
    def _get_parquet_compression_args(self) -> Dict[str, Any]:
        """Get compression arguments for Parquet writer"""
        writer_args = {
            'compression': self.parquet_compression,
            'use_dictionary': self.parquet_use_dictionary,
            'write_statistics': self.parquet_write_statistics,
        }
        
        if self.parquet_compression in ['zstd', 'gzip', 'brotli']:
            writer_args['compression_level'] = self.parquet_compression_level
        
        try:
            import pyarrow
            major_version = int(pyarrow.__version__.split('.')[0])
            if major_version >= 6 and self.parquet_write_page_index:
                writer_args['write_page_index'] = True
        except:
            pass
        
        return writer_args
    
    def _clean_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric data to prevent PyArrow conversion errors"""
        for col in df.columns:
            if df[col].dtype in ['float64', 'float32', 'float16', 'int64', 'int32', 'int16', 'int8']:
                if df[col].dtype.name.startswith('float'):
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            
            elif df[col].dtype == 'object':
                sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                
                if sample_val is not None:
                    try:
                        float(str(sample_val))
                        logger.debug(f"Converting object column {col} to numeric")
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    except (ValueError, TypeError):
                        pass
        
        return df
    
    def _optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage"""
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type.name == 'category':
                df[col] = df[col].astype(str)
                continue
            
            if col_type != 'object':
                if col_type.name.startswith('int'):
                    has_data = df[col].notna().any()
                    
                    if not has_data:
                        continue
                    
                    if df[col].isnull().any():
                        c_min = df[col].min()
                        c_max = df[col].max()
                        if pd.isna(c_min) or pd.isna(c_max):
                            continue
                        
                        if c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                            df[col] = df[col].astype('Int16')
                        elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                            df[col] = df[col].astype('Int32')
                        else:
                            df[col] = df[col].astype('Int64')
                    else:
                        c_min = df[col].min()
                        c_max = df[col].max()
                        if c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                            df[col] = df[col].astype(np.int16)
                        elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                            df[col] = df[col].astype(np.int32)
                        else:
                            df[col] = df[col].astype(np.int64)
                
                elif col_type.name.startswith('float'):
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    
                    if not df[col].notna().any():
                        continue
                    
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            else:
                if df[col].notna().any():
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace(['nan', 'None', '<NA>'], np.nan)
        
        return df
    
    def _create_consistent_schema(self, df: pd.DataFrame) -> pa.Schema:
        """Create a PyArrow schema that will remain consistent across batches"""
        schema_fields = []
        
        for col in df.columns:
            dtype = df[col].dtype
            has_data = df[col].notna().any()
            
            if dtype == 'object' or dtype.name == 'category':
                schema_fields.append(pa.field(col, pa.string()))
            elif dtype.name.startswith('int') or dtype.name.startswith('Int'):
                if not has_data:
                    schema_fields.append(pa.field(col, pa.int32()))
                elif dtype.name in ['int8', 'Int8']:
                    schema_fields.append(pa.field(col, pa.int16()))
                elif dtype.name in ['int16', 'Int16']:
                    schema_fields.append(pa.field(col, pa.int16()))
                elif dtype.name in ['int32', 'Int32']:
                    schema_fields.append(pa.field(col, pa.int32()))
                else:
                    schema_fields.append(pa.field(col, pa.int64()))
            elif dtype.name.startswith('float'):
                schema_fields.append(pa.field(col, pa.float64()))
            elif dtype.name == 'datetime64[ns]':
                schema_fields.append(pa.field(col, pa.timestamp('ns')))
            else:
                schema_fields.append(pa.field(col, pa.string()))
        
        logger.info(f"Created schema with {len(schema_fields)} fields")
        return pa.schema(schema_fields)
    
    def _apply_consistent_schema(self, df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        """Apply consistent schema to DataFrame"""
        for field in schema:
            col_name = field.name
            if col_name not in df.columns:
                continue
            
            current_dtype = df[col_name].dtype
            
            if current_dtype.name == 'category':
                df[col_name] = df[col_name].astype(str)
                current_dtype = df[col_name].dtype
            
            if pa.types.is_integer(field.type):
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                
                if df[col_name].isnull().any():
                    if pa.types.is_int16(field.type):
                        df[col_name] = df[col_name].astype('Int16')
                    elif pa.types.is_int32(field.type):
                        df[col_name] = df[col_name].astype('Int32')
                    elif pa.types.is_int64(field.type):
                        df[col_name] = df[col_name].astype('Int64')
                    else:
                        df[col_name] = df[col_name].astype('Int32')
                else:
                    if pa.types.is_int16(field.type):
                        df[col_name] = df[col_name].astype(np.int16)
                    elif pa.types.is_int32(field.type):
                        df[col_name] = df[col_name].astype(np.int32)
                    elif pa.types.is_int64(field.type):
                        df[col_name] = df[col_name].astype(np.int64)
                    else:
                        df[col_name] = df[col_name].astype(np.int32)
                        
            elif pa.types.is_floating(field.type):
                df[col_name] = df[col_name].replace([np.inf, -np.inf], np.nan)
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                df[col_name] = df[col_name].astype(np.float64)
                        
            elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                if current_dtype != 'object':
                    df[col_name] = df[col_name].astype(str)
                
                df[col_name] = df[col_name].replace(['nan', 'None', '<NA>', ''], np.nan)
                        
            elif pa.types.is_timestamp(field.type):
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
        
        return df
    
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
        """Execute the optimized ETL process"""
        # Use the enhanced parallel execution
        self.run_parallel()

def main():
    """Main execution function with memory-aware configuration"""
    
    # Memory-aware configuration for 16GB RAM constraint
    config = {
        # Oracle connection using SID
        'oracle_user': os.environ.get('ORACLE_USER', 'your_username'),
        'oracle_password': os.environ.get('ORACLE_PASSWORD', 'your_password'),
        'oracle_host': os.environ.get('ORACLE_HOST', 'hostname'),
        'oracle_port': os.environ.get('ORACLE_PORT', '1521'),
        'oracle_sid': os.environ.get('ORACLE_SID', 'ORCL'),
        
        # Output settings
        'output_table_name': 'joined_table1_table2_enhanced',
        'data_lake_path': '/mnt/data_lake/raw',
        'checkpoint_dir': './etl_checkpoints',
        'temp_dir': '/tmp/etl_temp',
        
        # Category configuration
        'categories_parquet_path': '/path/to/categories.parquet',
        
        # Date configuration
        'date_column': 'tb1.transaction_date',
        'start_date': '2024-01-01',
        'end_date': '2024-01-07',
        
        # Category exclusions
        'excluded_categories': ['cat1', 'cat2'],
        
        # Table 2 columns
        'table2_columns': [
            'id',
            '999_categories',
            'col1', 'col2', 'col3', 'col4', 'col5',
            'col6', 'col7', 'col8', 'col9', 'col10',
            'col11', 'col12', 'col13', 'col14', 'col15'
        ],
        
        # MEMORY-AWARE PERFORMANCE SETTINGS
        'chunk_size': 50000,               # Conservative chunk size
        'max_workers': 2,                  # Limited parallel workers
        'parallel_degree': 8,              # Oracle parallel degree
        'parallel_degree_range': (4, 16),
        
        # Memory management
        'memory_limit_percent': 70,        # Start throttling at 70%
        'critical_memory_percent': 80,     # Critical threshold
        'enable_parallel_monitoring': True,
        'monitor_gc': True,
        
        # Network optimization (still important)
        'oracle_fetch_size': 5000,         # Reduced for memory
        'oracle_prefetch_rows': 10000,     # Reduced for memory
        'use_connection_multiplexing': True,
        
        # Parquet optimization
        'parquet_compression': 'zstd',
        'parquet_compression_level': 6,
        'parquet_row_group_size': 671000,  # From your logs
        'parquet_use_dictionary': ['999_categories'],
        'parquet_write_statistics': True,
        'parquet_write_page_index': True,
        
        # Row size estimate for adaptive sizing
        'estimated_row_size_bytes': 200,
    }
    
    logger.info("="*60)
    logger.info("ETL Configuration Summary:")
    logger.info(f"Base chunk size: {config['chunk_size']:,} rows")
    logger.info(f"Max parallel workers: {config['max_workers']}")
    logger.info(f"Memory limit: {config['memory_limit_percent']}%")
    logger.info(f"Critical memory: {config['critical_memory_percent']}%")
    logger.info("="*60)
    
    # Monitor initial memory
    mem = psutil.virtual_memory()
    logger.info(f"Starting memory: {mem.percent:.1f}% used ({mem.used / (1024**3):.1f}GB / {mem.total / (1024**3):.1f}GB)")
    
    if mem.percent > 50:
        logger.warning("System memory usage is already above 50%, consider closing other applications")
    
    # Create and run enhanced ETL
    etl = OptimizedOracleJoinETL(config)
    
    try:
        etl.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted - checkpoints saved")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        logger.info("Checkpoints saved for resume")
        sys.exit(1)


if __name__ == "__main__":
    main()
