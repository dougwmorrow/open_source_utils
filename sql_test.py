#!/usr/bin/env python3
"""
Optimized Oracle to Parquet ETL Script for Billion-Row JOIN Operations
Version 2.0 - Enhanced with all performance optimizations
Designed for cross-server Oracle connectivity with 16GB RAM constraint
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
import glob
import cProfile
import pstats
from io import StringIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('oracle_etl_optimized_v2.log'),
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
    rowid_range: Optional[Tuple[str, str]] = None  # NEW: ROWID range support
    last_id: Optional[int] = None

class MemoryPool:
    """Reusable memory pool for DataFrames to reduce allocation overhead"""
    def __init__(self, pool_size: int = 10):
        self.pool = []
        self.pool_size = pool_size
        self.lock = threading.Lock()
        self.stats = {'hits': 0, 'misses': 0}
    
    def get_buffer(self, size: int, dtype=object) -> np.ndarray:
        """Get a reusable buffer"""
        with self.lock:
            for i, (buf_size, buf_dtype, buf) in enumerate(self.pool):
                if buf_size >= size and buf_dtype == dtype:
                    self.pool.pop(i)
                    self.stats['hits'] += 1
                    return buf[:size]
            
            # Create new buffer
            self.stats['misses'] += 1
            return np.empty(size, dtype=dtype)
    
    def return_buffer(self, buf: np.ndarray):
        """Return buffer to pool"""
        with self.lock:
            if len(self.pool) < self.pool_size:
                self.pool.append((len(buf), buf.dtype, buf))
    
    def get_stats(self) -> Dict[str, int]:
        """Get pool statistics"""
        return self.stats.copy()

class AsyncParquetWriter:
    """Enhanced asynchronous Parquet writer with parallel writing support"""
    def __init__(self, compression_args: Dict[str, Any], max_writers: int = 4, use_direct_io: bool = False):
        self.compression_args = compression_args
        self.max_writers = max_writers
        self.use_direct_io = use_direct_io
        
        # Create multiple write queues for parallel writing
        self.write_queues = [Queue(maxsize=5) for _ in range(max_writers)]
        self.writer_threads = []
        self.shutdown = False
        self.write_stats = {'files_written': 0, 'bytes_written': 0, 'write_time': 0.0}
        
    def start(self):
        """Start the async writer threads"""
        for i in range(self.max_writers):
            thread = threading.Thread(
                target=self._writer_loop, 
                args=(i, self.write_queues[i]),
                daemon=True
            )
            thread.start()
            self.writer_threads.append(thread)
        logger.info(f"Started {self.max_writers} async Parquet writer threads")
        
    def _writer_loop(self, writer_id: int, write_queue: Queue):
        """Main writer loop for each thread"""
        while not self.shutdown:
            try:
                task = write_queue.get(timeout=1)
                if task is None:
                    break
                    
                path, df, schema = task
                start_time = time.time()
                self._write_to_parquet(path, df, schema)
                write_time = time.time() - start_time
                
                # Update stats
                self.write_stats['files_written'] += 1
                self.write_stats['bytes_written'] += os.path.getsize(path)
                self.write_stats['write_time'] += write_time
                
                write_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in writer thread {writer_id}: {str(e)}")
    
    def _write_to_parquet(self, path: str, df: pd.DataFrame, schema: pa.Schema):
        """Write DataFrame to Parquet with optimal settings"""
        try:
            table = pa.Table.from_pandas(df, schema=schema)
            
            if self.use_direct_io and sys.platform == 'linux':
                # Use O_DIRECT for Linux (bypass OS cache)
                self._write_direct_io(path, table)
            else:
                # Standard write
                pq.write_table(
                    table,
                    path,
                    row_group_size=len(df),
                    **self.compression_args
                )
            
            logger.debug(f"Wrote {len(df):,} rows to {path}")
            
        except Exception as e:
            logger.error(f"Error writing to {path}: {str(e)}")
            raise
    
    def _write_direct_io(self, path: str, table: pa.Table):
        """Write with O_DIRECT flag on Linux"""
        import fcntl
        
        # Create directory if needed
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Open with O_DIRECT
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_DIRECT, 0o644)
        
        try:
            # Write using file descriptor
            with os.fdopen(fd, 'wb', buffering=0) as f:
                pq.write_table(table, f, **self.compression_args)
        except:
            os.close(fd)
            raise
    
    def submit(self, path: str, df: pd.DataFrame, schema: pa.Schema):
        """Submit write task to least loaded queue"""
        # Find queue with minimum size
        min_size = float('inf')
        selected_queue = None
        
        for queue in self.write_queues:
            size = queue.qsize()
            if size < min_size:
                min_size = size
                selected_queue = queue
        
        selected_queue.put((path, df, schema))
    
    def shutdown_writer(self):
        """Shutdown all writer threads"""
        self.shutdown = True
        
        # Send shutdown signal to all queues
        for queue in self.write_queues:
            queue.put(None)
        
        # Wait for all threads
        for thread in self.writer_threads:
            thread.join()
        
        logger.info(f"Shut down async writer threads. Stats: {self.write_stats}")

class OptimizedOracleJoinETL:
    def __init__(self, config: Dict[str, Any]):
        """Initialize optimized ETL processor with enhanced settings"""
        self.config = config
        
        # Performance settings
        self.chunk_size = config.get('chunk_size', 200000)
        self.max_workers = config.get('max_workers', 6)
        
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
        self.parallel_degree = config.get('parallel_degree', 16)
        self.memory_limit_percent = config.get('memory_limit_percent', 75)
        self.enable_parallel_monitoring = config.get('enable_parallel_monitoring', True)
        
        # Enhanced network optimization settings
        self.oracle_fetch_size = config.get('oracle_fetch_size', 50000)
        self.oracle_prefetch_rows = config.get('oracle_prefetch_rows', 100000)
        self.use_network_compression = config.get('use_network_compression', True)
        self.network_buffer_size = config.get('network_buffer_size', 1048576)
        
        # ROWID splitting
        self.use_rowid_splitting = config.get('use_rowid_splitting', True)
        
        # Parquet compression settings
        self.parquet_compression = config.get('parquet_compression', 'snappy')
        self.parquet_compression_level = config.get('parquet_compression_level', None)
        self.parquet_row_group_size = config.get('parquet_row_group_size', 1000000)
        self.parquet_use_dictionary = config.get('parquet_use_dictionary', ['999_categories'])
        self.parquet_write_statistics = config.get('parquet_write_statistics', True)
        self.parquet_write_page_index = config.get('parquet_write_page_index', True)
        
        # Direct I/O
        self.use_direct_io = config.get('use_direct_io', sys.platform == 'linux')
        
        # Number of parallel Parquet writers
        self.parallel_parquet_writers = config.get('parallel_parquet_writers', 4)
        
        logger.info(f"Using row group size: {self.parquet_row_group_size:,} rows")
        logger.info(f"Using compression: {self.parquet_compression}")
        
        # GC monitoring flag
        self.monitor_gc = config.get('monitor_gc', True)
        
        # Initialize Oracle client if needed (for thick mode)
        if 'oracle_lib_dir' in config:
            oracledb.init_oracle_client(lib_dir=config['oracle_lib_dir'])
            logger.info(f"Initialized Oracle client from {config['oracle_lib_dir']}")
        
        # Set TNS_ADMIN if sqlnet.ora directory is provided
        if 'tns_admin' in config:
            os.environ['TNS_ADMIN'] = config['tns_admin']
            logger.info(f"Set TNS_ADMIN to {config['tns_admin']}")
        
        # Initialize Oracle connection with enhanced settings
        oracle_host = config['oracle_host']
        oracle_port = config['oracle_port']
        oracle_sid = config['oracle_sid']
        
        logger.info(f"Connecting to Oracle: {oracle_host}:{oracle_port}/{oracle_sid} with enhanced network settings")
        
        # Try multiple connection methods
        self.pool = None
        connection_methods = [
            self._create_pool_with_params,
            self._create_pool_with_connect_string,
            self._create_pool_with_dsn
        ]
        
        for method in connection_methods:
            try:
                self.pool = method(config)
                if self.pool:
                    logger.info(f"Successfully created connection pool using {method.__name__}")
                    break
            except Exception as e:
                logger.warning(f"Failed to create pool with {method.__name__}: {str(e)}")
        
        if not self.pool:
            raise Exception("Failed to create connection pool with any method")
        
        # Test the pool
        try:
            test_conn = self.acquire_connection_with_retry()
            self.pool.release(test_conn)
            logger.info("Connection pool test successful")
        except Exception as e:
            logger.error(f"Connection pool test failed: {str(e)}")
            raise
        
        # Create directories
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Category optimization: Load categories from parquet file
        self.valid_categories = None
        self._initialize_categories_from_parquet_optimized()
        
        # Initialize memory pool
        self.memory_pool = MemoryPool(pool_size=20)
        
        # Initialize parallel monitoring
        self.parallel_stats = {
            'queries_executed': 0,
            'parallel_executions': 0,
            'avg_parallel_degree': 0,
            'max_parallel_degree': 0,
            'network_latency_ms': 0,
            'compression_ratio': 0,
            'connection_retries': 0,
            'connection_failures': 0
        }
        
        # Initialize async writer
        compression_args = self._get_parquet_compression_args()
        self.async_writer = AsyncParquetWriter(
            compression_args, 
            max_writers=self.parallel_parquet_writers,
            use_direct_io=self.use_direct_io
        )
        self.async_writer.start()
        
        # Initialize extraction executor
        self.extraction_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix='oracle_extract'
        )
        
        # Optimize GC settings
        self._optimize_gc_settings()
        
        # Set up GC monitoring
        if self.monitor_gc:
            self._setup_gc_monitoring()
        
        # Monitor initial network latency
        self._monitor_network_latency()
        
        # Start pool monitoring thread
        self.pool_monitor_thread = threading.Thread(
            target=self._pool_monitor_loop,
            daemon=True
        )
        self.pool_monitor_thread.start()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown = False
        
        # Log configuration
        logger.info("="*60)
        logger.info("ETL Configuration:")
        logger.info(f"Chunk size: {self.chunk_size:,} rows")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Connection pool size: {self.pool.min}-{self.pool.max}")
        logger.info(f"Oracle fetch size: {self.oracle_fetch_size:,}")
        logger.info(f"Oracle prefetch rows: {self.oracle_prefetch_rows:,}")
        logger.info(f"Network compression: {self.use_network_compression}")
        logger.info(f"ROWID splitting: {self.use_rowid_splitting}")
        logger.info(f"Parquet compression: {self.parquet_compression}")
        logger.info(f"Row group size: {self.parquet_row_group_size:,} rows")
        logger.info(f"Direct I/O: {self.use_direct_io}")
        logger.info(f"Parallel Parquet writers: {self.parallel_parquet_writers}")
        logger.info("="*60)
    
    def _create_pool_with_params(self, config: Dict[str, Any]) -> Any:
        """Create connection pool using ConnectParams (preferred method)"""
        # Create connection parameters with all optimizations
        conn_params = oracledb.ConnectParams(
            host=config['oracle_host'],
            port=config['oracle_port'],
            sid=config['oracle_sid'],
            tcp_connect_timeout=10.0,
            expire_time=5,
            sdu=65535,
            events=False,
            threaded=True
        )
        
        # Create pool with parameters
        return oracledb.create_pool(
            user=config['oracle_user'],
            password=config['oracle_password'],
            params=conn_params,
            min=2,
            max=max(self.max_workers * 2, 8),
            increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT,
            timeout=30,
            ping_interval=0,
            stmtcachesize=50,
            homogeneous=True
        )
    
    def _create_pool_with_connect_string(self, config: Dict[str, Any]) -> Any:
        """Create connection pool using Easy Connect string with parameters"""
        # Build connection string with network optimization parameters
        connect_string = self._build_optimized_connect_string(config)
        
        return oracledb.create_pool(
            user=config['oracle_user'],
            password=config['oracle_password'],
            dsn=connect_string,
            min=2,
            max=max(self.max_workers * 2, 8),
            increment=1,
            threaded=True,
            getmode=oracledb.POOL_GETMODE_WAIT,
            timeout=30,
            stmtcachesize=50,
            homogeneous=True
        )
    
    def _create_pool_with_dsn(self, config: Dict[str, Any]) -> Any:
        """Create connection pool using basic DSN (fallback method)"""
        # Create basic DSN
        dsn = oracledb.makedsn(
            host=config['oracle_host'],
            port=config['oracle_port'],
            sid=config['oracle_sid']
        )
        
        return oracledb.create_pool(
            user=config['oracle_user'],
            password=config['oracle_password'],
            dsn=dsn,
            min=2,
            max=max(self.max_workers * 2, 8),
            increment=1,
            threaded=True,
            getmode=oracledb.POOL_GETMODE_WAIT,
            timeout=30,
            stmtcachesize=50,
            homogeneous=True
        )
    
    def _build_optimized_connect_string(self, config: Dict[str, Any]) -> str:
        """Build an optimized connection string with network parameters"""
        host = config['oracle_host']
        port = config['oracle_port']
        sid = config['oracle_sid']
        
        # Build connection descriptor with optimizations
        connect_string = f"""(DESCRIPTION=
            (FAILOVER=ON)
            (CONNECT_TIMEOUT=10)
            (RETRY_COUNT=3)
            (RETRY_DELAY=1)
            (TRANSPORT_CONNECT_TIMEOUT=3)
            (ADDRESS_LIST=
                (ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port})(SDU=65535))
            )
            (CONNECT_DATA=
                (SID={sid})
                (SERVER=DEDICATED)
            )
        )"""
        
        # Remove extra whitespace
        connect_string = ' '.join(connect_string.split())
        
        return connect_string

    def acquire_connection_with_retry(self, max_retries: int = 3) -> Any:
        """Acquire connection with retry logic for network issues"""
        retry_delay = 1
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # Acquire connection from pool
                connection = self.pool.acquire()
                
                # Test connection health
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                cursor.close()
                
                # Configure session optimizations on first successful connection
                self.configure_session_optimizations(connection)
                
                # Success
                return connection
                
            except oracledb.Error as e:
                last_error = e
                self.parallel_stats['connection_retries'] += 1
                
                # Extract error code
                error_code = 0
                if hasattr(e, 'args') and e.args:
                    if hasattr(e.args[0], 'code'):
                        error_code = e.args[0].code
                    elif isinstance(e.args[0], int):
                        error_code = e.args[0]
                
                # Check for recoverable errors
                recoverable_errors = [
                    3113,   # ORA-03113: end-of-file on communication channel
                    3114,   # ORA-03114: not connected to ORACLE
                    3135,   # ORA-03135: connection lost contact
                    12170,  # ORA-12170: TNS:Connect timeout occurred
                    12571,  # ORA-12571: TNS:packet writer failure
                    12560,  # ORA-12560: TNS:protocol adapter error
                    12537,  # ORA-12537: TNS:connection closed
                    12541,  # ORA-12541: TNS:no listener
                    12543,  # ORA-12543: TNS:destination host unreachable
                    12152,  # ORA-12152: TNS:unable to send break message
                    1033,   # ORA-01033: ORACLE initialization or shutdown in progress
                    1034,   # ORA-01034: ORACLE not available
                    1089,   # ORA-01089: immediate shutdown in progress
                    28,     # ORA-00028: your session has been killed
                    31,     # ORA-00031: session marked for kill
                ]
                
                if error_code in recoverable_errors and attempt < max_retries - 1:
                    logger.warning(
                        f"Connection error (attempt {attempt + 1}/{max_retries}): "
                        f"ORA-{error_code:05d} - {str(e)}. Retrying in {retry_delay}s..."
                    )
                    
                    # Release the bad connection if we got one
                    try:
                        if 'connection' in locals():
                            self.pool.release(connection)
                    except:
                        pass
                    
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 10)  # Exponential backoff with cap
                    
                    # Try to recover the pool
                    try:
                        self.pool.reconfigure(min=self.pool.min, max=self.pool.max)
                    except:
                        pass
                else:
                    self.parallel_stats['connection_failures'] += 1
                    raise
            
            except Exception as e:
                last_error = e
                self.parallel_stats['connection_failures'] += 1
                logger.error(f"Unexpected error acquiring connection: {str(e)}")
                raise
        
        # All retries exhausted
        self.parallel_stats['connection_failures'] += 1
        raise last_error or Exception("Failed to acquire connection after all retries")

    def configure_session_optimizations(self, connection):
        """Configure comprehensive session-level optimizations"""
        try:
            cursor = connection.cursor()
            
            # Core performance optimizations
            optimization_statements = [
                # Memory and processing
                ("ALTER SESSION SET workarea_size_policy = 'AUTO'", "workarea sizing"),
                ("ALTER SESSION SET SORT_AREA_SIZE = 104857600", "sort area (100MB)"),
                ("ALTER SESSION SET HASH_AREA_SIZE = 104857600", "hash area (100MB)"),
                
                # Parallel execution
                ("ALTER SESSION ENABLE PARALLEL DML", "parallel DML"),
                (f"ALTER SESSION FORCE PARALLEL QUERY PARALLEL {self.parallel_degree}", "parallel query"),
                ("ALTER SESSION SET parallel_degree_policy = 'MANUAL'", "parallel degree policy"),
                
                # Network optimizations
                ("ALTER SESSION SET SDU_SIZE = 65535", "SDU size"),
                ("ALTER SESSION SET TDU_SIZE = 65535", "TDU size"),
                
                # Cursor and fetch optimizations
                ("ALTER SESSION SET CURSOR_SHARING = FORCE", "cursor sharing"),
                ("ALTER SESSION SET \"_serial_direct_read\" = TRUE", "serial direct read"),
                
                # Statistics and monitoring
                ("ALTER SESSION SET STATISTICS_LEVEL = TYPICAL", "statistics level"),
                ("ALTER SESSION SET SQL_TRACE = FALSE", "SQL trace off"),
                ("ALTER SESSION SET TIMED_STATISTICS = FALSE", "timed statistics off"),
                
                # Read optimizations
                ("ALTER SESSION SET DB_FILE_MULTIBLOCK_READ_COUNT = 128", "multiblock read"),
                ("ALTER SESSION SET \"_db_file_noncontig_mblock_read_count\" = 32", "non-contiguous read"),
                
                # Join and hash optimizations
                ("ALTER SESSION SET \"_hash_join_enabled\" = TRUE", "hash join"),
                ("ALTER SESSION SET \"_optimizer_use_feedback\" = FALSE", "optimizer feedback off"),
                ("ALTER SESSION SET \"_px_max_message_pool_pct\" = 80", "PX message pool"),
                ("ALTER SESSION SET \"_px_pwmr_enabled\" = FALSE", "PX PWMR off"),
                ("ALTER SESSION SET \"_parallel_broadcast_enabled\" = TRUE", "parallel broadcast"),
                ("ALTER SESSION SET \"_optimizer_batch_table_access_by_rowid\" = TRUE", "batch ROWID access"),
                
                # Timeout settings
                ("ALTER SESSION SET MAX_IDLE_TIME = 0", "no idle timeout"),
                ("ALTER SESSION SET MAX_IDLE_BLOCKER_TIME = 0", "no blocker timeout"),
            ]
            
            # Network compression (separate block for error handling)
            if self.use_network_compression:
                compression_statements = [
                    ("ALTER SESSION SET SQLNET.COMPRESSION = 'ON'", "SQL*Net compression"),
                    ("ALTER SESSION SET SQLNET.COMPRESSION_LEVELS = '(HIGH)'", "compression level"),
                    ("ALTER SESSION SET COMPRESSION_LEVEL = 'HIGH'", "general compression"),
                ]
                optimization_statements.extend(compression_statements)
            
            # Apply optimizations
            successful_opts = []
            failed_opts = []
            
            for statement, description in optimization_statements:
                try:
                    cursor.execute(statement)
                    successful_opts.append(description)
                except oracledb.DatabaseError as e:
                    error_code = e.args[0].code if hasattr(e.args[0], 'code') else 0
                    # Only log debug for expected failures (feature not available)
                    if error_code in [2248, 1031, 15000, 2097, 3113]:
                        logger.debug(f"Optional feature not available: {description}")
                    else:
                        failed_opts.append(f"{description} (ORA-{error_code:05d})")
            
            # NLS settings for consistency
            nls_statements = [
                "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'",
                "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
                "ALTER SESSION SET TIME_ZONE = 'UTC'"
            ]
            
            for statement in nls_statements:
                try:
                    cursor.execute(statement)
                except:
                    pass
            
            cursor.close()
            
            # Log results
            if successful_opts:
                logger.info(f"Applied {len(successful_opts)} session optimizations")
                logger.debug(f"Successful: {', '.join(successful_opts)}")
            
            if failed_opts:
                logger.debug(f"Failed optimizations: {', '.join(failed_opts)}")
            
        except Exception as e:
            logger.warning(f"Error configuring session optimizations: {str(e)}")
    
    def enable_parallel_session_enhanced(self, connection):
        """Enable parallel execution with enhanced settings and monitoring"""
        try:
            cursor = connection.cursor()
            
            # Core parallel settings
            parallel_statements = [
                # Enable parallel operations
                "ALTER SESSION ENABLE PARALLEL DML",
                "ALTER SESSION ENABLE PARALLEL DDL",
                f"ALTER SESSION FORCE PARALLEL QUERY PARALLEL {self.parallel_degree}",
                f"ALTER SESSION FORCE PARALLEL DML PARALLEL {self.parallel_degree}",
                
                # Parallel execution parameters
                "ALTER SESSION SET parallel_degree_policy = 'MANUAL'",
                "ALTER SESSION SET parallel_adaptive_multi_user = FALSE",
                "ALTER SESSION SET parallel_threads_per_cpu = 4",
                
                # Parallel optimization hints
                "ALTER SESSION SET \"_parallel_broadcast_enabled\" = TRUE",
                "ALTER SESSION SET \"_px_broadcast_fudge_factor\" = 100",
                "ALTER SESSION SET \"_px_chunk_size\" = 1000000",
                "ALTER SESSION SET \"_px_min_granules_per_slave\" = 4",
                "ALTER SESSION SET \"_px_max_granules_per_slave\" = 1000",
                "ALTER SESSION SET \"_px_chunklist_count_ratio\" = 1",
                
                # Memory management for parallel execution
                "ALTER SESSION SET \"_smm_px_max_size\" = 524288",  # 512MB per slave
                "ALTER SESSION SET \"_px_use_large_pool\" = TRUE",
                
                # Parallel join optimizations
                "ALTER SESSION SET \"_px_partial_rollup_pushdown\" = ADAPTIVE",
                "ALTER SESSION SET \"_px_filter_parallelized\" = TRUE",
                "ALTER SESSION SET \"_px_filter_skew_handling\" = TRUE",
                
                # Adaptive features for parallel
                "ALTER SESSION SET \"_optimizer_adaptive_plans\" = TRUE",
                "ALTER SESSION SET \"_optimizer_adaptive_statistics\" = TRUE",
                "ALTER SESSION SET \"_px_adaptive_dist_method\" = CHOOSE",
            ]
            
            # Execute parallel configuration
            for statement in parallel_statements:
                try:
                    cursor.execute(statement)
                except oracledb.DatabaseError as e:
                    error_code = e.args[0].code if hasattr(e.args[0], 'code') else 0
                    if error_code not in [2248, 1031, 15000]:  # Ignore "no such parameter" errors
                        logger.debug(f"Could not set parallel option: {statement} - ORA-{error_code:05d}")
            
            # Verify parallel configuration
            cursor.execute("""
                SELECT name, value 
                FROM v$parameter 
                WHERE name LIKE '%parallel%' 
                AND ismodified = 'MODIFIED'
            """)
            
            modified_params = cursor.fetchall()
            if modified_params:
                logger.info(f"Modified {len(modified_params)} parallel parameters")
                for name, value in modified_params[:5]:  # Log first 5
                    logger.debug(f"  {name} = {value}")
            
            # Enable network compression for parallel operations
            self.enable_network_compression(connection)
            
            cursor.close()
            logger.info(f"Enabled enhanced parallel execution with degree {self.parallel_degree}")
            
        except Exception as e:
            logger.warning(f"Could not fully enable parallel execution: {str(e)}")
    
    def enable_network_compression(self, connection):
        """Enable Oracle network compression for reduced transfer size"""
        if not self.use_network_compression:
            return
            
        try:
            cursor = connection.cursor()
            
            compression_statements = [
                # SQL*Net compression
                "ALTER SESSION SET SQLNET.COMPRESSION = 'ON'",
                "ALTER SESSION SET SQLNET.COMPRESSION_LEVELS = '(HIGH)'",
                "ALTER SESSION SET SQLNET.COMPRESSION_THRESHOLD = 1024",
                
                # Advanced compression for 12c+
                "ALTER SESSION SET COMPRESSION_LEVEL = 'HIGH'",
                
                # Table compression hints
                "ALTER SESSION SET \"_table_compress_blocks\" = 50",
                "ALTER SESSION SET \"_compression_compatibility\" = '12.2.0'",
            ]
            
            applied = 0
            for statement in compression_statements:
                try:
                    cursor.execute(statement)
                    applied += 1
                except:
                    pass
            
            cursor.close()
            
            if applied > 0:
                logger.info(f"Enabled Oracle network compression ({applied} settings applied)")
            
        except Exception as e:
            logger.debug(f"Could not enable all compression features: {str(e)}")
    
    def monitor_connection_pool(self):
        """Monitor connection pool health and performance"""
        try:
            stats = {
                'opened': self.pool.opened,
                'busy': self.pool.busy,
                'timeout': self.pool.timeout,
                'max': self.pool.max,
                'min': self.pool.min,
                'increment': self.pool.increment,
                'homogeneous': self.pool.homogeneous,
                'ping_interval': self.pool.ping_interval,
                'getmode': self.pool.getmode,
                'stmtcachesize': self.pool.stmtcachesize
            }
            
            # Calculate pool utilization
            utilization = (self.pool.busy / self.pool.max * 100) if self.pool.max > 0 else 0
            stats['utilization_percent'] = utilization
            
            # Log if high utilization
            if utilization > 80:
                logger.warning(
                    f"High connection pool utilization: {self.pool.busy}/{self.pool.max} "
                    f"({utilization:.1f}%) connections busy"
                )
                
                # Auto-scale pool if possible
                if self.pool.max < 20:  # Cap at 20 connections
                    new_max = min(self.pool.max + 2, 20)
                    try:
                        self.pool.reconfigure(max=new_max)
                        logger.info(f"Auto-scaled connection pool max from {self.pool.max} to {new_max}")
                    except Exception as e:
                        logger.error(f"Failed to auto-scale pool: {str(e)}")
            
            # Check for connection starvation
            if self.pool.busy == self.pool.max and self.pool.opened == self.pool.max:
                logger.error("Connection pool exhausted - all connections in use!")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error monitoring pool: {str(e)}")
            return {}
    
    def _pool_monitor_loop(self):
        """Background thread to monitor connection pool health"""
        monitor_interval = 30  # seconds
        
        while not self.shutdown:
            try:
                time.sleep(monitor_interval)
                
                if not self.shutdown:
                    stats = self.monitor_connection_pool()
                    
                    # Log stats periodically
                    if stats.get('utilization_percent', 0) > 50:
                        logger.info(
                            f"Pool stats: {stats['busy']}/{stats['max']} busy "
                            f"({stats['utilization_percent']:.1f}% utilization)"
                        )
                
            except Exception as e:
                logger.error(f"Error in pool monitor thread: {str(e)}")
    
    def _initialize_categories_from_parquet_optimized(self):
        """Load categories efficiently using PyArrow"""
        try:
            if not os.path.exists(self.categories_parquet_path):
                raise FileNotFoundError(f"Categories parquet file not found: {self.categories_parquet_path}")
            
            logger.info(f"Loading categories from parquet file: {self.categories_parquet_path}")
            
            # Use PyArrow for faster loading
            table = pq.read_table(
                self.categories_parquet_path,
                columns=['999_distinct values'],
                use_threads=True
            )
            
            # Convert to set efficiently
            category_array = table.column('999_distinct values')
            self.valid_categories = set(
                val.as_py() for val in category_array if val.is_valid
            ) - self.excluded_categories
            
            logger.info(f"Loaded {len(self.valid_categories)} valid categories")
            
            # Create optimized category list for Oracle
            self.oracle_category_list = list(self.valid_categories)
            
        except Exception as e:
            logger.error(f"Error loading categories from parquet file: {str(e)}")
            raise
    
    def _optimize_gc_settings(self):
        """Optimize GC for billion-row processing workload"""
        self.original_gc_thresholds = gc.get_threshold()
        logger.info(f"Original GC thresholds: {self.original_gc_thresholds}")
        
        # Increase thresholds for large data processing
        gc.set_threshold(700000, 20, 20)
        logger.info("Set GC threshold to (700000, 20, 20) for better performance")
        
        # Initial collection
        collected = gc.collect()
        logger.info(f"Initial GC collected {collected} objects")
        
        # Disable GC during critical sections
        gc.disable()
        logger.info("Disabled automatic GC - will handle manually")
    
    def _setup_gc_monitoring(self):
        """Set up GC monitoring callbacks"""
        self.gc_stats = {
            'collections': [],
            'total_collected': 0,
            'total_uncollectable': 0,
            'total_time': 0.0,
            'collection_count': 0
        }
        
        # Re-enable GC for monitoring
        gc.enable()
        
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
    
    def _monitor_network_latency(self):
        """Monitor network round-trip time to Oracle"""
        try:
            connection = self.pool.acquire()
            cursor = connection.cursor()
            
            # Measure round-trip time
            latencies = []
            for _ in range(5):
                start = time.time()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                latency = (time.time() - start) * 1000
                latencies.append(latency)
            
            avg_latency = sum(latencies) / len(latencies)
            self.parallel_stats['network_latency_ms'] = avg_latency
            
            cursor.close()
            self.pool.release(connection)
            
            logger.info(f"Network latency: {avg_latency:.2f}ms")
            
            # Adjust fetch size based on latency
            if avg_latency > 10:  # High latency
                self.oracle_fetch_size = min(self.oracle_fetch_size * 2, 100000)
                logger.info(f"High latency detected, increased fetch size to {self.oracle_fetch_size:,}")
                
        except Exception as e:
            logger.warning(f"Could not measure network latency: {str(e)}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Shutdown signal received, finishing current batches...")
        self.shutdown = True
    
    def get_rowid_ranges(self, connection, num_splits: int) -> List[Tuple[str, str]]:
        """Split table by ROWID for true parallel processing"""
        cursor = connection.cursor()
        ranges = []
        
        try:
            # Build base query for ROWID range detection
            base_where = "1=1"
            params = {}
            
            if self.date_column and self.start_date and self.end_date:
                base_where += f" AND {self.date_column} >= :start_date AND {self.date_column} < :end_date"
                params['start_date'] = datetime.strptime(self.start_date, '%Y-%m-%d')
                params['end_date'] = datetime.strptime(self.end_date, '%Y-%m-%d')
            
            # Get min/max ROWIDs
            query = f"""
            SELECT 
                MIN(ROWID) as min_rid,
                MAX(ROWID) as max_rid,
                COUNT(*) as cnt
            FROM table_1
            WHERE {base_where}
            """
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if not result or not result[0]:
                logger.warning("No data found for ROWID splitting")
                return [(None, None)]
            
            min_rid, max_rid, total_rows = result
            logger.info(f"ROWID range: {min_rid} to {max_rid}, {total_rows:,} rows")
            
            # Get ROWID boundaries using NTILE
            boundary_query = f"""
            WITH rowid_sample AS (
                SELECT ROWID as rid
                FROM (
                    SELECT ROWID,
                           ROW_NUMBER() OVER (ORDER BY ROWID) as rn,
                           COUNT(*) OVER () as total_cnt
                    FROM table_1
                    WHERE {base_where}
                )
                WHERE MOD(rn, GREATEST(1, TRUNC(total_cnt / {num_splits * 10}))) = 0
            )
            SELECT rid
            FROM (
                SELECT rid,
                       NTILE({num_splits - 1}) OVER (ORDER BY rid) as bucket
                FROM rowid_sample
            )
            WHERE bucket <= {num_splits - 1}
            GROUP BY bucket
            HAVING rid = MAX(rid)
            ORDER BY bucket
            """
            
            cursor.execute(boundary_query, params)
            boundaries = [row[0] for row in cursor.fetchall()]
            
            # Create ranges
            prev_rid = min_rid
            for boundary in boundaries:
                ranges.append((prev_rid, boundary))
                prev_rid = boundary
            ranges.append((prev_rid, max_rid))
            
            logger.info(f"Created {len(ranges)} ROWID ranges for parallel processing")
            
        except Exception as e:
            logger.error(f"Error creating ROWID ranges: {str(e)}")
            # Fallback to single range
            ranges = [(None, None)]
        finally:
            cursor.close()
        
        return ranges
    
    def build_optimized_query(self, date_range: Optional[Tuple[datetime, datetime]] = None,
                            last_id: Optional[int] = None,
                            partition_value: Optional[str] = None,
                            rowid_range: Optional[Tuple[str, str]] = None) -> Tuple[str, Dict]:
        """Build memory-optimized query with enhanced parallel processing"""
        params = {}
        
        # Enhanced hints for parallel processing
        parallel_hints = [
            f"PARALLEL(tb1, {self.parallel_degree})",
            f"PARALLEL(tb2, {self.parallel_degree})",
            "USE_HASH(tb1 tb2)",
            "PQ_DISTRIBUTE(tb2 HASH HASH)",
            "NO_PARALLEL_INDEX(tb1)",
            "NO_PARALLEL_INDEX(tb2)",
            f"FIRST_ROWS({self.chunk_size})",
            
            # Additional performance hints
            "OPT_PARAM('_parallel_broadcast_enabled' 'true')",
            "OPT_PARAM('_px_partition_scan_enabled' 'true')",
            "OPT_PARAM('_px_pwmr_enabled' 'false')",
            "OPT_PARAM('_optimizer_batch_table_access_by_rowid' 'true')",
            "MONITOR",
            "GATHER_PLAN_STATISTICS",
            "NO_MERGE",
            "PUSH_PRED",
            "DYNAMIC_SAMPLING(4)",
            "OPT_PARAM('_optim_peek_user_binds' 'false')",
            "OPT_PARAM('_db_file_multiblock_read_count' '128')",
            "OPT_PARAM('_sort_multiblock_read_count' '128')"
        ]
        
        # Add result cache for small lookups
        if len(self.valid_categories) < 100:
            parallel_hints.append("RESULT_CACHE")
        
        hints = f"/*+ {' '.join(parallel_hints)} */"
        
        # Build column list
        table2_column_list = []
        for col in self.table2_columns:
            table2_column_list.append(f'tb2."{col}" as "tb2_{col}"')
        
        table2_columns_str = ', '.join(table2_column_list)
        
        # Build query with optimized category filtering
        if len(self.valid_categories) < 10:
            # For small category sets, use WITH clause
            category_values = ','.join([f"'{cat}'" for cat in self.valid_categories])
            
            query = f"""
            WITH valid_cats AS (
                SELECT /*+ MATERIALIZE */ column_value as cat
                FROM TABLE(sys.odcivarchar2list({category_values}))
            )
            SELECT {hints}
                tb1.*,
                {table2_columns_str}
            FROM table_1 tb1
            JOIN table_2 tb2 ON tb1.id = tb2.id
            JOIN valid_cats vc ON tb2."999_categories" = vc.cat
            WHERE 1=1
            """
        else:
            # Standard query for larger category sets
            query = f"""
            SELECT {hints}
                tb1.*,
                {table2_columns_str}
            FROM table_1 tb1
            JOIN table_2 tb2 ON tb1.id = tb2.id
            WHERE 1=1
            """
            
            # Category filter
            if len(self.valid_categories) < 30:
                query += f' AND tb2."999_categories" IN ({",".join([f":cat_{i}" for i in range(len(self.valid_categories))])})'
                for i, cat in enumerate(self.valid_categories):
                    params[f'cat_{i}'] = cat
            elif self.excluded_categories:
                placeholders = ','.join([f':cat_{i}' for i in range(len(self.excluded_categories))])
                query += f' AND tb2."999_categories" NOT IN ({placeholders})'
                for i, cat in enumerate(self.excluded_categories):
                    params[f'cat_{i}'] = cat
        
        # Add ROWID range filter for parallel processing
        if rowid_range and rowid_range[0] is not None:
            query += " AND tb1.ROWID > :rowid_start AND tb1.ROWID <= :rowid_end"
            params['rowid_start'] = rowid_range[0]
            params['rowid_end'] = rowid_range[1]
        
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
        params['chunk_size'] = self.chunk_size
        
        return query, params
    
    def configure_array_interface(self, cursor, connection) -> int:
        """Configure optimal array sizes based on available memory and network latency"""
        # Get available memory
        mem = psutil.virtual_memory()
        available_mb = mem.available / (1024 * 1024)
        
        # Estimate memory per row
        bytes_per_row = self.config.get('estimated_row_size_bytes', 200)
        
        # Adjust for network latency
        latency_factor = 1.0
        if self.parallel_stats['network_latency_ms'] > 10:
            latency_factor = min(2.0, self.parallel_stats['network_latency_ms'] / 5)
        
        # Use 20% of available memory for array buffer
        buffer_memory_mb = available_mb * 0.20
        buffer_memory_bytes = buffer_memory_mb * 1024 * 1024
        
        # Calculate array size with latency adjustment
        optimal_array_size = int(buffer_memory_bytes / (bytes_per_row * 1.2) * latency_factor)
        
        # Set bounds
        optimal_array_size = max(10000, min(optimal_array_size, 200000))
        
        # Configure cursor
        cursor.arraysize = optimal_array_size
        cursor.prefetchrows = min(optimal_array_size * 2, 400000)
        
        # Set input sizes for better memory allocation
        cursor.setinputsizes(None, 4000)  # For VARCHAR2 columns
        
        logger.info(
            f"Set array size to {optimal_array_size:,} "
            f"(latency factor: {latency_factor:.1f}x, available memory: {available_mb:.0f}MB)"
        )
        
        return optimal_array_size
    
    def rows_to_dataframe_zero_copy(self, rows: List, columns: List) -> pd.DataFrame:
        """Convert rows to DataFrame with zero-copy optimization"""
        if not rows:
            return pd.DataFrame(columns=columns)
        
        n_rows = len(rows)
        n_cols = len(columns)
        
        # Pre-allocate arrays by column type
        col_arrays = []
        
        # Analyze first few rows to determine types
        sample_size = min(10, n_rows)
        col_types = []
        
        for col_idx in range(n_cols):
            # Sample values to determine type
            sample_values = [rows[i][col_idx] for i in range(sample_size) if rows[i][col_idx] is not None]
            
            if not sample_values:
                col_types.append('float')  # Default to float for null columns
            elif all(isinstance(v, (int, np.integer)) for v in sample_values):
                col_types.append('int')
            elif all(isinstance(v, (float, np.floating, int, np.integer)) for v in sample_values):
                col_types.append('float')
            else:
                col_types.append('object')
        
        # Create arrays with proper types
        for col_idx, col_type in enumerate(col_types):
            if col_type == 'int':
                # Check for nulls
                has_nulls = any(rows[i][col_idx] is None for i in range(n_rows))
                
                if has_nulls:
                    # Use float64 to handle NaN
                    arr = self.memory_pool.get_buffer(n_rows, dtype=np.float64)
                    for i in range(n_rows):
                        arr[i] = rows[i][col_idx] if rows[i][col_idx] is not None else np.nan
                    col_arrays.append(pd.array(arr[:n_rows], dtype="Int64"))
                else:
                    # Use native int
                    arr = self.memory_pool.get_buffer(n_rows, dtype=np.int64)
                    for i in range(n_rows):
                        arr[i] = rows[i][col_idx]
                    col_arrays.append(arr[:n_rows])
                    
            elif col_type == 'float':
                arr = self.memory_pool.get_buffer(n_rows, dtype=np.float64)
                for i in range(n_rows):
                    val = rows[i][col_idx]
                    arr[i] = val if val is not None else np.nan
                col_arrays.append(arr[:n_rows])
                
            else:
                # Object array for strings
                arr = self.memory_pool.get_buffer(n_rows, dtype=object)
                for i in range(n_rows):
                    arr[i] = rows[i][col_idx]
                col_arrays.append(arr[:n_rows])
        
        # Create DataFrame from pre-allocated arrays
        df_dict = {columns[i]: col_arrays[i] for i in range(n_cols)}
        df = pd.DataFrame(df_dict, copy=False)
        
        # Return buffers to pool
        for arr in col_arrays:
            if isinstance(arr, np.ndarray):
                self.memory_pool.return_buffer(arr)
        
        return df
        
    def stream_extract_to_parquet_enhanced(self, query: str, params: Dict, 
                                          output_path: str,
                                          task_id: str = "") -> Tuple[int, int]:
        """Enhanced streaming with all optimizations and better error handling"""
        connection = None
        rows_written = 0
        last_id = None
        schema = None
        
        try:
            # Acquire connection with retry logic
            connection = self.acquire_connection_with_retry()
            
            # Enable enhanced parallel execution
            self.enable_parallel_session_enhanced(connection)
            
            cursor = connection.cursor()
            
            # Configure optimal array interface
            array_size = self.configure_array_interface(cursor, connection)
            
            logger.debug(f"[{task_id}] Using array size: {array_size:,}")
            
            # Execute query with timeout handling
            start_time = time.time()
            
            try:
                cursor.execute(query, params)
            except oracledb.DatabaseError as e:
                error_code = e.args[0].code if hasattr(e.args[0], 'code') else 0
                if error_code == 1013:  # ORA-01013: user requested cancel of current operation
                    logger.warning(f"[{task_id}] Query cancelled by user")
                    return 0, None
                elif error_code == 1555:  # ORA-01555: snapshot too old
                    logger.error(f"[{task_id}] Snapshot too old - consider smaller chunks or UNDO tuning")
                    raise
                else:
                    raise
            
            execute_time = time.time() - start_time
            logger.info(f"[{task_id}] Query executed in {execute_time:.2f}s")
            
            # Monitor parallel execution
            if self.enable_parallel_monitoring:
                self.monitor_parallel_execution(connection)
            self.parallel_stats['queries_executed'] += 1
            
            # Get column info
            columns = [desc[0] for desc in cursor.description]
            
            # Batch processing with zero-copy
            batch_rows = []
            batch_size = min(20000, array_size)
            chunk_num = 0
            
            # Track performance metrics
            fetch_time = 0
            process_time = 0
            last_progress_log = time.time()
            
            while True:
                # Check for shutdown
                if self.shutdown:
                    logger.info(f"[{task_id}] Shutdown requested, stopping extraction")
                    break
                
                # Fetch batch of rows with timeout protection
                fetch_start = time.time()
                
                try:
                    rows = cursor.fetchmany(batch_size)
                except oracledb.DatabaseError as e:
                    error_code = e.args[0].code if hasattr(e.args[0], 'code') else 0
                    if error_code in [3113, 3114, 3135]:  # Connection errors
                        logger.error(f"[{task_id}] Lost connection during fetch: ORA-{error_code:05d}")
                        raise
                    else:
                        raise
                
                fetch_time += time.time() - fetch_start
                
                if not rows:
                    break
                
                batch_rows.extend(rows)
                
                # Update last_id for resume capability
                if rows:
                    last_id = rows[-1][0]  # Assumes first column is ID
                
                # Process when batch is full or on last iteration
                if len(batch_rows) >= batch_size or (len(rows) < batch_size and batch_rows):
                    process_start = time.time()
                    
                    # Check memory before processing
                    if not self.check_memory():
                        logger.warning(f"[{task_id}] Memory pressure detected, processing partial batch")
                    
                    # Convert to DataFrame with zero-copy
                    df = self.rows_to_dataframe_zero_copy(batch_rows, columns)
                    
                    # Clean and optimize
                    df = self._clean_numeric_data(df)
                    df = self._optimize_dataframe_memory(df)
                    
                    if schema is None:
                        # Create schema from first batch
                        schema = self._create_consistent_schema(df)
                        logger.info(f"[{task_id}] Created schema with {len(schema)} fields")
                    
                    # Apply schema
                    df = self._apply_consistent_schema(df, schema)
                    
                    # Generate filename for this chunk
                    chunk_file = f"{output_path}.chunk_{chunk_num:06d}.tmp"
                    
                    # Submit to async writer
                    self.async_writer.submit(chunk_file, df, schema)
                    
                    rows_written += len(df)
                    chunk_num += 1
                    batch_rows = []
                    
                    process_time += time.time() - process_start
                    
                    # Log progress periodically
                    if time.time() - last_progress_log > 10:  # Every 10 seconds
                        elapsed = time.time() - start_time
                        rate = rows_written / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"[{task_id}] Progress: {rows_written:,} rows at {rate:,.0f} rows/sec "
                            f"(fetch: {fetch_time:.1f}s, process: {process_time:.1f}s)"
                        )
                        last_progress_log = time.time()
            
            # Process final batch if exists
            if batch_rows and not self.shutdown:
                df = self.rows_to_dataframe_zero_copy(batch_rows, columns)
                df = self._clean_numeric_data(df)
                
                if schema is None:
                    df = self._optimize_dataframe_memory(df)
                    schema = self._create_consistent_schema(df)
                
                df = self._apply_consistent_schema(df, schema)
                
                chunk_file = f"{output_path}.chunk_{chunk_num:06d}.tmp"
                self.async_writer.submit(chunk_file, df, schema)
                rows_written += len(df)
            
            cursor.close()
            
            # Wait for async writes to complete
            if rows_written > 0:
                logger.info(f"[{task_id}] Waiting for async writes to complete...")
                for queue in self.async_writer.write_queues:
                    queue.join()
                
                # Merge chunk files into final output
                self._merge_chunk_files(output_path, schema)
                
                elapsed = time.time() - start_time
                rate = rows_written / elapsed if elapsed > 0 else 0
                logger.info(
                    f"[{task_id}] Completed {rows_written:,} rows to {output_path} "
                    f"at {rate:,.0f} rows/sec (total: {elapsed:.1f}s)"
                )
            
            return rows_written, last_id
            
        except Exception as e:
            logger.error(f"[{task_id}] Error in stream extraction: {str(e)}")
            # Log additional diagnostic info
            if connection:
                try:
                    logger.error(f"[{task_id}] Connection state: opened={self.pool.opened}, busy={self.pool.busy}")
                except:
                    pass
            raise
        finally:
            if connection:
                try:
                    self.pool.release(connection)
                except Exception as e:
                    logger.error(f"[{task_id}] Error releasing connection: {str(e)}")
    
    def _merge_chunk_files(self, output_path: str, schema: pa.Schema):
        """Merge temporary chunk files into final output"""
        chunk_pattern = f"{output_path}.chunk_*.tmp"
        chunk_files = sorted(glob.glob(chunk_pattern))
        
        if not chunk_files:
            return
        
        logger.info(f"Merging {len(chunk_files)} chunk files...")
        
        # Create final parquet file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        compression_args = self._get_parquet_compression_args()
        
        with pq.ParquetWriter(
            output_path,
            schema,
            **compression_args
        ) as writer:
            for chunk_file in chunk_files:
                try:
                    # Read chunk
                    table = pq.read_table(chunk_file)
                    # Write to final file
                    writer.write_table(table, row_group_size=self.parquet_row_group_size)
                    # Remove chunk file
                    os.remove(chunk_file)
                except Exception as e:
                    logger.error(f"Error merging chunk {chunk_file}: {str(e)}")
        
        # Log file size and compression ratio
        if os.path.exists(output_path):
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"Final Parquet file size: {file_size_mb:.2f} MB")
    
    def process_partition_parallel(self, task: ExtractionTask) -> Dict[str, Any]:
        """Process a partition with enhanced parallel extraction"""
        partition_id = task.partition_id
        
        logger.info(f"Processing partition {partition_id} in parallel")
        
        # Determine output path
        partition_path = os.path.join(self.data_lake_path, self.output_table_name)
        
        if task.date_range[0]:
            date_str = task.date_range[0].strftime('%Y-%m-%d_%H')
            partition_path = os.path.join(partition_path, f"date={date_str}")
        
        if task.partition_value is not None:
            partition_path = os.path.join(partition_path, f"{self.partition_column}={task.partition_value}")
        
        if task.rowid_range and task.rowid_range[0]:
            # Add ROWID range to path for uniqueness
            rowid_suffix = f"rowid_{hash(task.rowid_range[0])%1000000:06d}"
            partition_path = os.path.join(partition_path, rowid_suffix)
        
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
                # Build query with ROWID range if available
                query, params = self.build_optimized_query(
                    task.date_range, 
                    last_id, 
                    task.partition_value,
                    task.rowid_range
                )
                
                # Generate output filename
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"part_{partition_id}_{timestamp}_{chunk_num:06d}.parquet"
                file_path = os.path.join(partition_path, filename)
                
                # Stream extract with enhanced method
                chunk_rows, new_last_id = self.stream_extract_to_parquet_enhanced(
                    query, params, file_path, task_id=partition_id
                )
                
                if chunk_rows == 0:
                    consecutive_empty += 1
                    logger.debug(f"[{partition_id}] Empty result {consecutive_empty}/{max_consecutive_empty}")
                    continue
                
                consecutive_empty = 0
                rows_processed += chunk_rows
                chunk_num += 1
                files_written.append(filename)
                last_id = new_last_id
                
                logger.info(
                    f"Partition {partition_id}: {rows_processed:,} total rows "
                    f"(+{chunk_rows:,} in chunk {chunk_num})"
                )
                
            except Exception as e:
                logger.error(f"Error in partition {partition_id}: {str(e)}")
                raise
        
        return {
            'partition_id': partition_id,
            'rows_processed': rows_processed,
            'files_written': len(files_written),
            'path': partition_path
        }
    
    def run_parallel(self):
        """Execute ETL with enhanced parallel partition processing"""
        start_time = datetime.now()
        extraction_id = self.get_extraction_id()
        
        logger.info("="*60)
        logger.info(f"Starting Enhanced Parallel JOIN ETL Process v2.0")
        logger.info(f"Extraction ID: {extraction_id}")
        logger.info(f"Using {self.max_workers} parallel workers")
        logger.info("="*60)
        
        # Start profiling if enabled
        profiler = None
        if self.config.get('enable_profiling', False):
            profiler = cProfile.Profile()
            profiler.enable()
        
        try:
            # Create extraction tasks based on strategy
            tasks = []
            
            if self.use_rowid_splitting:
                # Get ROWID ranges for parallel processing
                connection = self.pool.acquire()
                try:
                    rowid_ranges = self.get_rowid_ranges(connection, self.max_workers * 2)
                finally:
                    self.pool.release(connection)
                
                # Create tasks for each ROWID range
                for i, rowid_range in enumerate(rowid_ranges):
                    task = ExtractionTask(
                        partition_id=f"rowid_{i:03d}",
                        date_range=(
                            datetime.strptime(self.start_date, '%Y-%m-%d') if self.start_date else None,
                            datetime.strptime(self.end_date, '%Y-%m-%d') if self.end_date else None
                        ),
                        partition_value=None,
                        rowid_range=rowid_range
                    )
                    tasks.append(task)
            else:
                # Fall back to date-based splitting
                date_ranges = self.get_adaptive_date_ranges()
                
                for date_range in date_ranges:
                    task = ExtractionTask(
                        partition_id=f"{date_range[0].strftime('%Y%m%d_%H') if date_range[0] else 'all'}",
                        date_range=date_range,
                        partition_value=None
                    )
                    tasks.append(task)
            
            logger.info(f"Created {len(tasks)} extraction tasks")
            
            # Process tasks in parallel
            completed_partitions = []
            future_to_task = {}
            
            # Submit initial batch of tasks
            for task in tasks[:self.max_workers]:
                future = self.extraction_executor.submit(self.process_partition_parallel, task)
                future_to_task[future] = task
            
            remaining_tasks = tasks[self.max_workers:]
            
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
                        
                        # Submit next task if available
                        if remaining_tasks and not self.shutdown:
                            next_task = remaining_tasks.pop(0)
                            new_future = self.extraction_executor.submit(
                                self.process_partition_parallel, next_task
                            )
                            future_to_task[new_future] = next_task
                            
                    except Exception as e:
                        logger.error(f"Failed partition {task.partition_id}: {str(e)}")
                        if "ORA-01555" in str(e):
                            logger.warning("Snapshot too old error - consider smaller chunks or ROWID splitting")
                        raise
                
                if self.shutdown:
                    logger.info("Shutdown requested, cancelling remaining tasks")
                    for future in future_to_task:
                        future.cancel()
                    break
            
            # Summary
            total_rows = sum(p['rows_processed'] for p in completed_partitions)
            total_files = sum(p['files_written'] for p in completed_partitions)
            
            # Stop profiling
            if profiler:
                profiler.disable()
                self._save_profile_stats(profiler, extraction_id)
            
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
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise
        finally:
            self.cleanup()
    
    def get_adaptive_date_ranges(self) -> List[Tuple[datetime, datetime]]:
        """Split date range adaptively based on estimated data volume"""
        if not self.start_date or not self.end_date:
            return [(None, None)]
        
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        date_ranges = []
        current = start
        
        # With enhanced parallel processing and ROWID splitting, use larger chunks
        chunk_hours = 48  # Increased from 24
        
        while current <= end:
            next_date = min(current + timedelta(hours=chunk_hours), end + timedelta(days=1))
            date_ranges.append((current, next_date))
            current = next_date
        
        return date_ranges
    
    def _save_profile_stats(self, profiler, extraction_id: str):
        """Save profiling statistics"""
        s = StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
        ps.print_stats(100)  # Top 100 functions
        
        profile_path = os.path.join(self.checkpoint_dir, f'profile_{extraction_id}.txt')
        with open(profile_path, 'w') as f:
            f.write(s.getvalue())
        
        logger.info(f"Saved profiling stats to {profile_path}")
    
    def _log_final_statistics(self, extraction_id: str, total_rows: int, 
                            total_files: int, completed_partitions: List,
                            start_time: datetime):
        """Log final statistics and metadata"""
        # Memory pool statistics
        pool_stats = self.memory_pool.get_stats()
        logger.info(f"Memory pool stats: {pool_stats}")
        
        # Async writer statistics
        writer_stats = self.async_writer.write_stats
        logger.info(f"Async writer stats: {writer_stats}")
        
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
            logger.info(f"Network latency: {self.parallel_stats['network_latency_ms']:.2f}ms")
            logger.info("="*60)
        
        # Write metadata
        metadata = {
            'extraction_id': extraction_id,
            'version': '2.0',
            'query_type': 'enhanced_parallel_join_v2',
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
                'connection_pool_size': self.pool.max,
                'use_rowid_splitting': self.use_rowid_splitting,
                'use_network_compression': self.use_network_compression,
                'network_buffer_size': self.network_buffer_size
            },
            'parquet_settings': {
                'compression': self.parquet_compression,
                'compression_level': self.parquet_compression_level,
                'row_group_size': self.parquet_row_group_size,
                'dictionary_columns': self.parquet_use_dictionary,
                'write_statistics': self.parquet_write_statistics,
                'write_page_index': self.parquet_write_page_index,
                'use_direct_io': self.use_direct_io,
                'parallel_writers': self.parallel_parquet_writers
            },
            'performance_stats': {
                'parallel_stats': self.parallel_stats,
                'memory_pool_stats': pool_stats,
                'writer_stats': writer_stats,
                'gc_stats_summary': {
                    'total_collections': self.gc_stats.get('collection_count', 0),
                    'total_gc_time': self.gc_stats.get('total_time', 0),
                    'total_collected': self.gc_stats.get('total_collected', 0)
                } if self.monitor_gc and hasattr(self, 'gc_stats') else None
            },
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
            gc.enable()
            logger.info("Restored original GC settings")
        
        # Log GC statistics
        if self.monitor_gc and hasattr(self, 'gc_stats'):
            self._log_gc_summary()
        
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Close connection pool
        if hasattr(self, 'pool'):
            self.pool.close()
    
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
    
    # Include other required methods
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
            result = cursor.fetchone()
            active_servers = result[0] if result else 0
            
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
                            self.parallel_stats['max_parallel_degree'], degree or 0
                        )
            
            cursor.close()
            
            if active_servers > 0:
                logger.info(f"Parallel execution active: {active_servers} servers in use")
                
        except Exception as e:
            logger.debug(f"Parallel monitoring error: {str(e)}")
    
    def check_memory(self) -> bool:
        """Check if memory usage is within limits"""
        mem = psutil.virtual_memory()
        current_percent = mem.percent
        
        if current_percent > self.memory_limit_percent:
            logger.warning(f"Memory usage at {current_percent:.1f}%, above limit of {self.memory_limit_percent}%")
            
            # Manual GC only when needed
            gc_counts_before = gc.get_count()
            start_time = time.time()
            collected = gc.collect(2)  # Full collection
            gc_duration = time.time() - start_time
            
            logger.info(
                f"GC collected {collected} objects in {gc_duration:.3f}s "
                f"(generations before: {gc_counts_before})"
            )
            
            # Give system time to release memory
            time.sleep(0.5)
            
            # Check memory again
            mem = psutil.virtual_memory()
            new_percent = mem.percent
            
            logger.info(
                f"Memory after GC: {new_percent:.1f}% "
                f"(reduced by {current_percent - new_percent:.1f}%)"
            )
            
            if new_percent > 85:
                logger.error(f"Memory critical at {new_percent:.1f}% even after GC")
                return False
                
        return True
    
    def _get_parquet_compression_args(self) -> Dict[str, Any]:
        """Get compression arguments for Parquet writer"""
        writer_args = {
            'compression': self.parquet_compression,
            'use_dictionary': self.parquet_use_dictionary,
            'write_statistics': self.parquet_write_statistics,
        }
        
        # Only add compression level for algorithms that support it
        if self.parquet_compression in ['zstd', 'gzip', 'brotli'] and self.parquet_compression_level:
            writer_args['compression_level'] = self.parquet_compression_level
        
        # Add page index support for newer PyArrow versions
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
            if df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                if df[col].dtype.name.startswith('float'):
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            
            elif df[col].dtype == 'object':
                # Try to convert object columns to numeric if possible
                sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                
                if sample_val is not None:
                    try:
                        float(str(sample_val))
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
                        # Use nullable integer types
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
                        # Use standard integer types
                        c_min = df[col].min()
                        c_max = df[col].max()
                        if c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                            df[col] = df[col].astype(np.int16)
                        elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                            df[col] = df[col].astype(np.int32)
                
                elif col_type.name.startswith('float'):
                    # Keep as float64 for compatibility
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    
            else:
                # String columns
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
                # Use appropriate integer type
                if dtype.name in ['int8', 'Int8', 'int16', 'Int16']:
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
            
            if pa.types.is_integer(field.type):
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                
                if df[col_name].isnull().any():
                    # Use nullable integer types
                    if pa.types.is_int16(field.type):
                        df[col_name] = df[col_name].astype('Int16')
                    elif pa.types.is_int32(field.type):
                        df[col_name] = df[col_name].astype('Int32')
                    else:
                        df[col_name] = df[col_name].astype('Int64')
                else:
                    # Use standard integer types
                    if pa.types.is_int16(field.type):
                        df[col_name] = df[col_name].astype(np.int16)
                    elif pa.types.is_int32(field.type):
                        df[col_name] = df[col_name].astype(np.int32)
                    else:
                        df[col_name] = df[col_name].astype(np.int64)
                        
            elif pa.types.is_floating(field.type):
                df[col_name] = df[col_name].replace([np.inf, -np.inf], np.nan)
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype(np.float64)
                        
            elif pa.types.is_string(field.type):
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
    
    def run(self):
        """Execute the optimized ETL process"""
        self.run_parallel()


def main():
    """Main execution function with enhanced configuration"""
    
    # Enhanced configuration for maximum performance
    config = {
        # Oracle connection using SID
        'oracle_user': os.environ.get('ORACLE_USER', 'your_username'),
        'oracle_password': os.environ.get('ORACLE_PASSWORD', 'your_password'),
        'oracle_host': os.environ.get('ORACLE_HOST', 'hostname'),
        'oracle_port': os.environ.get('ORACLE_PORT', '1521'),
        'oracle_sid': os.environ.get('ORACLE_SID', 'ORCL'),
        
        # Output settings
        'output_table_name': 'joined_table1_table2_v2',
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
            'col1',
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
        
        # Enhanced performance settings
        'chunk_size': 200000,              # Doubled from 100k
        'max_workers': 6,                  # Increased from 4
        'parallel_degree': 16,             # Increased from 8
        'memory_limit_percent': 75,
        'enable_parallel_monitoring': True,
        'monitor_gc': True,
        
        # Network optimization
        'oracle_fetch_size': 50000,        # 5x increase
        'oracle_prefetch_rows': 100000,    # 5x increase
        'use_network_compression': True,   # Enable compression
        'network_buffer_size': 1048576,    # 1MB buffers
        
        # ROWID splitting for true parallelism
        'use_rowid_splitting': True,
        
        # Parquet optimization
        'parquet_compression': 'snappy',   # Faster than zstd
        'parquet_compression_level': None, # Not needed for snappy
        'parquet_row_group_size': 1000000, # Larger row groups
        'parquet_use_dictionary': ['999_categories'],
        'parquet_write_statistics': True,
        'parquet_write_page_index': True,
        
        # I/O optimization
        'use_direct_io': True,             # Linux only
        'parallel_parquet_writers': 4,     # Parallel writers
        
        # Row size estimate for adaptive fetching
        'estimated_row_size_bytes': 200,
        
        # Enable profiling
        'enable_profiling': False,  # Set to True to profile
    }
    
    # Set environment variables for optimal performance
    os.environ['PYTHONHASHSEED'] = '0'
    os.environ['MALLOC_TRIM_THRESHOLD_'] = '128000'
    
    # Log startup
    logger.info("Starting Oracle ETL v2.0 with all optimizations")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {sys.platform}")
    logger.info(f"CPU count: {os.cpu_count()}")
    logger.info(f"Total memory: {psutil.virtual_memory().total / (1024**3):.1f} GB")
    
    # Create and run enhanced ETL
    etl = OptimizedOracleJoinETL(config)
    
    try:
        etl.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
