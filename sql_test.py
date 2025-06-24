#!/usr/bin/env python3
"""
Optimized Oracle to Parquet ETL Script for Billion-Row JOIN Operations
Enhanced with Oracle Parallel Processing capabilities and optimized Parquet compression
Designed for 16GB RAM constraint with streaming and memory management
Modified to read category values from a parquet file instead of database
Uses Oracle SID connection instead of DSN
UPDATED: Optimized garbage collection for Python 3.12 and enhanced Parquet compression
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
        
        # ENHANCED: Parquet compression settings
        self.parquet_compression = config.get('parquet_compression', 'zstd')
        self.parquet_compression_level = config.get('parquet_compression_level', 3)
        self.parquet_row_group_size = config.get('parquet_row_group_size', None)  # Auto-calculate if None
        self.parquet_use_dictionary = config.get('parquet_use_dictionary', ['999_categories'])
        self.parquet_write_statistics = config.get('parquet_write_statistics', True)
        self.parquet_write_page_index = config.get('parquet_write_page_index', True)
        
        # Calculate optimal row group size if not specified
        if self.parquet_row_group_size is None:
            # Aim for 128MB row groups
            estimated_row_size_bytes = config.get('estimated_row_size_bytes', 200)
            target_row_group_mb = config.get('target_row_group_mb', 128)
            self.parquet_row_group_size = (target_row_group_mb * 1024 * 1024) // estimated_row_size_bytes
            # Ensure it's at least as large as chunk size for efficiency
            self.parquet_row_group_size = max(self.parquet_row_group_size, self.chunk_size)
            logger.info(f"Calculated optimal row group size: {self.parquet_row_group_size:,} rows")
        
        # GC monitoring flag
        self.monitor_gc = config.get('monitor_gc', True)
        
        # Initialize Oracle connection pool using SID
        # Create DSN from host, port, and SID
        oracle_host = config['oracle_host']
        oracle_port = config['oracle_port']
        oracle_sid = config['oracle_sid']
        
        # Create DSN string for SID connection
        dsn = oracledb.makedsn(oracle_host, oracle_port, sid=oracle_sid)
        
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
        
        # NEW: Optimize GC settings for large data processing
        self._optimize_gc_settings()
        
        # NEW: Set up GC monitoring
        if self.monitor_gc:
            self._setup_gc_monitoring()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown = False
        
        # Log Parquet configuration
        logger.info("="*60)
        logger.info("Parquet Configuration:")
        logger.info(f"Compression: {self.parquet_compression} (level {self.parquet_compression_level})")
        logger.info(f"Row Group Size: {self.parquet_row_group_size:,} rows")
        logger.info(f"Dictionary Encoding: {self.parquet_use_dictionary}")
        logger.info(f"Write Statistics: {self.parquet_write_statistics}")
        logger.info(f"Write Page Index: {self.parquet_write_page_index}")
        logger.info("="*60)
    
    def _optimize_gc_settings(self):
        """Optimize GC for billion-row processing workload"""
        # Store original settings for cleanup
        self.original_gc_thresholds = gc.get_threshold()
        logger.info(f"Original GC thresholds: {self.original_gc_thresholds}")
        
        # Increase generation 0 threshold significantly
        # This reduces GC overhead from ~3% to ~0.5% of runtime
        # With 50k rows per chunk, this allows ~10 chunks before GC
        gc.set_threshold(500000, 10, 10)
        logger.info("Set GC threshold to (500000, 10, 10) for better performance")
        
        # Collect once after all initialization
        collected = gc.collect()
        logger.info(f"Initial GC collected {collected} objects")
        
        # Freeze all currently tracked objects
        # This prevents GC from scanning long-lived objects created during init
        gc.freeze()
        logger.info("Froze initial objects to reduce GC overhead")
        
        # Log current GC stats
        for i in range(3):
            count = len(gc.get_objects(i))
            logger.info(f"Generation {i} objects: {count}")
    
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
                # Store start time for this collection
                info['start_time'] = time.time()
            elif phase == "stop":
                # Calculate collection duration
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
                
                # Log if collection took too long or collected many objects
                if duration > 0.1 or info['collected'] > 10000:
                    logger.info(
                        f"GC Gen {info['generation']}: collected {info['collected']} objects "
                        f"in {duration:.3f}s, {info['uncollectable']} uncollectable"
                    )
        
        gc.callbacks.append(gc_callback)
        logger.info("GC monitoring enabled")
    
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
        """Check if memory usage is within limits with smart GC triggering"""
        mem = psutil.virtual_memory()
        current_percent = mem.percent
        
        if current_percent > self.memory_limit_percent:
            logger.warning(f"Memory usage at {current_percent:.1f}%, above limit of {self.memory_limit_percent}%")
            
            # Get GC stats before collection
            gc_counts_before = gc.get_count()
            
            # Only call gc.collect() when actually needed
            start_time = time.time()
            collected = gc.collect()  # Full collection
            gc_duration = time.time() - start_time
            
            logger.info(
                f"GC collected {collected} objects in {gc_duration:.3f}s "
                f"(generations before: {gc_counts_before})"
            )
            
            # Give system time to release memory
            time.sleep(1)
            
            # Check memory again after GC
            mem = psutil.virtual_memory()
            new_percent = mem.percent
            
            logger.info(
                f"Memory after GC: {new_percent:.1f}% "
                f"(reduced by {current_percent - new_percent:.1f}%)"
            )
            
            if new_percent > 85:
                logger.error(f"Memory critical at {new_percent:.1f}% even after GC")
                # Log top memory consumers if available
                try:
                    import tracemalloc
                    if tracemalloc.is_tracing():
                        snapshot = tracemalloc.take_snapshot()
                        top_stats = snapshot.statistics('lineno')
                        logger.error("Top memory allocations:")
                        for stat in top_stats[:5]:
                            logger.error(f"  {stat}")
                except:
                    pass
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
            "NO_PARALLEL_INDEX(tb1)",
            "NO_PARALLEL_INDEX(tb2)"
        ]
        
        # Add statement-level parallel hint
        hints = f"/*+ {' '.join(parallel_hints)} */"
        
        # Build column list with comprehensive aliasing to avoid any ambiguity
        # Always alias table2 columns with tb2_ prefix
        table2_column_list = []
        for col in self.table2_columns:
            # Always use tb2_ prefix for clarity and to avoid any conflicts
            table2_column_list.append(f'tb2."{col}" as "tb2_{col}"')
        
        table2_columns_str = ', '.join(table2_column_list)
        
        # For parallel pipelined table function (if available in your Oracle version)
        if use_parallel_pipelined and hasattr(self, 'pipelined_function_name'):
            query = f"""
            SELECT {hints} *
            FROM TABLE({self.pipelined_function_name}(
                CURSOR(
                    SELECT tb1.*, {table2_columns_str}
                    FROM table_1 tb1
                    JOIN table_2 tb2 ON tb1.id = tb2.id
                    WHERE 1=1
            """
        else:
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
            
            # Use workarea_size_policy instead of pga_aggregate_target
            cursor.execute("ALTER SESSION SET workarea_size_policy = 'AUTO'")
            
            cursor.close()
            logger.info(f"Enabled parallel execution with degree {self.parallel_degree}")
            
        except Exception as e:
            logger.warning(f"Could not set all parallel options: {str(e)}")
        
    def _get_parquet_compression_args(self) -> Dict[str, Any]:
        """Get compression arguments for Parquet writer"""
        # Arguments for ParquetWriter constructor
        writer_args = {
            'compression': self.parquet_compression,
            'use_dictionary': self.parquet_use_dictionary,
            'write_statistics': self.parquet_write_statistics,
        }
        
        # Add compression level for algorithms that support it
        if self.parquet_compression in ['zstd', 'gzip', 'brotli']:
            writer_args['compression_level'] = self.parquet_compression_level
        
        # Add page index if supported (PyArrow 6.0+)
        try:
            import pyarrow
            major_version = int(pyarrow.__version__.split('.')[0])
            if major_version >= 6 and self.parquet_write_page_index:
                writer_args['write_page_index'] = True
        except:
            pass
        
        # Note: row_group_size is handled separately in write_table()
        return writer_args
    
    def stream_extract_to_parquet(self, query: str, params: Dict, 
                                 output_path: str) -> Tuple[int, int]:
        """
        Stream data directly from Oracle to Parquet with parallel execution monitoring
        ENHANCED: Optimized Parquet compression and writing with improved data cleaning
        """
        connection = self.pool.acquire()
        temp_file = None
        rows_written = 0
        last_id = None
        schema = None  # Store the schema from first batch
        
        try:
            # Enable parallel execution for this session
            self.enable_parallel_session(connection)
            
            cursor = connection.cursor()
            cursor.arraysize = 5000
            cursor.prefetchrows = cursor.arraysize + 1
            
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
            batch_size = 5000
            
            # Use temp file to avoid network I/O during write
            temp_file = os.path.join(self.temp_dir, f"temp_{os.getpid()}_{time.time()}.parquet")
            
            # Get compression arguments
            compression_args = self._get_parquet_compression_args()
            
            for row in cursor:
                if self.shutdown:
                    break
                    
                batch_rows.append(row)
                last_id = row[0]
                
                if len(batch_rows) >= batch_size:
                    # Check memory before processing
                    if not self.check_memory():
                        logger.error("Memory limit exceeded, stopping batch")
                        break
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(batch_rows, columns=columns)
                    
                    # NEW: Clean numeric data before optimization
                    df = self._clean_numeric_data(df)
                    
                    # Debug problematic columns
                    if 'pacnb' in df.columns and logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"pacnb dtype before optimization: {df['pacnb'].dtype}")
                        logger.debug(f"pacnb sample values: {df['pacnb'].dropna().head()}")
                        logger.debug(f"pacnb has inf: {np.isinf(df['pacnb'].dropna()).any() if df['pacnb'].dtype in ['float64', 'float32'] else 'N/A'}")
                        logger.debug(f"pacnb null count: {df['pacnb'].isnull().sum()}")
                    
                    if writer is None:
                        # First batch: optimize and create schema
                        df = self._optimize_dataframe_memory(df)
                        
                        try:
                            table = pa.Table.from_pandas(df)
                            schema = table.schema
                            
                            writer = pq.ParquetWriter(
                                temp_file,
                                schema,
                                **compression_args
                            )
                            
                            logger.debug(f"Created Parquet writer with {self.parquet_compression} compression")
                            logger.debug(f"Schema: {schema}")
                            
                        except Exception as e:
                            logger.error(f"Error creating Parquet table: {str(e)}")
                            logger.error(f"DataFrame dtypes: {df.dtypes.to_dict()}")
                            # Log problematic columns
                            for col in df.columns:
                                if df[col].dtype == 'object':
                                    unique_types = df[col].dropna().apply(type).unique()
                                    logger.error(f"Column {col} has mixed types: {unique_types}")
                            raise
                            
                    else:
                        # Subsequent batches: ensure schema consistency
                        df = self._apply_consistent_schema(df, schema)
                        
                        try:
                            table = pa.Table.from_pandas(df, schema=schema)
                        except Exception as e:
                            logger.error(f"Error creating table with consistent schema: {str(e)}")
                            logger.error(f"DataFrame dtypes: {df.dtypes.to_dict()}")
                            # Try without enforcing schema as fallback
                            logger.warning("Falling back to creating table without schema enforcement")
                            df = self._optimize_dataframe_memory(df)
                            table = pa.Table.from_pandas(df)
                    
                    # Write batch
                    writer.write_table(table, row_group_size=self.parquet_row_group_size)
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
                
                # Clean numeric data
                df = self._clean_numeric_data(df)
                
                if writer is None:
                    df = self._optimize_dataframe_memory(df)
                    try:
                        table = pa.Table.from_pandas(df)
                        schema = table.schema
                        writer = pq.ParquetWriter(
                            temp_file,
                            schema,
                            **compression_args
                        )
                    except Exception as e:
                        logger.error(f"Error creating final batch table: {str(e)}")
                        raise
                else:
                    df = self._apply_consistent_schema(df, schema)
                    try:
                        table = pa.Table.from_pandas(df, schema=schema)
                    except Exception as e:
                        logger.warning(f"Schema consistency failed for final batch: {str(e)}")
                        df = self._optimize_dataframe_memory(df)
                        table = pa.Table.from_pandas(df)
                
                writer.write_table(table, row_group_size=self.parquet_row_group_size)
                rows_written += len(df)
            
            cursor.close()
            
            # Close writer and move file
            if writer:
                writer.close()
                
                if os.path.exists(temp_file):
                    file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
                    logger.info(f"Temporary Parquet file size: {file_size_mb:.2f} MB ({self.parquet_compression} compression)")
            
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
    
    def _apply_consistent_schema(self, df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        """Apply consistent schema to DataFrame based on PyArrow schema"""
        for i, field in enumerate(schema):
            col_name = field.name
            if col_name not in df.columns:
                continue
            
            # Skip if column has nulls and target type can't handle them
            has_nulls = df[col_name].isnull().any()
            
            # Handle numeric types
            if pa.types.is_integer(field.type):
                # First ensure the column is numeric
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                
                if has_nulls:
                    # Use nullable integer types
                    if pa.types.is_int8(field.type):
                        df[col_name] = df[col_name].astype('Int8')
                    elif pa.types.is_int16(field.type):
                        df[col_name] = df[col_name].astype('Int16')
                    elif pa.types.is_int32(field.type):
                        df[col_name] = df[col_name].astype('Int32')
                    elif pa.types.is_int64(field.type):
                        df[col_name] = df[col_name].astype('Int64')
                else:
                    # Regular integer types
                    if pa.types.is_int8(field.type):
                        df[col_name] = df[col_name].astype(np.int8)
                    elif pa.types.is_int16(field.type):
                        df[col_name] = df[col_name].astype(np.int16)
                    elif pa.types.is_int32(field.type):
                        df[col_name] = df[col_name].astype(np.int32)
                    elif pa.types.is_int64(field.type):
                        df[col_name] = df[col_name].astype(np.int64)
                        
            elif pa.types.is_floating(field.type):
                # ENHANCED: Clean float data before conversion
                # Replace infinity values with NaN
                df[col_name] = df[col_name].replace([np.inf, -np.inf], np.nan)
                
                # Ensure numeric type
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                
                # Handle any mixed types or string representations
                if df[col_name].dtype == 'object':
                    # Try to convert string representations of numbers
                    df[col_name] = df[col_name].apply(lambda x: float(x) if isinstance(x, str) and x.strip() != '' else x)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                
                # Float types can handle NaN
                try:
                    if pa.types.is_float32(field.type):
                        df[col_name] = df[col_name].astype(np.float32)
                    else:
                        df[col_name] = df[col_name].astype(np.float64)
                except Exception as e:
                    logger.warning(f"Error converting {col_name} to float: {str(e)}")
                    # Fallback to float64
                    df[col_name] = df[col_name].astype(np.float64)
                        
            # Handle dictionary (categorical) types
            elif pa.types.is_dictionary(field.type):
                # FIXED: Only convert to category if no nulls
                if not df[col_name].isnull().any():
                    df[col_name] = df[col_name].astype('category')
                # Otherwise keep as string
                    
            # Handle string types
            elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                # Keep as string, don't convert to category if it has one
                if df[col_name].dtype.name == 'category' and df[col_name].isnull().any():
                    # Convert back to string if category with nulls
                    df[col_name] = df[col_name].astype(str)
                elif df[col_name].dtype != 'object' and df[col_name].dtype != 'string':
                    # Convert non-string types to string
                    df[col_name] = df[col_name].astype(str)
                        
        return df

    def _clean_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric data to prevent PyArrow conversion errors"""
        for col in df.columns:
            # Check if it's a numeric column or should be numeric
            if df[col].dtype in ['float64', 'float32', 'float16', 'int64', 'int32', 'int16', 'int8']:
                # Replace infinity values
                if df[col].dtype.name.startswith('float'):
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            
            elif df[col].dtype == 'object':
                # Check if this object column contains numeric data
                # Sample first non-null value
                sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                
                if sample_val is not None:
                    # Check if it's a numeric string or actual number
                    try:
                        float(str(sample_val))
                        # This column appears to contain numeric data
                        logger.debug(f"Converting object column {col} to numeric")
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        # Replace infinity values after conversion
                        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    except (ValueError, TypeError):
                        # Not numeric, leave as is
                        pass
        
        # Special handling for known problematic columns
        if hasattr(self, 'config') and 'problem_columns' in self.config:
            for col in self.config.get('problem_columns', []):
                if col in df.columns:
                    logger.debug(f"Special handling for problem column: {col}")
                    # Force to clean float64
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        
        return df
    
    def _optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame memory usage in-place
        Enhanced for better compression with column-specific optimizations
        """
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type != 'object':
                if col_type.name.startswith('int'):
                    # Check for nulls before optimization
                    if df[col].isnull().any():
                        # Use nullable integer types for columns with nulls
                        c_min = df[col].min()
                        c_max = df[col].max()
                        if pd.isna(c_min) or pd.isna(c_max):
                            # Skip optimization if all values are null
                            continue
                        
                        if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                            df[col] = df[col].astype('Int8')  # Nullable int8
                        elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                            df[col] = df[col].astype('Int16')  # Nullable int16
                        elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                            df[col] = df[col].astype('Int32')  # Nullable int32
                        else:
                            df[col] = df[col].astype('Int64')  # Nullable int64
                    else:
                        # No nulls, use regular integer types
                        c_min = df[col].min()
                        c_max = df[col].max()
                        if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                            df[col] = df[col].astype(np.int8)
                        elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                            df[col] = df[col].astype(np.int16)
                        elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                            df[col] = df[col].astype(np.int32)
                
                elif col_type.name.startswith('float'):
                    # ENHANCED: Handle special float values before downcasting
                    # Replace infinity values with NaN
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    
                    # Only downcast if no special values that might cause issues
                    try:
                        # Check if downcasting is safe
                        if df[col].isnull().sum() < len(df[col]):  # Not all nulls
                            # First ensure it's numeric
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                            # Try downcasting
                            temp_col = pd.to_numeric(df[col], downcast='float', errors='coerce')
                            
                            # Verify no data loss
                            if not df[col].isnull().equals(temp_col.isnull()):
                                logger.warning(f"Skipping downcast for {col} due to potential data loss")
                            else:
                                df[col] = temp_col
                                
                    except Exception as e:
                        logger.warning(f"Could not downcast float column {col}: {str(e)}")
                        # Keep original dtype if downcasting fails
                        # Ensure it's at least clean float64
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            
            else:
                # Convert 999_categories column to categorical
                if col == '999_categories' or col == 'tb2_999_categories':
                    # Only convert to category if no nulls or handle nulls first
                    if not df[col].isnull().any():
                        df[col] = df[col].astype('category')
                    # If there are nulls, keep as string for now
                
                # Convert other string columns with low cardinality
                elif df[col].nunique() < 50:
                    # FIXED: Don't convert to category if column has nulls
                    # PyArrow has issues with null values in categorical columns
                    if not df[col].isnull().any():
                        df[col] = df[col].astype('category')
                    # If there are nulls, keep as string type
                    # This avoids the NumpyConverter error
        
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
        if checkpoint and 'partitions' in checkpoint and partition_id in checkpoint['partitions']:
            last_id = checkpoint['partitions'][partition_id].get('last_id')
            logger.info(f"Resuming partition {partition_id} from ID: {last_id}")
        
        # Process data in chunks - FIXED SECTION
        if checkpoint and 'partitions' in checkpoint and partition_id in checkpoint['partitions']:
            partition_checkpoint = checkpoint['partitions'][partition_id]
            rows_processed = partition_checkpoint.get('rows_processed', 0)
            files_written = partition_checkpoint.get('files_written', [])
        else:
            rows_processed = 0
            files_written = []
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
                
                # REMOVED: Manual gc.collect() call - let Python handle it
                
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
    
    def cleanup(self):
        """Restore original GC settings and clean up resources"""
        logger.info("Cleaning up ETL resources...")
        
        # Restore original GC settings
        if hasattr(self, 'original_gc_thresholds'):
            gc.set_threshold(*self.original_gc_thresholds)
            # Note: gc.unfreeze() is only available in Python 3.7+
            # For Python 3.12, frozen objects remain frozen
            logger.info("Restored original GC thresholds")
        
        # Log GC statistics if monitoring was enabled
        if self.monitor_gc and hasattr(self, 'gc_stats'):
            self._log_gc_summary()
        
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Close connection pool
        if hasattr(self, 'pool'):
            self.pool.close()
    
    def _log_gc_summary(self):
        """Log summary of GC activity during ETL run"""
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
        
        # Show generation statistics
        gen_stats = {}
        for collection in self.gc_stats['collections']:
            gen = collection['generation']
            if gen not in gen_stats:
                gen_stats[gen] = {'count': 0, 'collected': 0, 'time': 0}
            gen_stats[gen]['count'] += 1
            gen_stats[gen]['collected'] += collection['collected']
            gen_stats[gen]['time'] += collection['duration']
        
        for gen, stats in sorted(gen_stats.items()):
            logger.info(
                f"Generation {gen}: {stats['count']} collections, "
                f"{stats['collected']:,} objects, {stats['time']:.3f}s total"
            )
        
        logger.info("="*60)
    
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
        logger.info(f"GC threshold: {gc.get_threshold()}")
        logger.info(f"GC monitoring: {'Enabled' if self.monitor_gc else 'Disabled'}")
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
                        # REMOVED: Manual gc.collect() - handled in check_memory()
                    
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
            # Ensure cleanup is always called
            self.cleanup()


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
        'monitor_gc': True,            # Enable GC monitoring
        
        # ENHANCED: Parquet compression settings
        'parquet_compression': 'zstd',  # Changed from snappy to zstd
        'parquet_compression_level': 3,  # Good balance of speed and compression
        # Alternative: Use 6-9 for better compression if write speed is not critical
        # 'parquet_compression_level': 6,
        
        # Row group size - auto-calculated based on estimated row size
        'estimated_row_size_bytes': 200,  # Adjust based on your actual data
        'target_row_group_mb': 128,       # Target 128MB row groups
        # 'parquet_row_group_size': 640000,  # Or set explicitly
        
        # Dictionary encoding for low-cardinality columns
        'parquet_use_dictionary': ['999_categories'],  # Add other categorical columns
        
        # Enable statistics and page indexes for better query performance
        'parquet_write_statistics': True,
        'parquet_write_page_index': True,
        
        # Alternative compression options for different use cases:
        # For fastest write speed (real-time ETL):
        # 'parquet_compression': 'lz4',
        
        # For maximum compression (archival):
        # 'parquet_compression': 'gzip',
        # 'parquet_compression_level': 9,
        
        # For balanced performance with tunable compression:
        # 'parquet_compression': 'zstd',
        # 'parquet_compression_level': 6,  # Range 1-22, default 3
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
