#!/usr/bin/env python3
"""
Data Pipeline Project Setup Script
Creates a comprehensive directory structure for a data pipeline project
using Medallion architecture with support for multiple data sources.
"""

import os
import json
import yaml
from pathlib import Path
from datetime import datetime


class ProjectSetup:
    def __init__(self, project_name="data_pipeline", base_path=None):
        """
        Initialize project setup with project name and base path.
        
        Args:
            project_name (str): Name of the project
            base_path (str): Base path where project will be created. 
                           Defaults to current directory.
        """
        self.project_name = project_name
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.project_root = self.base_path / self.project_name
        
    def create_directory_structure(self):
        """Create the complete project directory structure."""
        
        # Define the directory structure
        directories = [
            # Root directories
            "src",
            "config",
            "docs",
            "tests",
            "notebooks",
            "logs",
            "data",
            "scripts",
            
            # Source code structure
            "src/pipelines",
            "src/pipelines/bronze",
            "src/pipelines/silver", 
            "src/pipelines/gold",
            
            # Connectors for different data sources
            "src/connectors",
            "src/connectors/oracle",
            "src/connectors/sqlserver",
            "src/connectors/mongodb",
            "src/connectors/databricks",
            
            # Data processing modules
            "src/transformations",
            "src/transformations/bronze_to_silver",
            "src/transformations/silver_to_gold",
            
            # Utilities and common functions
            "src/utils",
            "src/utils/logging",
            "src/utils/validation",
            "src/utils/monitoring",
            
            # Schema definitions
            "src/schemas",
            "src/schemas/bronze",
            "src/schemas/silver",
            "src/schemas/gold",
            "src/schemas/registry",
            "src/schemas/versions",
            "src/schemas/versions/v1",
            "src/schemas/versions/migrations",
            
            # Orchestration (Airflow alternative)
            "src/orchestration",
            "src/orchestration/workflows",
            "src/orchestration/workflows/daily",
            "src/orchestration/workflows/weekly",
            "src/orchestration/workflows/monthly",
            
            # Data Quality & Monitoring
            "src/quality",
            "src/quality/rules",
            "src/quality/profilers",
            "src/quality/alerts",
            
            # State Management & Checkpointing
            "src/state",
            
            # Security & Credentials
            "src/security",
            
            # Performance & Optimization
            "src/performance",
            
            # Configuration files (removed connections subdirectory)
            "config/pipelines",
            "config/databricks",
            "config/environments",
            
            # Test directories
            "tests/unit",
            "tests/integration",
            "tests/fixtures",
            
            # Data directories (for local development/testing)
            "data/raw",
            "data/bronze",
            "data/silver",
            "data/gold",
            "data/archive",
            "data/checkpoints",
            
            # Documentation
            "docs/api",
            "docs/design",
            "docs/guides",
            "docs/runbooks",
            "docs/adr",
            
            # Databricks notebooks
            "notebooks/exploration",
            "notebooks/bronze",
            "notebooks/silver",
            "notebooks/gold",
            
            # Deployment and orchestration
            "scripts/deployment",
            "scripts/migration",
            "scripts/maintenance",
        ]
        
        # Create all directories
        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"Created: {dir_path}")
            
        # Create __init__.py files for Python packages
        self._create_init_files()
        
        # Create configuration templates (removed database_config.json)
        self._create_config_templates()

        # Create env files
        self._create_env_files()

        # Create config loader
        self._create_config_loader()

        # Create Oracle connector
        self._create_oracle_connector()
        
        # Create enhanced configuration files
        self._create_enhanced_configs()
        
        # Create sample files
        self._create_sample_files()
        
        # Create orchestration files
        self._create_orchestration_files()
        
        # Create data quality files
        self._create_data_quality_files()
        
        # Create state management files
        self._create_state_management_files()
        
        # Create security files
        self._create_security_files()
        
        # Create performance files
        self._create_performance_files()
        
        # Create documentation files
        self._create_documentation()
        
        # Create operational runbooks
        self._create_runbooks()
        
        # Create ADRs
        self._create_adrs()
        
        # Create requirements file
        self._create_requirements()
        
        # Create .gitignore
        self._create_gitignore()
        
        # Create Makefile
        self._create_makefile()
        
        # Create Docker files
        self._create_docker_files()
        
        print(f"\n✅ Project '{self.project_name}' created successfully at: {self.project_root}")
        
    def _create_init_files(self):
        """Create __init__.py files to make directories Python packages."""
        init_dirs = [
            "src",
            "src/pipelines",
            "src/pipelines/bronze",
            "src/pipelines/silver",
            "src/pipelines/gold",
            "src/connectors",
            "src/connectors/oracle",
            "src/connectors/sqlserver",
            "src/connectors/mongodb",
            "src/connectors/databricks",
            "src/transformations",
            "src/transformations/bronze_to_silver",
            "src/transformations/silver_to_gold",
            "src/utils",
            "src/utils/logging",
            "src/utils/validation",
            "src/utils/monitoring",
            "src/schemas",
            "src/schemas/bronze",
            "src/schemas/silver",
            "src/schemas/gold",
            "src/schemas/registry",
            "src/schemas/versions",
            "src/orchestration",
            "src/orchestration/workflows",
            "src/quality",
            "src/quality/rules",
            "src/quality/profilers",
            "src/quality/alerts",
            "src/state",
            "src/security",
            "src/performance",
            "tests",
            "tests/unit",
            "tests/integration",
        ]
        
        for dir_name in init_dirs:
            init_file = self.project_root / dir_name / "__init__.py"
            init_file.touch()
            
    def _create_config_templates(self):
        """Create configuration file templates (excluding database config)."""
        
        # Pipeline configuration
        pipeline_config = {
            "bronze": {
                "batch_size": 10000,
                "parallel_jobs": 4,
                "error_threshold": 0.05
            },
            "silver": {
                "deduplication": True,
                "data_quality_checks": True,
                "schema_validation": True
            },
            "gold": {
                "aggregation_enabled": True,
                "business_rules": [],
                "output_formats": ["parquet", "delta"]
            }
        }
        
        pipeline_file = self.project_root / "config" / "pipelines" / "pipeline_config.json"
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline_config, f, indent=4)
    
    def _create_enhanced_configs(self):
        """Create enhanced configuration files."""
        
        # Environment-specific configs
        env_configs = {
            "dev": {
                "name": "development",
                "debug": True,
                "batch_size": 1000,
                "parallel_jobs": 2,
                "data_retention_days": 7,
                "checkpoint_interval_minutes": 30
            },
            "staging": {
                "name": "staging",
                "debug": False,
                "batch_size": 5000,
                "parallel_jobs": 4,
                "data_retention_days": 30,
                "checkpoint_interval_minutes": 15
            },
            "prod": {
                "name": "production",
                "debug": False,
                "batch_size": 10000,
                "parallel_jobs": 8,
                "data_retention_days": 90,
                "checkpoint_interval_minutes": 5
            }
        }
        
        for env_name, config in env_configs.items():
            env_file = self.project_root / "config" / "environments" / f"{env_name}.yaml"
            with open(env_file, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
        
        # Logging configuration
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                },
                "json": {
                    "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    "format": "%(asctime)s %(name)s %(levelname)s %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "INFO",
                    "formatter": "standard",
                    "stream": "ext://sys.stdout"
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": "DEBUG",
                    "formatter": "json",
                    "filename": "logs/pipeline.log",
                    "maxBytes": 10485760,
                    "backupCount": 5
                }
            },
            "root": {
                "level": "INFO",
                "handlers": ["console", "file"]
            }
        }
        
        logging_file = self.project_root / "config" / "logging_config.yaml"
        with open(logging_file, 'w') as f:
            yaml.dump(logging_config, f, default_flow_style=False)
        
        # Job schedules configuration
        schedules_config = {
            "jobs": [
                {
                    "name": "bronze_daily_ingestion",
                    "schedule": "0 2 * * *",
                    "enabled": True,
                    "pipeline": "bronze",
                    "tables": ["customers", "orders", "products"]
                },
                {
                    "name": "silver_daily_processing",
                    "schedule": "0 4 * * *",
                    "enabled": True,
                    "pipeline": "silver",
                    "dependencies": ["bronze_daily_ingestion"]
                },
                {
                    "name": "gold_weekly_aggregation",
                    "schedule": "0 6 * * 1",
                    "enabled": True,
                    "pipeline": "gold",
                    "dependencies": ["silver_daily_processing"]
                }
            ]
        }
        
        schedules_file = self.project_root / "config" / "job_schedules.yaml"
        with open(schedules_file, 'w') as f:
            yaml.dump(schedules_config, f, default_flow_style=False)

    def _create_env_files(self):
        """Create .env and .env.template files for secure credential storage."""
        
        # Create .env.template (for version control)
        env_template_content = """# Database Credentials
# Oracle Database
ORACLE_HOST=your_oracle_host
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=your_service_name
ORACLE_USER=your_oracle_username
ORACLE_PASSWORD=your_oracle_password

# SQL Server
SQLSERVER_SERVER=your_sqlserver_host
SQLSERVER_DATABASE=your_database
SQLSERVER_USER=your_sqlserver_username
SQLSERVER_PASSWORD=your_sqlserver_password
SQLSERVER_DRIVER={ODBC Driver 17 for SQL Server}

# MongoDB
MONGO_CONNECTION_STRING=mongodb://localhost:27017/
MONGO_DATABASE=your_database
MONGO_USER=your_mongo_username
MONGO_PASSWORD=your_mongo_password

# Databricks
DATABRICKS_HOST=your-databricks-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_token
DATABRICKS_CLUSTER_ID=your-cluster-id

# Vault Configuration (Optional)
VAULT_URL=http://localhost:8200
VAULT_TOKEN=your_vault_token

# Encryption Key Path
ENCRYPTION_KEY_PATH=.encryption_key

# Environment
ENV=development

# Logging Level
LOG_LEVEL=INFO

# Data Retention (days)
DATA_RETENTION_BRONZE=30
DATA_RETENTION_SILVER=90
DATA_RETENTION_GOLD=365

# Performance Settings
MAX_PARALLEL_JOBS=4
BATCH_SIZE=10000
CHECKPOINT_INTERVAL_MINUTES=30

# Cache Settings
CACHE_MAX_SIZE_GB=10
CACHE_TTL_HOURS=24

# Monitoring
PROMETHEUS_PORT=8000
METRICS_ENABLED=true

# Alert Settings
ALERT_EMAIL=data-team@company.com
PAGERDUTY_API_KEY=your_pagerduty_key

# Additional Security
API_KEY=your_api_key
JWT_SECRET=your_jwt_secret
"""
        
        env_template_file = self.project_root / ".env.template"
        with open(env_template_file, 'w') as f:
            f.write(env_template_content)
        
        # Create actual .env file (not for version control)
        env_file = self.project_root / ".env"
        if not env_file.exists():  # Don't overwrite if exists
            with open(env_file, 'w') as f:
                f.write(env_template_content)
            print(f"Created: {env_file} (Remember to update with real credentials!)")
        
        # Create .env.example with documentation
        env_example_content = """# Environment Variables Documentation
#
# This file documents all environment variables used by the data pipeline.
# Copy this file to .env and fill in your actual values.
#
# SECURITY NOTES:
# - Never commit .env files to version control
# - Use strong, unique passwords for each service
# - Rotate credentials regularly
# - Consider using a secret management service in production

# ==================== DATABASE CONNECTIONS ====================

# Oracle Database Connection
# Used for: Source data extraction from Oracle databases
ORACLE_HOST=<hostname>              # Oracle database hostname or IP
ORACLE_PORT=<port>                  # Oracle database port (default: 1521)
ORACLE_SERVICE_NAME=<service>       # Oracle service name
ORACLE_USER=<username>              # Oracle database username
ORACLE_PASSWORD=<password>          # Oracle database password

# SQL Server Connection
# Used for: Source data extraction from SQL Server
SQLSERVER_SERVER=<hostname>         # SQL Server hostname or IP
SQLSERVER_DATABASE=<database>       # Database name
SQLSERVER_USER=<username>           # SQL Server username  
SQLSERVER_PASSWORD=<password>       # SQL Server password
SQLSERVER_DRIVER=<driver>          # ODBC driver name

# MongoDB Connection
# Used for: NoSQL data source integration
MONGO_CONNECTION_STRING=<connection_string>  # MongoDB connection string
MONGO_DATABASE=<database>           # MongoDB database name
MONGO_USER=<username>               # MongoDB username
MONGO_PASSWORD=<password>           # MongoDB password

# Databricks Connection
# Used for: Spark processing and Delta Lake operations
DATABRICKS_HOST=<host>              # Databricks workspace URL
DATABRICKS_TOKEN=<token>            # Databricks personal access token
DATABRICKS_CLUSTER_ID=<cluster_id>  # Default cluster ID

# ==================== SECURITY CONFIGURATION ====================

# HashiCorp Vault (Optional - for production environments)
# If not using Vault, credentials will be read from environment variables
VAULT_URL=http://localhost:8200     # Vault server URL
VAULT_TOKEN=<token>                 # Vault access token

# Encryption Settings
ENCRYPTION_KEY_PATH=.encryption_key # Path to encryption key file

# API Security
API_KEY=<key>                       # API key for external services
JWT_SECRET=<secret>                 # JWT signing secret

# ==================== ENVIRONMENT SETTINGS ====================

# Environment Name
# Options: development, staging, production
ENV=development

# Logging Configuration
# Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL=INFO

# ==================== PERFORMANCE TUNING ====================

# Parallel Processing
MAX_PARALLEL_JOBS=4                 # Maximum concurrent jobs
BATCH_SIZE=10000                    # Records per batch
CHECKPOINT_INTERVAL_MINUTES=30      # How often to save checkpoints

# Caching Configuration
CACHE_MAX_SIZE_GB=10               # Maximum cache size in GB
CACHE_TTL_HOURS=24                 # Cache time-to-live in hours

# ==================== DATA RETENTION ====================

# Data retention periods in days
DATA_RETENTION_BRONZE=30           # Raw data retention
DATA_RETENTION_SILVER=90           # Cleaned data retention  
DATA_RETENTION_GOLD=365            # Aggregated data retention

# ==================== MONITORING & ALERTING ====================

# Prometheus Metrics
PROMETHEUS_PORT=8000               # Port for metrics endpoint
METRICS_ENABLED=true               # Enable/disable metrics collection

# Alerting Configuration
ALERT_EMAIL=data-team@company.com  # Email for alerts
PAGERDUTY_API_KEY=<key>           # PagerDuty integration key

# ==================== ADVANCED SETTINGS ====================

# Connection Pool Settings (per database)
DB_POOL_SIZE=10                    # Connection pool size
DB_POOL_TIMEOUT=30                 # Connection timeout in seconds

# Memory Management
MEMORY_LIMIT_GB=16                 # Maximum memory usage
SPARK_MEMORY_FRACTION=0.6          # Spark memory fraction

# Network Settings
REQUEST_TIMEOUT_SECONDS=300        # API request timeout
RETRY_ATTEMPTS=3                   # Number of retry attempts
RETRY_DELAY_SECONDS=5              # Delay between retries
"""
        
        env_example_file = self.project_root / ".env.example"
        with open(env_example_file, 'w') as f:
            f.write(env_example_content)
            
    def _create_config_loader(self):
        """Create the configuration loader utility."""
        
        config_loader_content = '''"""Configuration loader for the data pipeline project using environment variables"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Loads configuration from environment variables and config files."""
    
    def __init__(self, env_file: str = ".env", config_dir: str = "config"):
        """
        Initialize the configuration loader.
        
        Args:
            env_file: Path to .env file
            config_dir: Directory containing configuration files
        """
        self.env_file = Path(env_file)
        self.config_dir = Path(config_dir)
        
        # Load environment variables
        if self.env_file.exists():
            load_dotenv(self.env_file)
            logger.info(f"Loaded environment variables from {self.env_file}")
        else:
            logger.warning(f"No {self.env_file} found. Using system environment variables.")
            
    def get_oracle_config(self) -> Dict[str, Any]:
        """Get Oracle database configuration from environment variables."""
        return {
            'host': os.getenv('ORACLE_HOST'),
            'port': int(os.getenv('ORACLE_PORT', '1521')),
            'service_name': os.getenv('ORACLE_SERVICE_NAME'),
            'username': os.getenv('ORACLE_USER'),
            'password': os.getenv('ORACLE_PASSWORD')
        }
        
    def get_sqlserver_config(self) -> Dict[str, Any]:
        """Get SQL Server configuration from environment variables."""
        return {
            'server': os.getenv('SQLSERVER_SERVER'),
            'database': os.getenv('SQLSERVER_DATABASE'),
            'username': os.getenv('SQLSERVER_USER'),
            'password': os.getenv('SQLSERVER_PASSWORD'),
            'driver': os.getenv('SQLSERVER_DRIVER', '{ODBC Driver 17 for SQL Server}')
        }
        
    def get_mongodb_config(self) -> Dict[str, Any]:
        """Get MongoDB configuration from environment variables."""
        return {
            'connection_string': os.getenv('MONGO_CONNECTION_STRING', 'mongodb://localhost:27017/'),
            'database': os.getenv('MONGO_DATABASE'),
            'username': os.getenv('MONGO_USER'),
            'password': os.getenv('MONGO_PASSWORD')
        }
        
    def get_databricks_config(self) -> Dict[str, Any]:
        """Get Databricks configuration from environment variables."""
        return {
            'host': os.getenv('DATABRICKS_HOST'),
            'token': os.getenv('DATABRICKS_TOKEN'),
            'cluster_id': os.getenv('DATABRICKS_CLUSTER_ID')
        }
        
    def get_environment_config(self) -> Dict[str, Any]:
        """Get environment-specific configuration."""
        env_name = os.getenv('ENV', 'development')
        env_config_path = self.config_dir / 'environments' / f'{env_name}.yaml'
        
        if env_config_path.exists():
            with open(env_config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            logger.warning(f"Environment config not found: {env_config_path}")
            return {}
            
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration."""
        pipeline_config_path = self.config_dir / 'pipelines' / 'pipeline_config.json'
        
        if pipeline_config_path.exists():
            with open(pipeline_config_path, 'r') as f:
                import json
                return json.load(f)
        else:
            logger.warning(f"Pipeline config not found: {pipeline_config_path}")
            return {}
            
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        logging_config_path = self.config_dir / 'logging_config.yaml'
        
        if logging_config_path.exists():
            with open(logging_config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            return {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'standard': {
                        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    }
                },
                'handlers': {
                    'console': {
                        'class': 'logging.StreamHandler',
                        'level': os.getenv('LOG_LEVEL', 'INFO'),
                        'formatter': 'standard'
                    }
                },
                'root': {
                    'level': os.getenv('LOG_LEVEL', 'INFO'),
                    'handlers': ['console']
                }
            }
            
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance-related configuration from environment variables."""
        return {
            'max_parallel_jobs': int(os.getenv('MAX_PARALLEL_JOBS', '4')),
            'batch_size': int(os.getenv('BATCH_SIZE', '10000')),
            'checkpoint_interval_minutes': int(os.getenv('CHECKPOINT_INTERVAL_MINUTES', '30')),
            'cache_max_size_gb': float(os.getenv('CACHE_MAX_SIZE_GB', '10')),
            'cache_ttl_hours': int(os.getenv('CACHE_TTL_HOURS', '24'))
        }
        
    def get_retention_config(self) -> Dict[str, Any]:
        """Get data retention configuration from environment variables."""
        return {
            'bronze_days': int(os.getenv('DATA_RETENTION_BRONZE', '30')),
            'silver_days': int(os.getenv('DATA_RETENTION_SILVER', '90')),
            'gold_days': int(os.getenv('DATA_RETENTION_GOLD', '365'))
        }
        
    def validate_required_env_vars(self, required_vars: list) -> bool:
        """Validate that required environment variables are set."""
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
                
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            return False
            
        return True
'''
        
        config_loader_file = self.project_root / "src" / "utils" / "config_loader.py"
        with open(config_loader_file, 'w') as f:
            f.write(config_loader_content)
            
    def _create_oracle_connector(self):
        """Create the Oracle connector using environment variables."""
        
        oracle_connector_content = '''"""Oracle Database Connector using environment variables"""
import oracledb
import pandas as pd
from typing import Optional, Dict, Any
import logging
from src.utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class OracleConnector:
    """Connector for Oracle Database operations using environment variables."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Oracle connector.
        
        Args:
            config: Optional configuration dict. If not provided, loads from environment.
        """
        if config is None:
            # Load configuration from environment variables
            config_loader = ConfigLoader()
            config = config_loader.get_oracle_config()
            
            # Validate required configuration
            required_fields = ['host', 'port', 'service_name', 'username', 'password']
            if not config_loader.validate_required_env_vars([f'ORACLE_{field.upper()}' for field in required_fields if field != 'port']):
                raise ValueError("Missing required Oracle environment variables")
                
        self.config = config
        self.connection = None
        
    def connect(self):
        """Establish connection to Oracle database."""
        try:
            # Never log passwords!
            logger.info(f"Connecting to Oracle database at {self.config['host']}:{self.config['port']}")
            
            self.connection = oracledb.connect(
                user=self.config['username'],
                password=self.config['password'],
                dsn=f"{self.config['host']}:{self.config['port']}/{self.config['service_name']}"
            )
            logger.info("Successfully connected to Oracle database")
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {str(e)}")
            raise
            
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Oracle database")
            
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            df = pd.read_sql(query, self.connection)
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
            
    def execute_many(self, query: str, data: list):
        """Execute bulk insert/update operations."""
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, data)
            self.connection.commit()
            logger.info(f"Bulk operation completed: {cursor.rowcount} rows affected")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Bulk operation failed: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            self.connect()
            # Execute a simple test query
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            self.disconnect()
            return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
'''
        
        oracle_file = self.project_root / "src" / "connectors" / "oracle" / "oracle_connector.py"
        with open(oracle_file, 'w') as f:
            f.write(oracle_connector_content)
            
    def _create_sample_files(self):
        """Create sample Python files to get started."""
        
        # SQL Server connector
        sqlserver_connector = '''"""SQL Server Database Connector using environment variables"""
import pyodbc
import pandas as pd
from typing import Optional, Dict, Any, List
import logging
from src.utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class SQLServerConnector:
    """Connector for SQL Server Database operations using environment variables."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize SQL Server connector.
        
        Args:
            config: Optional configuration dict. If not provided, loads from environment.
        """
        if config is None:
            # Load configuration from environment variables
            config_loader = ConfigLoader()
            config = config_loader.get_sqlserver_config()
            
            # Validate required configuration
            required_vars = ['SQLSERVER_SERVER', 'SQLSERVER_DATABASE', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD']
            if not config_loader.validate_required_env_vars(required_vars):
                raise ValueError("Missing required SQL Server environment variables")
                
        self.config = config
        self.connection = None
        
    def connect(self):
        """Establish connection to SQL Server database."""
        try:
            # Build connection string
            conn_str = (
                f"DRIVER={self.config.get('driver', '{ODBC Driver 17 for SQL Server}')};"
                f"SERVER={self.config['server']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['username']};"
                f"PWD={self.config['password']}"
            )
            
            # Add optional parameters
            if self.config.get('trusted_connection'):
                conn_str += ";Trusted_Connection=yes"
            if self.config.get('encrypt'):
                conn_str += ";Encrypt=yes"
            if self.config.get('trust_server_certificate'):
                conn_str += ";TrustServerCertificate=yes"
                
            self.connection = pyodbc.connect(conn_str)
            logger.info(f"Successfully connected to SQL Server: {self.config['server']}/{self.config['database']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            raise
            
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from SQL Server database")
            
    def execute_query(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            if params:
                df = pd.read_sql(query, self.connection, params=params)
            else:
                df = pd.read_sql(query, self.connection)
                
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
            
    def execute_command(self, command: str, params: Optional[List] = None):
        """Execute SQL command (INSERT, UPDATE, DELETE, etc.)."""
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(command, params)
            else:
                cursor.execute(command)
                
            self.connection.commit()
            logger.info(f"Command executed successfully: {cursor.rowcount} rows affected")
            return cursor.rowcount
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Command execution failed: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def execute_many(self, query: str, data: List[tuple]):
        """Execute bulk insert/update operations."""
        cursor = self.connection.cursor()
        try:
            cursor.fast_executemany = True  # Enable fast bulk inserts
            cursor.executemany(query, data)
            self.connection.commit()
            logger.info(f"Bulk operation completed: {cursor.rowcount} rows affected")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Bulk operation failed: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def execute_stored_procedure(self, proc_name: str, params: Optional[List] = None):
        """Execute a stored procedure."""
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(f"EXEC {proc_name} {','.join(['?'] * len(params))}", params)
            else:
                cursor.execute(f"EXEC {proc_name}")
                
            # Fetch results if any
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                return pd.DataFrame.from_records(results, columns=columns)
            else:
                self.connection.commit()
                return None
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Stored procedure execution failed: {str(e)}")
            raise
        finally:
            cursor.close()
            
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema information for a table."""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query, [table_name])
        
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
'''
        
        sqlserver_file = self.project_root / "src" / "connectors" / "sqlserver" / "sqlserver_connector.py"
        with open(sqlserver_file, 'w') as f:
            f.write(sqlserver_connector)
        
        # MongoDB connector
        mongodb_connector = '''"""MongoDB Connector using environment variables"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import pandas as pd
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
from src.utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class MongoDBConnector:
    """Connector for MongoDB operations using environment variables."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MongoDB connector.
        
        Args:
            config: Optional configuration dict. If not provided, loads from environment.
        """
        if config is None:
            # Load configuration from environment variables
            config_loader = ConfigLoader()
            config = config_loader.get_mongodb_config()
            
            # Validate required configuration
            if not config.get('connection_string'):
                raise ValueError("MONGO_CONNECTION_STRING environment variable is required")
                
        self.config = config
        self.client = None
        self.database = None
        
    def connect(self):
        """Establish connection to MongoDB."""
        try:
            # Build connection string
            if self.config.get('username') and self.config.get('password'):
                # With authentication
                conn_str = (
                    f"mongodb://{self.config['username']}:{self.config['password']}@"
                    f"{self.config.get('connection_string', 'localhost:27017/').replace('mongodb://', '')}"
                )
            else:
                # Without authentication
                conn_str = self.config.get('connection_string', 'mongodb://localhost:27017/')
                
            # Connection options
            options = {
                'serverSelectionTimeoutMS': 5000,
                'connectTimeoutMS': 10000,
                'retryWrites': True,
                'w': 'majority'
            }
            
            self.client = MongoClient(conn_str, **options)
            
            # Test connection
            self.client.admin.command('ping')
            
            # Select database
            db_name = self.config.get('database', 'test')
            self.database = self.client[db_name]
            
            logger.info(f"Successfully connected to MongoDB: {db_name}")
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
            
    def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
            
    def find(self, collection_name: str, query: Dict = None, 
             projection: Dict = None, limit: int = None) -> pd.DataFrame:
        """Query documents from a collection and return as DataFrame."""
        try:
            collection = self.database[collection_name]
            
            cursor = collection.find(query or {}, projection)
            
            if limit:
                cursor = cursor.limit(limit)
                
            # Convert to list of dictionaries
            documents = list(cursor)
            
            if documents:
                # Convert to DataFrame
                df = pd.DataFrame(documents)
                
                # Convert ObjectId to string
                if '_id' in df.columns:
                    df['_id'] = df['_id'].astype(str)
                    
                logger.info(f"Retrieved {len(df)} documents from {collection_name}")
                return df
            else:
                logger.info(f"No documents found in {collection_name}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to query {collection_name}: {str(e)}")
            raise
            
    def find_one(self, collection_name: str, query: Dict) -> Optional[Dict]:
        """Find a single document."""
        try:
            collection = self.database[collection_name]
            document = collection.find_one(query)
            
            if document and '_id' in document:
                document['_id'] = str(document['_id'])
                
            return document
            
        except Exception as e:
            logger.error(f"Failed to find document in {collection_name}: {str(e)}")
            raise
            
    def insert_many(self, collection_name: str, documents: List[Dict]):
        """Insert multiple documents into a collection."""
        try:
            collection = self.database[collection_name]
            
            # Add metadata to documents
            for doc in documents:
                doc['_inserted_at'] = datetime.utcnow()
                
            result = collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
            
            return result.inserted_ids
            
        except Exception as e:
            logger.error(f"Failed to insert documents into {collection_name}: {str(e)}")
            raise
            
    def update_many(self, collection_name: str, filter_query: Dict, update: Dict):
        """Update multiple documents."""
        try:
            collection = self.database[collection_name]
            
            # Add update timestamp
            if '$set' in update:
                update['$set']['_updated_at'] = datetime.utcnow()
            else:
                update['$set'] = {'_updated_at': datetime.utcnow()}
                
            result = collection.update_many(filter_query, update)
            
            logger.info(f"Updated {result.modified_count} documents in {collection_name}")
            return result.modified_count
            
        except Exception as e:
            logger.error(f"Failed to update documents in {collection_name}: {str(e)}")
            raise
            
    def aggregate(self, collection_name: str, pipeline: List[Dict]) -> pd.DataFrame:
        """Execute an aggregation pipeline."""
        try:
            collection = self.database[collection_name]
            
            cursor = collection.aggregate(pipeline)
            documents = list(cursor)
            
            if documents:
                df = pd.DataFrame(documents)
                
                # Convert ObjectId to string if present
                if '_id' in df.columns:
                    df['_id'] = df['_id'].astype(str)
                    
                logger.info(f"Aggregation returned {len(df)} results from {collection_name}")
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Aggregation failed on {collection_name}: {str(e)}")
            raise
            
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get statistics for a collection."""
        try:
            stats = self.database.command("collStats", collection_name)
            
            return {
                'count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'average_object_size': stats.get('avgObjSize', 0),
                'storage_size': stats.get('storageSize', 0),
                'indexes': stats.get('nindexes', 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to get stats for {collection_name}: {str(e)}")
            raise
            
    def list_collections(self) -> List[str]:
        """List all collections in the database."""
        return self.database.list_collection_names()
        
    def create_index(self, collection_name: str, keys: List[tuple], unique: bool = False):
        """Create an index on a collection."""
        try:
            collection = self.database[collection_name]
            collection.create_index(keys, unique=unique)
            logger.info(f"Created index on {collection_name}: {keys}")
            
        except Exception as e:
            logger.error(f"Failed to create index on {collection_name}: {str(e)}")
            raise
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
'''
        
        mongodb_file = self.project_root / "src" / "connectors" / "mongodb" / "mongodb_connector.py"
        with open(mongodb_file, 'w') as f:
            f.write(mongodb_connector)
        
        # Databricks connector
        databricks_connector = '''"""Databricks Connector using environment variables"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
import pandas as pd
from typing import Optional, Dict, Any, List
import logging
import time
from src.utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class DatabricksConnector:
    """Connector for Databricks operations using environment variables."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Databricks connector.
        
        Args:
            config: Optional configuration dict. If not provided, loads from environment.
        """
        if config is None:
            # Load configuration from environment variables
            config_loader = ConfigLoader()
            config = config_loader.get_databricks_config()
            
            # Validate required configuration
            required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
            if not config_loader.validate_required_env_vars(required_vars):
                raise ValueError("Missing required Databricks environment variables")
                
        self.config = config
        self.client = None
        self.sql_client = None
        
    def connect(self):
        """Establish connection to Databricks workspace."""
        try:
            # Initialize Databricks SDK client
            self.client = WorkspaceClient(
                host=self.config['host'],
                token=self.config['token']
            )
            
            # Test connection by listing clusters
            clusters = list(self.client.clusters.list())
            logger.info(f"Successfully connected to Databricks. Found {len(clusters)} clusters.")
            
            # Get SQL warehouses/endpoints
            self.sql_endpoints = list(self.client.warehouses.list())
            if self.sql_endpoints:
                logger.info(f"Found {len(self.sql_endpoints)} SQL warehouses")
            
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {str(e)}")
            raise
            
    def disconnect(self):
        """Close Databricks connection."""
        # SDK handles connection lifecycle
        logger.info("Databricks connection closed")
        
    def execute_query(self, query: str, warehouse_id: Optional[str] = None, 
                     catalog: str = "main", schema: str = "default") -> pd.DataFrame:
        """Execute SQL query on Databricks SQL warehouse."""
        try:
            # Use provided warehouse_id or get the first available
            if not warehouse_id and self.sql_endpoints:
                warehouse_id = self.sql_endpoints[0].id
                
            if not warehouse_id:
                raise ValueError("No SQL warehouse available")
                
            logger.info(f"Executing query on warehouse: {warehouse_id}")
            
            # Execute statement
            response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                catalog=catalog,
                schema=schema,
                wait_timeout="30s"
            )
            
            # Wait for completion
            while response.status.state in [sql.StatementState.PENDING, sql.StatementState.RUNNING]:
                time.sleep(1)
                response = self.client.statement_execution.get_statement(
                    statement_id=response.statement_id
                )
                
            if response.status.state == sql.StatementState.FAILED:
                raise Exception(f"Query failed: {response.status.error}")
                
            # Convert result to DataFrame
            if response.result and response.result.data_array:
                columns = [col.name for col in response.manifest.schema.columns]
                data = [row.as_dict() for row in response.result.data_array]
                df = pd.DataFrame(data, columns=columns)
                
                logger.info(f"Query returned {len(df)} rows")
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
            
    def read_table(self, table_name: str, catalog: str = "main", 
                   schema: str = "default", limit: Optional[int] = None) -> pd.DataFrame:
        """Read a complete table from Databricks."""
        query = f"SELECT * FROM {catalog}.{schema}.{table_name}"
        if limit:
            query += f" LIMIT {limit}"
            
        return self.execute_query(query, catalog=catalog, schema=schema)
        
    def write_table(self, df: pd.DataFrame, table_name: str, 
                    catalog: str = "main", schema: str = "default",
                    mode: str = "overwrite"):
        """Write DataFrame to Databricks table."""
        try:
            # For large DataFrames, consider using Databricks' file upload APIs
            # This is a simplified version using SQL INSERT
            
            if mode == "overwrite":
                # Drop table if exists
                drop_query = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}"
                self.execute_query(drop_query, catalog=catalog, schema=schema)
                
            # Create table with schema inference
            # In production, define schema explicitly
            create_query = self._generate_create_table(df, table_name, catalog, schema)
            self.execute_query(create_query, catalog=catalog, schema=schema)
            
            # Insert data in batches
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                insert_query = self._generate_insert_query(batch, table_name, catalog, schema)
                self.execute_query(insert_query, catalog=catalog, schema=schema)
                
            logger.info(f"Successfully wrote {len(df)} rows to {catalog}.{schema}.{table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write table: {str(e)}")
            raise
            
    def list_tables(self, catalog: str = "main", schema: str = "default") -> List[str]:
        """List all tables in a schema."""
        query = f"SHOW TABLES IN {catalog}.{schema}"
        result = self.execute_query(query, catalog=catalog, schema=schema)
        return result['tableName'].tolist() if not result.empty else []
        
    def get_table_info(self, table_name: str, catalog: str = "main", 
                       schema: str = "default") -> pd.DataFrame:
        """Get table schema information."""
        query = f"DESCRIBE TABLE {catalog}.{schema}.{table_name}"
        return self.execute_query(query, catalog=catalog, schema=schema)
        
    def execute_notebook(self, notebook_path: str, cluster_id: str, 
                        parameters: Optional[Dict[str, Any]] = None) -> str:
        """Execute a Databricks notebook."""
        try:
            # Use cluster_id from config if not provided
            if not cluster_id:
                cluster_id = self.config.get('cluster_id')
                
            if not cluster_id:
                raise ValueError("No cluster_id provided")
                
            # Submit notebook run
            run = self.client.jobs.submit(
                run_name=f"Notebook run: {notebook_path}",
                tasks=[
                    {
                        "task_key": "notebook_task",
                        "notebook_task": {
                            "notebook_path": notebook_path,
                            "base_parameters": parameters or {}
                        },
                        "existing_cluster_id": cluster_id
                    }
                ]
            )
            
            logger.info(f"Submitted notebook run: {run.run_id}")
            
            # Wait for completion
            while True:
                run_info = self.client.jobs.get_run(run_id=run.run_id)
                if run_info.state.life_cycle_state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                    break
                time.sleep(5)
                
            if run_info.state.result_state == 'SUCCESS':
                logger.info(f"Notebook execution completed successfully")
                return run.run_id
            else:
                raise Exception(f"Notebook execution failed: {run_info.state.state_message}")
                
        except Exception as e:
            logger.error(f"Notebook execution failed: {str(e)}")
            raise
            
    def _generate_create_table(self, df: pd.DataFrame, table_name: str, 
                              catalog: str, schema: str) -> str:
        """Generate CREATE TABLE statement based on DataFrame."""
        # Map pandas dtypes to SQL types
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INT',
            'float64': 'DOUBLE',
            'float32': 'FLOAT',
            'object': 'STRING',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN'
        }
        
        columns = []
        for col, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), 'STRING')
            columns.append(f"`{col}` {sql_type}")
            
        columns_str = ", ".join(columns)
        return f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} ({columns_str})"
        
    def _generate_insert_query(self, df: pd.DataFrame, table_name: str, 
                              catalog: str, schema: str) -> str:
        """Generate INSERT statement for DataFrame."""
        # Convert DataFrame to SQL values
        values = []
        for _, row in df.iterrows():
            row_values = []
            for val in row.values:
                if pd.isna(val):
                    row_values.append("NULL")
                elif isinstance(val, str):
                    row_values.append(f"'{val.replace("'", "''")}'")
                else:
                    row_values.append(str(val))
            values.append(f"({', '.join(row_values)})")
            
        values_str = ", ".join(values)
        columns_str = ", ".join([f"`{col}`" for col in df.columns])
        
        return f"INSERT INTO {catalog}.{schema}.{table_name} ({columns_str}) VALUES {values_str}"
        
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
'''
        
        databricks_file = self.project_root / "src" / "connectors" / "databricks" / "databricks_connector.py"
        with open(databricks_file, 'w') as f:
            f.write(databricks_connector)
        
        # Bronze pipeline
        bronze_pipeline = '''"""Bronze Layer Pipeline - Raw Data Ingestion"""
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class BronzePipeline:
    """Pipeline for ingesting raw data into Bronze layer."""
    
    def __init__(self, source_connector, target_path: str):
        self.source_connector = source_connector
        self.target_path = target_path
        
    def ingest_table(self, table_name: str, query: str = None) -> Dict[str, Any]:
        """
        Ingest a single table from source to bronze layer.
        
        Args:
            table_name: Name of the table to ingest
            query: Optional custom query, otherwise SELECT * is used
            
        Returns:
            Dictionary with ingestion metadata
        """
        start_time = datetime.now()
        
        try:
            # Default query if not provided
            if not query:
                query = f"SELECT * FROM {table_name}"
                
            logger.info(f"Starting ingestion for table: {table_name}")
            
            # Extract data
            df = self.source_connector.execute_query(query)
            
            # Add metadata columns
            df['_bronze_loaded_at'] = datetime.now()
            df['_bronze_source'] = self.source_connector.__class__.__name__
            
            # Save to bronze layer
            output_path = f"{self.target_path}/{table_name}/data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.to_parquet(output_path, index=False)
            
            end_time = datetime.now()
            
            metadata = {
                'table_name': table_name,
                'records_count': len(df),
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': (end_time - start_time).total_seconds(),
                'output_path': output_path,
                'status': 'success'
            }
            
            logger.info(f"Successfully ingested {len(df)} records from {table_name}")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to ingest {table_name}: {str(e)}")
            return {
                'table_name': table_name,
                'status': 'failed',
                'error': str(e),
                'start_time': start_time,
                'end_time': datetime.now()
            }
'''
        
        bronze_file = self.project_root / "src" / "pipelines" / "bronze" / "bronze_pipeline.py"
        with open(bronze_file, 'w') as f:
            f.write(bronze_pipeline)

        # Connection testing script
        connection_tester = '''"""Test all database connections defined in environment variables"""
import sys
from typing import Dict, List, Tuple
from datetime import datetime
import logging

from src.connectors.oracle.oracle_connector import OracleConnector
from src.connectors.sqlserver.sqlserver_connector import SQLServerConnector
from src.connectors.mongodb.mongodb_connector import MongoDBConnector
from src.connectors.databricks.databricks_connector import DatabricksConnector
from src.utils.config_loader import ConfigLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectionTester:
    """Test all configured database connections."""
    
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.results = []
        
    def test_oracle_connection(self) -> Tuple[bool, str]:
        """Test Oracle connection."""
        try:
            connector = OracleConnector()
            connector.connect()
            df = connector.execute_query("SELECT 1 FROM DUAL")
            connector.disconnect()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
            
    def test_sqlserver_connection(self) -> Tuple[bool, str]:
        """Test SQL Server connection."""
        try:
            connector = SQLServerConnector()
            connector.connect()
            df = connector.execute_query("SELECT 1 AS test")
            connector.disconnect()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
            
    def test_mongodb_connection(self) -> Tuple[bool, str]:
        """Test MongoDB connection."""
        try:
            connector = MongoDBConnector()
            connector.connect()
            collections = connector.list_collections()
            connector.disconnect()
            return True, f"Connection successful, {len(collections)} collections found"
        except Exception as e:
            return False, str(e)
            
    def test_databricks_connection(self) -> Tuple[bool, str]:
        """Test Databricks connection."""
        try:
            connector = DatabricksConnector()
            connector.connect()
            connector.disconnect()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
            
    def test_all_connections(self):
        """Test all configured connections."""
        print("\\n" + "="*60)
        print("DATABASE CONNECTION TESTS")
        print("="*60)
        
        # Test Oracle
        print("\\nOracle Connection:")
        try:
            # Check if Oracle config exists
            oracle_config = self.config_loader.get_oracle_config()
            if oracle_config.get('host'):
                success, message = self.test_oracle_connection()
                status = "✓" if success else "✗"
                print(f"  {status} {message}")
            else:
                print("  ⚠ Oracle not configured (missing ORACLE_HOST)")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        # Test SQL Server
        print("\\nSQL Server Connection:")
        try:
            sqlserver_config = self.config_loader.get_sqlserver_config()
            if sqlserver_config.get('server'):
                success, message = self.test_sqlserver_connection()
                status = "✓" if success else "✗"
                print(f"  {status} {message}")
            else:
                print("  ⚠ SQL Server not configured (missing SQLSERVER_SERVER)")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        # Test MongoDB
        print("\\nMongoDB Connection:")
        try:
            mongo_config = self.config_loader.get_mongodb_config()
            if mongo_config.get('connection_string'):
                success, message = self.test_mongodb_connection()
                status = "✓" if success else "✗"
                print(f"  {status} {message}")
            else:
                print("  ⚠ MongoDB not configured (missing MONGO_CONNECTION_STRING)")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        # Test Databricks
        print("\\nDatabricks Connection:")
        try:
            databricks_config = self.config_loader.get_databricks_config()
            if databricks_config.get('host'):
                success, message = self.test_databricks_connection()
                status = "✓" if success else "✗"
                print(f"  {status} {message}")
            else:
                print("  ⚠ Databricks not configured (missing DATABRICKS_HOST)")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        print("\\n" + "="*60)


if __name__ == "__main__":
    tester = ConnectionTester()
    tester.test_all_connections()
'''
        
        connection_tester_file = self.project_root / "scripts" / "test_connections.py"
        with open(connection_tester_file, 'w') as f:
            f.write(connection_tester)
        
    # Continue with all other methods unchanged...
    def _create_orchestration_files(self):
        """Create orchestration-related files."""
        
        # Scheduler
        scheduler_content = '''"""Job Scheduler using APScheduler"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
import logging
import yaml
from typing import Dict, Any, Callable

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Central scheduler for all pipeline jobs."""
    
    def __init__(self, config_path: str = "config/job_schedules.yaml"):
        self.config_path = config_path
        self.jobs_config = self._load_config()
        
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///jobs.db')
        }
        
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }
        
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
    def _load_config(self) -> Dict[str, Any]:
        """Load job schedules from configuration."""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
            
    def start(self):
        """Start the scheduler."""
        self.scheduler.start()
        logger.info("Pipeline scheduler started")
        
    def stop(self):
        """Stop the scheduler."""
        self.scheduler.shutdown()
        logger.info("Pipeline scheduler stopped")
        
    def schedule_job(self, job_config: Dict[str, Any], job_func: Callable):
        """Schedule a single job based on configuration."""
        if job_config.get('enabled', True):
            self.scheduler.add_job(
                func=job_func,
                trigger='cron',
                id=job_config['name'],
                name=job_config['name'],
                cron=job_config['schedule'],
                replace_existing=True
            )
            logger.info(f"Scheduled job: {job_config['name']}")
            
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a specific job."""
        job = self.scheduler.get_job(job_id)
        if job:
            return {
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time,
                'pending': job.pending
            }
        return None
'''
        
        scheduler_file = self.project_root / "src" / "orchestration" / "scheduler.py"
        with open(scheduler_file, 'w') as f:
            f.write(scheduler_content)
        
        # Job Manager
        job_manager_content = '''"""Job execution and tracking manager"""
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import json
import sqlite3

@dataclass
class JobExecution:
    """Represents a single job execution."""
    job_id: str
    job_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class JobManager:
    """Manages job execution tracking and history."""
    
    def __init__(self, db_path: str = "job_history.db"):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        """Initialize the job history database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_executions (
                job_id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status TEXT NOT NULL,
                error_message TEXT,
                metadata TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        
    def start_job(self, job_name: str, metadata: Dict[str, Any] = None) -> str:
        """Record the start of a job execution."""
        job_id = str(uuid.uuid4())
        execution = JobExecution(
            job_id=job_id,
            job_name=job_name,
            start_time=datetime.now(),
            metadata=metadata
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO job_executions 
            (job_id, job_name, start_time, status, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, (
            execution.job_id,
            execution.job_name,
            execution.start_time,
            execution.status,
            json.dumps(execution.metadata) if execution.metadata else None
        ))
        
        conn.commit()
        conn.close()
        
        return job_id
        
    def complete_job(self, job_id: str, status: str = "success", 
                    error_message: str = None):
        """Mark a job as completed."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE job_executions
            SET end_time = ?, status = ?, error_message = ?
            WHERE job_id = ?
        """, (datetime.now(), status, error_message, job_id))
        
        conn.commit()
        conn.close()
        
    def get_job_history(self, job_name: str = None, 
                       limit: int = 100) -> List[Dict[str, Any]]:
        """Get job execution history."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if job_name:
            cursor.execute("""
                SELECT * FROM job_executions
                WHERE job_name = ?
                ORDER BY start_time DESC
                LIMIT ?
            """, (job_name, limit))
        else:
            cursor.execute("""
                SELECT * FROM job_executions
                ORDER BY start_time DESC
                LIMIT ?
            """, (limit,))
            
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
            
        conn.close()
        return results
'''
        
        job_manager_file = self.project_root / "src" / "orchestration" / "job_manager.py"
        with open(job_manager_file, 'w') as f:
            f.write(job_manager_content)
        
        # Dependency Manager
        dependency_manager_content = '''"""Manages job dependencies and workflow orchestration"""
from typing import Dict, List, Set, Optional
import networkx as nx
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DependencyManager:
    """Manages dependencies between pipeline jobs."""
    
    def __init__(self):
        self.dependency_graph = nx.DiGraph()
        self.job_status = {}
        
    def add_job(self, job_name: str, dependencies: List[str] = None):
        """Add a job and its dependencies to the graph."""
        self.dependency_graph.add_node(job_name)
        
        if dependencies:
            for dep in dependencies:
                self.dependency_graph.add_edge(dep, job_name)
                
    def can_run(self, job_name: str) -> bool:
        """Check if a job can run based on its dependencies."""
        if job_name not in self.dependency_graph:
            return True
            
        predecessors = list(self.dependency_graph.predecessors(job_name))
        
        for pred in predecessors:
            if self.job_status.get(pred) != 'success':
                return False
                
        return True
        
    def get_ready_jobs(self) -> List[str]:
        """Get all jobs that are ready to run."""
        ready_jobs = []
        
        for job in self.dependency_graph.nodes():
            if self.job_status.get(job) not in ['success', 'running']:
                if self.can_run(job):
                    ready_jobs.append(job)
                    
        return ready_jobs
        
    def update_job_status(self, job_name: str, status: str):
        """Update the status of a job."""
        self.job_status[job_name] = status
        logger.info(f"Job {job_name} status updated to: {status}")
        
    def get_execution_order(self) -> List[str]:
        """Get the topological order of job execution."""
        try:
            return list(nx.topological_sort(self.dependency_graph))
        except nx.NetworkXError:
            logger.error("Circular dependency detected in job graph")
            raise
            
    def visualize_dependencies(self, output_path: str = "job_dependencies.png"):
        """Create a visualization of the job dependency graph."""
        try:
            import matplotlib.pyplot as plt
            
            pos = nx.spring_layout(self.dependency_graph)
            nx.draw(self.dependency_graph, pos, with_labels=True, 
                   node_color='lightblue', node_size=2000, 
                   font_size=10, font_weight='bold')
            
            plt.title("Job Dependencies")
            plt.savefig(output_path)
            plt.close()
            
            logger.info(f"Dependency graph saved to: {output_path}")
        except ImportError:
            logger.warning("matplotlib not installed, skipping visualization")
'''
        
        dep_manager_file = self.project_root / "src" / "orchestration" / "dependency_manager.py"
        with open(dep_manager_file, 'w') as f:
            f.write(dependency_manager_content)
    
    def _create_data_quality_files(self):
        """Create data quality related files."""
        
        # Data Quality Rules Base
        quality_base_content = '''"""Base classes for data quality rules"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd
from datetime import datetime


class DataQualityRule(ABC):
    """Abstract base class for data quality rules."""
    
    def __init__(self, rule_name: str, severity: str = "warning"):
        self.rule_name = rule_name
        self.severity = severity  # warning, error, critical
        
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate the data against this rule."""
        pass
        
    def log_result(self, result: Dict[str, Any]):
        """Log the validation result."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] Rule: {self.rule_name} - Result: {result}")


class DataQualityValidator:
    """Orchestrates data quality validation."""
    
    def __init__(self):
        self.rules: List[DataQualityRule] = []
        
    def add_rule(self, rule: DataQualityRule):
        """Add a validation rule."""
        self.rules.append(rule)
        
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run all validation rules."""
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_rules': len(self.rules),
            'passed': 0,
            'failed': 0,
            'warnings': 0,
            'errors': 0,
            'rule_results': []
        }
        
        for rule in self.rules:
            rule_result = rule.validate(df)
            rule_result['rule_name'] = rule.rule_name
            rule_result['severity'] = rule.severity
            
            results['rule_results'].append(rule_result)
            
            if rule_result['passed']:
                results['passed'] += 1
            else:
                results['failed'] += 1
                
                if rule.severity == 'warning':
                    results['warnings'] += 1
                elif rule.severity in ['error', 'critical']:
                    results['errors'] += 1
                    
        return results
'''
        
        quality_base_file = self.project_root / "src" / "quality" / "base.py"
        with open(quality_base_file, 'w') as f:
            f.write(quality_base_content)
        
        # Silver Data Quality Rules
        silver_rules_content = '''"""Data quality rules for Silver layer"""
import pandas as pd
from typing import Dict, Any, List
from src.quality.base import DataQualityRule


class NoNullsRule(DataQualityRule):
    """Ensure specified columns have no null values."""
    
    def __init__(self, columns: List[str], severity: str = "error"):
        super().__init__("no_nulls", severity)
        self.columns = columns
        
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for null values in specified columns."""
        null_counts = {}
        total_nulls = 0
        
        for col in self.columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_counts[col] = int(null_count)
                    total_nulls += null_count
                    
        return {
            'passed': total_nulls == 0,
            'message': f"Found {total_nulls} null values" if total_nulls > 0 else "No nulls found",
            'details': null_counts
        }


class UniqueKeyRule(DataQualityRule):
    """Ensure specified columns form a unique key."""
    
    def __init__(self, key_columns: List[str], severity: str = "error"):
        super().__init__("unique_key", severity)
        self.key_columns = key_columns
        
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for duplicate keys."""
        duplicates = df.duplicated(subset=self.key_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        return {
            'passed': duplicate_count == 0,
            'message': f"Found {duplicate_count} duplicate records" if duplicate_count > 0 else "All keys are unique",
            'duplicate_count': int(duplicate_count)
        }


class ReferentialIntegrityRule(DataQualityRule):
    """Check foreign key relationships."""
    
    def __init__(self, foreign_key: str, reference_df: pd.DataFrame, 
                 reference_key: str, severity: str = "error"):
        super().__init__("referential_integrity", severity)
        self.foreign_key = foreign_key
        self.reference_df = reference_df
        self.reference_key = reference_key
        
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check if all foreign keys exist in reference table."""
        fk_values = set(df[self.foreign_key].dropna().unique())
        ref_values = set(self.reference_df[self.reference_key].unique())
        
        missing_keys = fk_values - ref_values
        
        return {
            'passed': len(missing_keys) == 0,
            'message': f"Found {len(missing_keys)} orphaned foreign keys" if missing_keys else "All foreign keys valid",
            'missing_keys': list(missing_keys)[:10]  # Show first 10
        }


class DataTypeRule(DataQualityRule):
    """Ensure columns have expected data types."""
    
    def __init__(self, column_types: Dict[str, str], severity: str = "warning"):
        super().__init__("data_types", severity)
        self.column_types = column_types
        
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data types match expectations."""
        mismatches = {}
        
        for col, expected_type in self.column_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    mismatches[col] = {
                        'expected': expected_type,
                        'actual': actual_type
                    }
                    
        return {
            'passed': len(mismatches) == 0,
            'message': f"Found {len(mismatches)} type mismatches" if mismatches else "All types match",
            'mismatches': mismatches
        }


class ValueRangeRule(DataQualityRule):
    """Check if numeric values are within expected ranges."""
    
    def __init__(self, column_ranges: Dict[str, Dict[str, float]], 
                 severity: str = "warning"):
        super().__init__("value_ranges", severity)
        self.column_ranges = column_ranges
        
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check value ranges."""
        out_of_range = {}
        
        for col, ranges in self.column_ranges.items():
            if col in df.columns:
                min_val = ranges.get('min', float('-inf'))
                max_val = ranges.get('max', float('inf'))
                
                out_of_range_count = ((df[col] < min_val) | (df[col] > max_val)).sum()
                
                if out_of_range_count > 0:
                    out_of_range[col] = {
                        'count': int(out_of_range_count),
                        'percentage': round(out_of_range_count / len(df) * 100, 2)
                    }
                    
        return {
            'passed': len(out_of_range) == 0,
            'message': f"Found out-of-range values in {len(out_of_range)} columns" if out_of_range else "All values in range",
            'details': out_of_range
        }
'''
        
        silver_rules_file = self.project_root / "src" / "quality" / "rules" / "silver_rules.py"
        with open(silver_rules_file, 'w') as f:
            f.write(silver_rules_content)
        
        # Data Profiler
        profiler_content = '''"""Data profiling utilities"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime


class DataProfiler:
    """Profile datasets to understand their characteristics."""
    
    @staticmethod
    def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a comprehensive profile of a DataFrame."""
        profile = {
            'timestamp': datetime.now().isoformat(),
            'shape': {
                'rows': len(df),
                'columns': len(df.columns)
            },
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'columns': {}
        }
        
        for col in df.columns:
            col_profile = DataProfiler._profile_column(df[col])
            profile['columns'][col] = col_profile
            
        return profile
        
    @staticmethod
    def _profile_column(series: pd.Series) -> Dict[str, Any]:
        """Profile a single column."""
        profile = {
            'dtype': str(series.dtype),
            'null_count': int(series.isnull().sum()),
            'null_percentage': round(series.isnull().sum() / len(series) * 100, 2),
            'unique_count': int(series.nunique()),
            'unique_percentage': round(series.nunique() / len(series) * 100, 2)
        }
        
        # Numeric columns
        if pd.api.types.is_numeric_dtype(series):
            profile.update({
                'mean': float(series.mean()) if not series.empty else None,
                'median': float(series.median()) if not series.empty else None,
                'std': float(series.std()) if not series.empty else None,
                'min': float(series.min()) if not series.empty else None,
                'max': float(series.max()) if not series.empty else None,
                'quartiles': {
                    'q1': float(series.quantile(0.25)) if not series.empty else None,
                    'q3': float(series.quantile(0.75)) if not series.empty else None
                }
            })
            
        # String columns
        elif pd.api.types.is_string_dtype(series):
            profile.update({
                'avg_length': series.str.len().mean() if not series.empty else None,
                'max_length': series.str.len().max() if not series.empty else None,
                'min_length': series.str.len().min() if not series.empty else None
            })
            
        # Datetime columns
        elif pd.api.types.is_datetime64_any_dtype(series):
            profile.update({
                'min_date': series.min().isoformat() if not series.empty else None,
                'max_date': series.max().isoformat() if not series.empty else None
            })
            
        # Top values for all columns
        top_values = series.value_counts().head(5).to_dict()
        profile['top_values'] = {str(k): int(v) for k, v in top_values.items()}
        
        return profile
        
    @staticmethod
    def compare_profiles(profile1: Dict[str, Any], 
                        profile2: Dict[str, Any]) -> Dict[str, Any]:
        """Compare two data profiles to identify changes."""
        comparison = {
            'timestamp': datetime.now().isoformat(),
            'shape_changes': {
                'rows_diff': profile2['shape']['rows'] - profile1['shape']['rows'],
                'columns_diff': profile2['shape']['columns'] - profile1['shape']['columns']
            },
            'column_changes': {}
        }
        
        all_columns = set(profile1['columns'].keys()) | set(profile2['columns'].keys())
        
        for col in all_columns:
            if col not in profile1['columns']:
                comparison['column_changes'][col] = {'status': 'added'}
            elif col not in profile2['columns']:
                comparison['column_changes'][col] = {'status': 'removed'}
            else:
                # Compare column statistics
                col1 = profile1['columns'][col]
                col2 = profile2['columns'][col]
                
                changes = {}
                
                # Check null changes
                null_diff = col2['null_percentage'] - col1['null_percentage']
                if abs(null_diff) > 1:  # More than 1% change
                    changes['null_percentage_change'] = round(null_diff, 2)
                    
                # Check unique value changes
                unique_diff = col2['unique_percentage'] - col1['unique_percentage']
                if abs(unique_diff) > 5:  # More than 5% change
                    changes['unique_percentage_change'] = round(unique_diff, 2)
                    
                if changes:
                    comparison['column_changes'][col] = changes
                    
        return comparison
'''
        
        profiler_file = self.project_root / "src" / "quality" / "profilers" / "data_profiler.py"
        with open(profiler_file, 'w') as f:
            f.write(profiler_content)
    
    def _create_state_management_files(self):
        """Create state management related files."""
        
        # Checkpoint Manager
        checkpoint_content = '''"""Checkpoint management for pipeline state tracking"""
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import pickle
import logging

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages checkpoints for pipeline state recovery."""
    
    def __init__(self, checkpoint_dir: str = "data/checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
    def save_checkpoint(self, pipeline_name: str, state: Dict[str, Any], 
                       checkpoint_id: Optional[str] = None) -> str:
        """Save pipeline state to checkpoint."""
        if not checkpoint_id:
            checkpoint_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        checkpoint_data = {
            'pipeline_name': pipeline_name,
            'checkpoint_id': checkpoint_id,
            'timestamp': datetime.now().isoformat(),
            'state': state
        }
        
        # Save as JSON for readability
        json_path = self.checkpoint_dir / f"{pipeline_name}_{checkpoint_id}.json"
        with open(json_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
            
        # Also save as pickle for complex objects
        pickle_path = self.checkpoint_dir / f"{pipeline_name}_{checkpoint_id}.pkl"
        with open(pickle_path, 'wb') as f:
            pickle.dump(checkpoint_data, f)
            
        logger.info(f"Checkpoint saved: {checkpoint_id}")
        return checkpoint_id
        
    def load_checkpoint(self, pipeline_name: str, 
                       checkpoint_id: Optional[str] = None) -> Dict[str, Any]:
        """Load pipeline state from checkpoint."""
        if not checkpoint_id:
            # Get latest checkpoint
            checkpoints = list(self.checkpoint_dir.glob(f"{pipeline_name}_*.json"))
            if not checkpoints:
                raise ValueError(f"No checkpoints found for pipeline: {pipeline_name}")
            
            latest_checkpoint = max(checkpoints, key=os.path.getctime)
            checkpoint_id = latest_checkpoint.stem.split('_', 1)[1]
            
        # Try loading pickle first (preserves more data types)
        pickle_path = self.checkpoint_dir / f"{pipeline_name}_{checkpoint_id}.pkl"
        if pickle_path.exists():
            with open(pickle_path, 'rb') as f:
                checkpoint_data = pickle.load(f)
        else:
            # Fall back to JSON
            json_path = self.checkpoint_dir / f"{pipeline_name}_{checkpoint_id}.json"
            with open(json_path, 'r') as f:
                checkpoint_data = json.load(f)
                
        logger.info(f"Checkpoint loaded: {checkpoint_id}")
        return checkpoint_data
        
    def list_checkpoints(self, pipeline_name: str = None) -> List[Dict[str, Any]]:
        """List available checkpoints."""
        pattern = f"{pipeline_name}_*.json" if pipeline_name else "*.json"
        checkpoints = []
        
        for checkpoint_file in self.checkpoint_dir.glob(pattern):
            parts = checkpoint_file.stem.split('_', 1)
            checkpoints.append({
                'pipeline_name': parts[0],
                'checkpoint_id': parts[1] if len(parts) > 1 else 'unknown',
                'file_path': str(checkpoint_file),
                'created_at': datetime.fromtimestamp(
                    checkpoint_file.stat().st_ctime
                ).isoformat()
            })
            
        return sorted(checkpoints, key=lambda x: x['created_at'], reverse=True)
        
    def cleanup_old_checkpoints(self, pipeline_name: str, keep_last: int = 10):
        """Remove old checkpoints, keeping only the most recent ones."""
        checkpoints = self.list_checkpoints(pipeline_name)
        
        if len(checkpoints) > keep_last:
            for checkpoint in checkpoints[keep_last:]:
                # Remove both JSON and pickle files
                for ext in ['.json', '.pkl']:
                    file_path = Path(checkpoint['file_path']).with_suffix(ext)
                    if file_path.exists():
                        file_path.unlink()
                        
                logger.info(f"Removed old checkpoint: {checkpoint['checkpoint_id']}")
'''
        
        checkpoint_file = self.project_root / "src" / "state" / "checkpoint_manager.py"
        with open(checkpoint_file, 'w') as f:
            f.write(checkpoint_content)
        
        # Watermark Tracker
        watermark_content = '''"""Watermark tracking for incremental data processing"""
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)


class WatermarkTracker:
    """Tracks high watermarks for incremental data processing."""
    
    def __init__(self, db_path: str = "watermarks.db"):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        """Initialize the watermark database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                pipeline_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                watermark_column TEXT NOT NULL,
                watermark_value TEXT,
                watermark_type TEXT,
                updated_at TIMESTAMP,
                PRIMARY KEY (pipeline_name, table_name, watermark_column)
            )
        """)
        
        conn.commit()
        conn.close()
        
    def get_watermark(self, pipeline_name: str, table_name: str, 
                     watermark_column: str) -> Optional[Union[str, int, datetime]]:
        """Get the current watermark value."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT watermark_value, watermark_type
            FROM watermarks
            WHERE pipeline_name = ? AND table_name = ? AND watermark_column = ?
        """, (pipeline_name, table_name, watermark_column))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            value, value_type = result
            
            # Convert to appropriate type
            if value_type == 'datetime':
                return datetime.fromisoformat(value)
            elif value_type == 'int':
                return int(value)
            else:
                return value
                
        return None
        
    def set_watermark(self, pipeline_name: str, table_name: str, 
                     watermark_column: str, watermark_value: Union[str, int, datetime]):
        """Update the watermark value."""
        # Determine value type
        if isinstance(watermark_value, datetime):
            value_type = 'datetime'
            value_str = watermark_value.isoformat()
        elif isinstance(watermark_value, int):
            value_type = 'int'
            value_str = str(watermark_value)
        else:
            value_type = 'string'
            value_str = str(watermark_value)
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO watermarks
            (pipeline_name, table_name, watermark_column, watermark_value, 
             watermark_type, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (pipeline_name, table_name, watermark_column, value_str, 
              value_type, datetime.now()))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Watermark updated for {pipeline_name}.{table_name}: "
                   f"{watermark_column} = {watermark_value}")
        
    def get_all_watermarks(self, pipeline_name: str = None) -> List[Dict[str, Any]]:
        """Get all watermarks, optionally filtered by pipeline."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if pipeline_name:
            cursor.execute("""
                SELECT * FROM watermarks
                WHERE pipeline_name = ?
                ORDER BY updated_at DESC
            """, (pipeline_name,))
        else:
            cursor.execute("""
                SELECT * FROM watermarks
                ORDER BY updated_at DESC
            """)
            
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
            
        conn.close()
        return results
        
    def build_incremental_query(self, base_query: str, table_name: str,
                               watermark_column: str, pipeline_name: str) -> str:
        """Build an incremental query using the stored watermark."""
        watermark = self.get_watermark(pipeline_name, table_name, watermark_column)
        
        if watermark:
            if isinstance(watermark, datetime):
                watermark_str = f"'{watermark.isoformat()}'"
            elif isinstance(watermark, str):
                watermark_str = f"'{watermark}'"
            else:
                watermark_str = str(watermark)
                
            # Add WHERE clause for incremental load
            if 'WHERE' in base_query.upper():
                query = f"{base_query} AND {watermark_column} > {watermark_str}"
            else:
                query = f"{base_query} WHERE {watermark_column} > {watermark_str}"
                
            logger.info(f"Incremental query built with watermark: {watermark}")
        else:
            query = base_query
            logger.info("No watermark found, performing full load")
            
        return query
'''
        
        watermark_file = self.project_root / "src" / "state" / "watermark_tracker.py"
        with open(watermark_file, 'w') as f:
            f.write(watermark_content)
    
    def _create_security_files(self):
        """Create security-related files."""
        
        # Vault Client
        vault_content = '''"""Secret management using HashiCorp Vault or environment variables"""
import os
import hvac
from typing import Dict, Any, Optional
import logging
from cryptography.fernet import Fernet
import json
import base64

logger = logging.getLogger(__name__)


class SecretManager:
    """Manages secrets using Vault or environment variables as fallback."""
    
    def __init__(self, vault_url: Optional[str] = None, 
                 vault_token: Optional[str] = None):
        self.vault_url = vault_url or os.getenv('VAULT_URL')
        self.vault_token = vault_token or os.getenv('VAULT_TOKEN')
        self.vault_client = None
        
        if self.vault_url and self.vault_token:
            try:
                self.vault_client = hvac.Client(
                    url=self.vault_url,
                    token=self.vault_token
                )
                if self.vault_client.is_authenticated():
                    logger.info("Successfully connected to Vault")
                else:
                    logger.warning("Vault authentication failed, falling back to env vars")
                    self.vault_client = None
            except Exception as e:
                logger.warning(f"Vault connection failed: {e}, falling back to env vars")
                self.vault_client = None
        else:
            logger.info("Using environment variables for secrets")
            
    def get_secret(self, secret_path: str, key: Optional[str] = None) -> Any:
        """Get a secret from Vault or environment variables."""
        if self.vault_client:
            try:
                response = self.vault_client.secrets.kv.v2.read_secret_version(
                    path=secret_path
                )
                data = response['data']['data']
                return data.get(key) if key else data
            except Exception as e:
                logger.error(f"Failed to read secret from Vault: {e}")
                
        # Fallback to environment variables
        env_key = secret_path.upper().replace('/', '_')
        if key:
            env_key = f"{env_key}_{key.upper()}"
            
        value = os.getenv(env_key)
        if not value:
            raise ValueError(f"Secret not found: {secret_path}")
            
        return value
        
    def set_secret(self, secret_path: str, data: Dict[str, Any]):
        """Store a secret in Vault."""
        if self.vault_client:
            try:
                self.vault_client.secrets.kv.v2.create_or_update_secret(
                    path=secret_path,
                    secret=data
                )
                logger.info(f"Secret stored successfully: {secret_path}")
            except Exception as e:
                logger.error(f"Failed to store secret in Vault: {e}")
                raise
        else:
            logger.warning("Vault not available, cannot store secrets")
            
    def get_database_config(self, db_name: str) -> Dict[str, Any]:
        """Get database configuration with decrypted credentials."""
        config = {
            'host': self.get_secret(f'databases/{db_name}', 'host'),
            'port': int(self.get_secret(f'databases/{db_name}', 'port')),
            'database': self.get_secret(f'databases/{db_name}', 'database'),
            'username': self.get_secret(f'databases/{db_name}', 'username'),
            'password': self.get_secret(f'databases/{db_name}', 'password')
        }
        
        return config


class DataEncryption:
    """Handles encryption/decryption of sensitive data."""
    
    def __init__(self, key: Optional[bytes] = None):
        if key:
            self.cipher = Fernet(key)
        else:
            # Generate or load encryption key
            key_path = os.getenv('ENCRYPTION_KEY_PATH', '.encryption_key')
            if os.path.exists(key_path):
                with open(key_path, 'rb') as f:
                    key = f.read()
            else:
                key = Fernet.generate_key()
                with open(key_path, 'wb') as f:
                    f.write(key)
                logger.info(f"Encryption key generated and saved to {key_path}")
                
            self.cipher = Fernet(key)
            
    def encrypt_string(self, plaintext: str) -> str:
        """Encrypt a string and return base64 encoded result."""
        encrypted = self.cipher.encrypt(plaintext.encode())
        return base64.b64encode(encrypted).decode()
        
    def decrypt_string(self, ciphertext: str) -> str:
        """Decrypt a base64 encoded encrypted string."""
        encrypted = base64.b64decode(ciphertext.encode())
        decrypted = self.cipher.decrypt(encrypted)
        return decrypted.decode()
        
    def encrypt_dataframe_column(self, df, column: str):
        """Encrypt a specific column in a DataFrame."""
        df[f'{column}_encrypted'] = df[column].apply(
            lambda x: self.encrypt_string(str(x)) if pd.notna(x) else None
        )
        df.drop(columns=[column], inplace=True)
        return df
        
    def decrypt_dataframe_column(self, df, column: str):
        """Decrypt a specific column in a DataFrame."""
        original_column = column.replace('_encrypted', '')
        df[original_column] = df[column].apply(
            lambda x: self.decrypt_string(x) if pd.notna(x) else None
        )
        df.drop(columns=[column], inplace=True)
        return df
'''
        
        vault_file = self.project_root / "src" / "security" / "vault_client.py"
        with open(vault_file, 'w') as f:
            f.write(vault_content)
    
    def _create_performance_files(self):
        """Create performance optimization files."""
        
        # Query Optimizer
        optimizer_content = '''"""SQL query optimization utilities"""
import re
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class QueryOptimizer:
    """Optimizes SQL queries for better performance."""
    
    @staticmethod
    def analyze_query(query: str) -> Dict[str, Any]:
        """Analyze a query and provide optimization suggestions."""
        suggestions = []
        
        # Check for SELECT *
        if re.search(r'SELECT\s+\*', query, re.IGNORECASE):
            suggestions.append({
                'type': 'select_star',
                'severity': 'medium',
                'suggestion': 'Avoid SELECT *, specify only needed columns'
            })
            
        # Check for missing WHERE clause in DELETE/UPDATE
        if re.search(r'(DELETE|UPDATE)\s+', query, re.IGNORECASE):
            if not re.search(r'WHERE\s+', query, re.IGNORECASE):
                suggestions.append({
                    'type': 'missing_where',
                    'severity': 'critical',
                    'suggestion': 'Add WHERE clause to avoid updating/deleting all rows'
                })
                
        # Check for functions in WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:ORDER|GROUP|LIMIT|$)', 
                               query, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            if re.search(r'(UPPER|LOWER|SUBSTRING|DATEPART)\s*\(', 
                        where_clause, re.IGNORECASE):
                suggestions.append({
                    'type': 'function_in_where',
                    'severity': 'high',
                    'suggestion': 'Avoid functions in WHERE clause, they prevent index usage'
                })
                
        # Check for OR conditions
        if re.search(r'WHERE.*\sOR\s', query, re.IGNORECASE):
            suggestions.append({
                'type': 'or_condition',
                'severity': 'medium',
                'suggestion': 'Consider using UNION instead of OR for better performance'
            })
            
        # Check for NOT IN
        if re.search(r'NOT\s+IN\s*\(', query, re.IGNORECASE):
            suggestions.append({
                'type': 'not_in',
                'severity': 'medium',
                'suggestion': 'Consider using NOT EXISTS instead of NOT IN'
            })
            
        return {
            'original_query': query,
            'suggestions': suggestions,
            'optimization_score': max(0, 100 - len(suggestions) * 20)
        }
        
    @staticmethod
    def add_query_hints(query: str, hints: List[str]) -> str:
        """Add optimization hints to a query."""
        # This is database-specific, example for SQL Server
        if hints:
            hint_str = ' '.join([f'OPTION ({hint})' for hint in hints])
            
            # Add hints at the end of the query
            if query.rstrip().endswith(';'):
                query = query.rstrip(';')
                
            query = f"{query} {hint_str};"
            
        return query
        
    @staticmethod
    def generate_index_recommendations(table_stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate index recommendations based on table statistics."""
        recommendations = []
        
        # Example logic - would need actual query patterns and statistics
        for column, stats in table_stats.get('columns', {}).items():
            if stats.get('cardinality', 0) > 1000 and stats.get('used_in_where', False):
                recommendations.append({
                    'type': 'index',
                    'column': column,
                    'reason': 'High cardinality column frequently used in WHERE clause',
                    'sql': f"CREATE INDEX idx_{table_stats['table_name']}_{column} "
                          f"ON {table_stats['table_name']} ({column});"
                })
                
        return recommendations


class PartitioningStrategy:
    """Handles data partitioning for improved performance."""
    
    @staticmethod
    def suggest_partition_key(df_profile: Dict[str, Any]) -> Optional[str]:
        """Suggest the best column for partitioning based on data profile."""
        candidates = []
        
        for column, profile in df_profile.get('columns', {}).items():
            score = 0
            
            # Date columns are good partition keys
            if profile['dtype'] in ['datetime64[ns]', 'object']:
                if 'date' in column.lower() or 'time' in column.lower():
                    score += 50
                    
            # Low cardinality columns are good for partitioning
            cardinality = profile.get('unique_count', 0)
            if 10 <= cardinality <= 1000:
                score += 30
                
            # Columns with even distribution
            if profile.get('distribution_score', 0) > 0.7:
                score += 20
                
            if score > 0:
                candidates.append({'column': column, 'score': score})
                
        if candidates:
            best_candidate = max(candidates, key=lambda x: x['score'])
            return best_candidate['column']
            
        return None
        
    @staticmethod
    def calculate_partition_size(total_rows: int, target_size_mb: int = 128) -> int:
        """Calculate optimal number of partitions."""
        # Assuming average row size of 1KB
        avg_row_size_kb = 1
        total_size_mb = (total_rows * avg_row_size_kb) / 1024
        
        num_partitions = max(1, int(total_size_mb / target_size_mb))
        
        return num_partitions


class CacheManager:
    """Manages caching for frequently accessed data."""
    
    def __init__(self, cache_dir: str = "cache", max_size_gb: float = 10):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        self.cache_index = self._load_cache_index()
        
    def _load_cache_index(self) -> Dict[str, Any]:
        """Load the cache index."""
        index_path = self.cache_dir / "cache_index.json"
        if index_path.exists():
            with open(index_path, 'r') as f:
                return json.load(f)
        return {}
        
    def _save_cache_index(self):
        """Save the cache index."""
        index_path = self.cache_dir / "cache_index.json"
        with open(index_path, 'w') as f:
            json.dump(self.cache_index, f)
            
    def get(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Get data from cache."""
        if cache_key in self.cache_index:
            cache_info = self.cache_index[cache_key]
            cache_path = self.cache_dir / cache_info['filename']
            
            if cache_path.exists():
                # Update access time
                cache_info['last_accessed'] = datetime.now().isoformat()
                self._save_cache_index()
                
                return pd.read_parquet(cache_path)
                
        return None
        
    def put(self, cache_key: str, data: pd.DataFrame, ttl_hours: int = 24):
        """Store data in cache."""
        # Check cache size
        self._evict_if_needed()
        
        filename = f"{cache_key}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        cache_path = self.cache_dir / filename
        
        data.to_parquet(cache_path)
        
        self.cache_index[cache_key] = {
            'filename': filename,
            'size_bytes': cache_path.stat().st_size,
            'created_at': datetime.now().isoformat(),
            'last_accessed': datetime.now().isoformat(),
            'ttl_hours': ttl_hours
        }
        
        self._save_cache_index()
        
    def _evict_if_needed(self):
        """Evict old cache entries if size limit exceeded."""
        total_size = sum(entry['size_bytes'] for entry in self.cache_index.values())
        
        if total_size > self.max_size_bytes:
            # Sort by last accessed time
            sorted_entries = sorted(
                self.cache_index.items(),
                key=lambda x: x[1]['last_accessed']
            )
            
            # Remove oldest entries
            while total_size > self.max_size_bytes * 0.8:  # Keep 20% buffer
                cache_key, cache_info = sorted_entries.pop(0)
                
                cache_path = self.cache_dir / cache_info['filename']
                if cache_path.exists():
                    cache_path.unlink()
                    
                total_size -= cache_info['size_bytes']
                del self.cache_index[cache_key]
                
            self._save_cache_index()
'''
        
        optimizer_file = self.project_root / "src" / "performance" / "query_optimizer.py"
        with open(optimizer_file, 'w') as f:
            f.write(optimizer_content)
            
    def _create_documentation(self):
        """Create documentation files."""
        
        readme_content = f"""# {self.project_name.replace('_', ' ').title()}

## Overview
Data pipeline project implementing Medallion architecture (Bronze, Silver, Gold layers) 
with support for multiple data sources including Oracle, SQL Server, MongoDB, and Databricks.

## Architecture

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion with minimal transformations
- **Silver Layer**: Cleaned, validated, and standardized data
- **Gold Layer**: Business-ready aggregated data and metrics

### Data Sources
- Oracle Database
- SQL Server (SSMS)
- MongoDB
- Databricks

## Project Structure
```
{self.project_name}/
├── src/                    # Source code
│   ├── connectors/        # Database connectors
│   ├── pipelines/         # ETL pipelines for each layer
│   ├── transformations/   # Data transformation logic
│   ├── orchestration/     # Job scheduling and workflow management
│   ├── quality/           # Data quality and validation
│   ├── state/            # State management and checkpointing
│   ├── security/         # Security and credential management
│   ├── performance/      # Performance optimization utilities
│   ├── utils/            # Utility functions
│   └── schemas/          # Data schema definitions
├── config/               # Configuration files
│   ├── environments/     # Environment-specific configs
│   └── pipelines/        # Pipeline configurations
├── tests/               # Unit and integration tests
├── notebooks/           # Databricks/Jupyter notebooks
├── docs/               # Documentation
│   ├── runbooks/       # Operational runbooks
│   └── adr/           # Architecture decision records
├── data/               # Local data directory (dev only)
├── logs/               # Application logs
└── scripts/            # Deployment and maintenance scripts
```

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Copy `.env.template` to `.env` and update with your credentials:
   ```bash
   cp .env.template .env
   ```

3. Set environment variables for credentials:
   ```bash
   export ORACLE_USER=your_username
   export ORACLE_PASSWORD=your_password
   # ... other credentials
   ```

4. Test database connections:
   ```bash
   python scripts/test_connections.py
   ```

5. Run a sample pipeline:
   ```python
   from src.connectors.oracle import OracleConnector
   from src.pipelines.bronze import BronzePipeline
   
   # Initialize connector and pipeline
   oracle = OracleConnector()
   pipeline = BronzePipeline(oracle, "data/bronze")
   
   # Ingest data
   result = pipeline.ingest_table("your_table_name")
   ```

## Configuration

All database connections are configured through environment variables. See `.env.example` for a complete list with documentation.

### Required Environment Variables

- **Oracle**: ORACLE_HOST, ORACLE_USER, ORACLE_PASSWORD, ORACLE_SERVICE_NAME
- **SQL Server**: SQLSERVER_SERVER, SQLSERVER_DATABASE, SQLSERVER_USER, SQLSERVER_PASSWORD
- **MongoDB**: MONGO_CONNECTION_STRING
- **Databricks**: DATABRICKS_HOST, DATABRICKS_TOKEN

## Key Features

### Orchestration (No Airflow)
- Custom scheduler using APScheduler
- Job dependency management
- Workflow tracking and monitoring

### Data Quality
- Comprehensive data quality rules
- Data profiling and comparison
- Automated validation framework

### State Management
- Checkpoint system for failure recovery
- Watermark tracking for incremental loads
- State persistence across runs

### Security
- Integration with HashiCorp Vault
- Encryption for sensitive data
- Secure credential management through environment variables

### Performance
- Query optimization recommendations
- Data partitioning strategies
- Intelligent caching system

## Development

- Follow PEP 8 style guidelines
- Write unit tests for new features
- Document all functions and classes
- Use type hints for better code clarity

## Operations

See the `docs/runbooks/` directory for:
- Incident response procedures
- Recovery procedures
- Monitoring guidelines

## Architecture Decisions

See the `docs/adr/` directory for documented architecture decisions.

## Created
{datetime.now().strftime('%Y-%m-%d')}
"""
        
        readme_file = self.project_root / "README.md"
        with open(readme_file, 'w') as f:
            f.write(readme_content)
    
    def _create_runbooks(self):
        """Create operational runbook documentation."""
        
        # Incident Response Runbook
        incident_response = """# Incident Response Runbook

## Overview
This document outlines the procedures for responding to incidents in the data pipeline system.

## Incident Severity Levels

### Severity 1 (Critical)
- Complete pipeline failure
- Data loss or corruption
- Security breach
- Production database unavailable

### Severity 2 (High)
- Partial pipeline failure
- Significant performance degradation
- Data quality issues affecting downstream systems

### Severity 3 (Medium)
- Minor performance issues
- Non-critical job failures
- Data quality warnings

### Severity 4 (Low)
- Cosmetic issues
- Documentation errors
- Development environment issues

## Response Procedures

### Initial Response (All Severities)
1. **Acknowledge the incident**
   - Log incident in tracking system
   - Notify relevant stakeholders
   
2. **Assess impact**
   - Identify affected systems/data
   - Determine severity level
   - Estimate business impact

3. **Gather information**
   ```bash
   # Check pipeline status
   python -m src.orchestration.job_manager status
   
   # View recent logs
   tail -f logs/pipeline.log
   
   # Check system resources
   df -h
   free -m
   ```

### Severity 1 & 2 Response

#### Immediate Actions
1. **Notify incident commander**
2. **Open bridge call if needed**
3. **Begin investigation**

#### Investigation Steps
1. **Check recent changes**
   ```bash
   git log --oneline -10
   ```

2. **Review job history**
   ```python
   from src.orchestration.job_manager import JobManager
   jm = JobManager()
   jm.get_job_history(limit=50)
   ```

3. **Examine checkpoints**
   ```python
   from src.state.checkpoint_manager import CheckpointManager
   cm = CheckpointManager()
   cm.list_checkpoints()
   ```

4. **Database connectivity**
   ```python
   # Test each database connection
   from src.connectors.oracle import OracleConnector
   oracle = OracleConnector()
   oracle.connect()
   ```

### Common Issues and Solutions

#### Pipeline Job Failure
**Symptoms**: Job shows as failed in job history

**Actions**:
1. Check job logs for error messages
2. Verify source system availability
3. Check for schema changes
4. Review recent code changes
5. Attempt manual retry:
   ```python
   scheduler.run_job(job_name, force=True)
   ```

#### Data Quality Failures
**Symptoms**: Quality checks failing, alerts triggered

**Actions**:
1. Review quality rule violations
2. Check source data for anomalies
3. Verify transformation logic
4. Consider temporarily relaxing rules if appropriate

#### Performance Degradation
**Symptoms**: Jobs taking longer than usual

**Actions**:
1. Check database query performance
2. Review system resources
3. Analyze query execution plans
4. Consider scaling resources

## Recovery Procedures

### From Checkpoint
```python
from src.state.checkpoint_manager import CheckpointManager
from src.pipelines.bronze import BronzePipeline

# Load checkpoint
cm = CheckpointManager()
checkpoint = cm.load_checkpoint('bronze_pipeline')

# Resume from checkpoint
pipeline = BronzePipeline()
pipeline.resume_from_checkpoint(checkpoint)
```

### Incremental Reload
```python
from src.state.watermark_tracker import WatermarkTracker

# Reset watermark to specific date
wt = WatermarkTracker()
wt.set_watermark('bronze_pipeline', 'orders', 'modified_date', 
                 datetime(2024, 1, 1))
```

### Full Reload
```bash
# Backup current data
./scripts/maintenance/backup_layer.sh bronze

# Clear and reload
python -m src.pipelines.bronze.full_reload --confirm
```

## Post-Incident

### Required Actions
1. Update incident ticket with resolution
2. Document root cause
3. Create follow-up tasks for permanent fixes
4. Update runbooks if new issue type

### Post-Mortem (Sev 1 & 2)
- Schedule within 48 hours
- Include all stakeholders
- Document lessons learned
- Create action items

## Contact Information

### On-Call Rotation
- Primary: Check PagerDuty
- Secondary: Check PagerDuty
- Manager: [Manager Name]

### Escalation Path
1. On-call engineer
2. Team lead
3. Engineering manager
4. Director of Engineering

### External Contacts
- Oracle DBA Team: oracle-support@company.com
- SQL Server Team: sqlserver-support@company.com
- Infrastructure: infra-support@company.com
"""
        
        incident_file = self.project_root / "docs" / "runbooks" / "incident_response.md"
        with open(incident_file, 'w') as f:
            f.write(incident_response)
        
        # Recovery Procedures
        recovery_procedures = """# Recovery Procedures Runbook

## Overview
This document provides step-by-step procedures for recovering from various failure scenarios.

## Failure Scenarios

### 1. Complete Pipeline Failure

#### Symptoms
- All jobs failing
- No data flowing through pipeline
- Multiple system alerts

#### Recovery Steps

1. **Stop all running jobs**
   ```python
   from src.orchestration.scheduler import PipelineScheduler
   scheduler = PipelineScheduler()
   scheduler.stop()
   ```

2. **Check system health**
   ```bash
   # Database connections
   python scripts/test_connections.py
   
   # Disk space
   df -h /data
   
   # Memory
   free -m
   ```

3. **Review recent changes**
   ```bash
   # Check deployment history
   cat logs/deployment.log
   
   # Git changes
   git log --since="2 days ago"
   ```

4. **Restart services**
   ```bash
   # Restart in safe mode
   python -m src.main --safe-mode
   
   # Monitor startup
   tail -f logs/pipeline.log
   ```

### 2. Partial Data Loss

#### Identifying Missing Data

```python
from src.quality.profilers import DataProfiler
from datetime import datetime, timedelta

# Check data gaps
profiler = DataProfiler()
for date in last_7_days:
    profile = profiler.profile_layer('bronze', date)
    print(f"{date}: {profile['record_count']} records")
```

#### Recovery Options

**Option 1: From Checkpoint**
```python
from src.state.checkpoint_manager import CheckpointManager

cm = CheckpointManager()
# List available checkpoints
checkpoints = cm.list_checkpoints('silver_pipeline')

# Restore from checkpoint
checkpoint = cm.load_checkpoint('silver_pipeline', '20240115_120000')
```

**Option 2: Reprocess from Bronze**
```python
from src.pipelines.silver import SilverPipeline
from datetime import datetime

pipeline = SilverPipeline()
pipeline.reprocess_date_range(
    start_date=datetime(2024, 1, 10),
    end_date=datetime(2024, 1, 15)
)
```

**Option 3: Reload from Source**
```python
from src.pipelines.bronze import BronzePipeline

pipeline = BronzePipeline()
tables = ['orders', 'customers', 'products']

for table in tables:
    pipeline.reload_table(
        table_name=table,
        start_date=missing_data_start,
        end_date=missing_data_end
    )
```

### 3. Corrupted Data

#### Detection
```python
from src.quality.rules import DataQualityValidator

validator = DataQualityValidator()
results = validator.validate_layer('silver')

if results['errors'] > 0:
    print("Data corruption detected!")
    print(results['rule_results'])
```

#### Remediation

1. **Quarantine corrupted data**
   ```python
   from src.utils.data_quarantine import quarantine_data
   
   quarantine_data(
       layer='silver',
       table='orders',
       condition="order_amount < 0 OR order_amount > 1000000"
   )
   ```

2. **Identify root cause**
   - Check transformation logs
   - Review source data quality
   - Validate business rules

3. **Reprocess clean data**
   ```python
   # After fixing the issue
   pipeline.reprocess_quarantined_data()
   ```

### 4. Schema Changes

#### Handling Breaking Changes

1. **Detect schema differences**
   ```python
   from src.schemas.registry import SchemaRegistry
   
   registry = SchemaRegistry()
   changes = registry.compare_schemas('orders', 'v1', 'v2')
   ```

2. **Create migration plan**
   ```python
   from src.schemas.migrations import create_migration
   
   migration = create_migration(
       table='orders',
       from_version='v1',
       to_version='v2',
       changes=changes
   )
   ```

3. **Execute migration**
   ```bash
   python scripts/migration/run_migration.py \
       --table orders \
       --migration v1_to_v2 \
       --dry-run
   
   # If dry run successful
   python scripts/migration/run_migration.py \
       --table orders \
       --migration v1_to_v2 \
       --execute
   ```

## Backup and Restore

### Creating Backups

```bash
# Automated daily backups
./scripts/maintenance/backup_all_layers.sh

# Manual backup
python -m src.utils.backup create \
    --layer gold \
    --tables all \
    --compress
```

### Restoring from Backup

```bash
# List available backups
python -m src.utils.backup list --layer gold

# Restore specific backup
python -m src.utils.backup restore \
    --backup-id 20240115_gold_daily \
    --target-layer gold_restore \
    --verify
```

## Monitoring Recovery

### Key Metrics to Track

1. **Data Completeness**
   ```python
   from src.utils.monitoring import RecoveryMonitor
   
   monitor = RecoveryMonitor()
   monitor.track_recovery_progress(
       start_time=recovery_start,
       expected_records=1000000
   )
   ```

2. **Performance Metrics**
   - Processing rate (records/second)
   - Memory usage
   - CPU utilization
   - I/O wait time

3. **Quality Metrics**
   - Validation pass rate
   - Data freshness
   - Schema compliance

## Validation After Recovery

### Data Validation Checklist

- [ ] Row counts match expected
- [ ] No data gaps in time series
- [ ] Key relationships intact
- [ ] Business rules satisfied
- [ ] Performance back to normal
- [ ] All quality checks passing

### Sign-off Procedure

1. Generate recovery report
   ```python
   from src.utils.reports import RecoveryReport
   
   report = RecoveryReport()
   report.generate(
       incident_id='INC-2024-001',
       recovery_actions=actions_taken,
       validation_results=validation_results
   )
   ```

2. Get stakeholder approval
3. Update documentation
4. Close incident ticket

## Preventive Measures

### Regular Maintenance

1. **Weekly Tasks**
   - Review job failure trends
   - Check checkpoint health
   - Validate backup integrity

2. **Monthly Tasks**
   - Test recovery procedures
   - Update runbooks
   - Review capacity planning

3. **Quarterly Tasks**
   - Disaster recovery drill
   - Performance baseline update
   - Security audit
"""
        
        recovery_file = self.project_root / "docs" / "runbooks" / "recovery_procedures.md"
        with open(recovery_file, 'w') as f:
            f.write(recovery_procedures)
        
        # Monitoring Guide
        monitoring_guide = """# Monitoring Guide

## Overview
This guide covers monitoring setup, key metrics, and alerting for the data pipeline system.

## Monitoring Stack

### Components
- **Metrics Collection**: Prometheus
- **Visualization**: Grafana
- **Alerting**: PagerDuty
- **Logging**: ELK Stack

## Key Metrics

### Pipeline Health Metrics

#### Job Execution Metrics
- `pipeline_job_duration_seconds` - Job execution time
- `pipeline_job_status` - Job completion status (success/failure)
- `pipeline_job_records_processed` - Records processed per job
- `pipeline_job_error_rate` - Job failure rate

#### Data Quality Metrics
- `data_quality_rule_violations` - Quality rule failures
- `data_quality_score` - Overall quality score per layer
- `data_freshness_lag_minutes` - Data staleness

#### System Metrics
- `database_connection_pool_size` - Active DB connections
- `memory_usage_percent` - Memory utilization
- `disk_usage_percent` - Disk space usage
- `cpu_usage_percent` - CPU utilization

### Setting Up Monitoring

#### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'data_pipeline'
    static_configs:
      - targets: ['localhost:8000']
```

#### Application Metrics

```python
# src/utils/monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Define metrics
job_counter = Counter('pipeline_jobs_total', 
                     'Total number of pipeline jobs',
                     ['pipeline', 'status'])

job_duration = Histogram('pipeline_job_duration_seconds',
                        'Job execution time',
                        ['pipeline', 'job_name'])

active_connections = Gauge('database_connections_active',
                          'Active database connections',
                          ['database'])
```

### Grafana Dashboards

#### Pipeline Overview Dashboard

Key panels:
1. **Job Success Rate** - Line graph showing success percentage
2. **Processing Volume** - Bar chart of records processed
3. **Error Rate** - Heat map of errors by pipeline/time
4. **System Resources** - CPU, memory, disk usage

#### Data Quality Dashboard

Key panels:
1. **Quality Score Trend** - Line graph per layer
2. **Rule Violations** - Table of recent violations
3. **Data Freshness** - Gauge showing lag time
4. **Schema Compliance** - Pie chart of compliant vs non-compliant

## Alerting Rules

### Critical Alerts (Immediate Response)

```yaml
# alerts.yml
groups:
  - name: critical
    rules:
      - alert: PipelineCompleteFailure
        expr: rate(pipeline_jobs_total{status="failure"}[5m]) == 1
        for: 5m
        annotations:
          summary: "All pipeline jobs failing"
          severity: "critical"
          
      - alert: DatabaseConnectionFailure
        expr: database_connections_active == 0
        for: 1m
        annotations:
          summary: "Database connection lost"
          severity: "critical"
          
      - alert: DiskSpaceCritical
        expr: disk_usage_percent > 90
        for: 5m
        annotations:
          summary: "Disk space critical"
          severity: "critical"
```

### Warning Alerts (Business Hours)

```yaml
      - alert: HighErrorRate
        expr: rate(pipeline_job_error_rate[15m]) > 0.1
        for: 15m
        annotations:
          summary: "Error rate above 10%"
          severity: "warning"
          
      - alert: DataQualityDegraded
        expr: data_quality_score < 0.8
        for: 30m
        annotations:
          summary: "Data quality below threshold"
          severity: "warning"
          
      - alert: ProcessingDelayed
        expr: data_freshness_lag_minutes > 60
        for: 30m
        annotations:
          summary: "Data processing delayed"
          severity: "warning"
```

## Logging Best Practices

### Log Levels

```python
import logging

# Debug - Detailed diagnostic info
logger.debug(f"Query executed: {query}")

# Info - General informational messages  
logger.info(f"Pipeline started: {pipeline_name}")

# Warning - Warning messages
logger.warning(f"Retry attempt {attempt} for {job_name}")

# Error - Error events
logger.error(f"Failed to process: {error}")

# Critical - Critical problems
logger.critical(f"Database connection lost: {db_name}")
```

### Structured Logging

```python
import structlog

logger = structlog.get_logger()

logger.info("job_completed",
    pipeline="bronze",
    table="orders", 
    records=10000,
    duration=45.2,
    status="success"
)
```

### Log Aggregation Queries

#### Kibana Queries

```
# Failed jobs in last hour
status:"failure" AND @timestamp:[now-1h TO now]

# Slow queries
duration:>10000 AND type:"query_execution"

# Data quality issues
level:"ERROR" AND category:"data_quality"
```

## Performance Monitoring

### Query Performance

```sql
-- Long running queries (PostgreSQL)
SELECT 
    pid,
    now() - pg_stat_activity.query_start AS duration,
    query,
    state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';
```

### Pipeline Bottlenecks

```python
from src.utils.monitoring import PerformanceProfiler

profiler = PerformanceProfiler()

with profiler.profile("bronze_ingestion"):
    # Pipeline code here
    pass

# Get performance report
report = profiler.get_report()
print(report.bottlenecks)
```

## Monitoring Checklist

### Daily Checks
- [ ] Review overnight job failures
- [ ] Check data freshness
- [ ] Verify backup completion
- [ ] Review error logs

### Weekly Checks
- [ ] Analyze performance trends
- [ ] Review capacity utilization
- [ ] Check for anomalies
- [ ] Update alert thresholds

### Monthly Checks
- [ ] Dashboard review and updates
- [ ] Alert rule optimization
- [ ] Log retention cleanup
- [ ] Performance baseline update

## Troubleshooting

### Common Issues

#### High Memory Usage
```bash
# Find memory-intensive processes
ps aux | sort -nrk 4 | head

# Check Python memory usage
python -m memory_profiler src/main.py
```

#### Slow Queries
```python
# Enable query logging
from src.utils.monitoring import slow_query_logger

@slow_query_logger(threshold=10.0)
def execute_query(query):
    # Query execution
    pass
```

#### Connection Pool Exhaustion
```python
# Monitor connection pool
from src.utils.monitoring import connection_monitor

monitor = connection_monitor()
stats = monitor.get_pool_stats()
print(f"Active: {stats['active']}, Idle: {stats['idle']}")
```
"""
        
        monitoring_file = self.project_root / "docs" / "runbooks" / "monitoring_guide.md"
        with open(monitoring_file, 'w') as f:
            f.write(monitoring_guide)
    
    def _create_adrs(self):
        """Create Architecture Decision Records."""
        
        # ADR 001 - Medallion Architecture
        adr_001 = """# ADR-001: Medallion Architecture

## Status
Accepted

## Context
We need a data architecture pattern that supports incremental data quality improvement, 
allows for data reprocessing, and provides clear separation between raw and processed data.

## Decision
We will implement the Medallion Architecture pattern with three layers:
- **Bronze**: Raw data ingestion with minimal transformation
- **Silver**: Cleaned, validated, and standardized data
- **Gold**: Business-ready aggregated data

## Consequences

### Positive
- Clear data lineage and traceability
- Ability to reprocess data from any layer
- Progressive data quality improvement
- Separation of concerns

### Negative
- Additional storage requirements
- More complex pipeline orchestration
- Potential data duplication

## Alternatives Considered
1. **Lambda Architecture**: Too complex for our use case
2. **Direct ETL**: Less flexibility for reprocessing
3. **Data Vault**: Overcomplicated for current requirements
"""
        
        adr_001_file = self.project_root / "docs" / "adr" / "001-medallion-architecture.md"
        with open(adr_001_file, 'w') as f:
            f.write(adr_001)
        
        # ADR 002 - Orchestration Without Airflow
        adr_002 = """# ADR-002: Orchestration Without Airflow

## Status
Accepted

## Context
Apache Airflow cannot be used in our environment due to infrastructure constraints.
We need an alternative orchestration solution that provides scheduling, dependency 
management, and monitoring capabilities.

## Decision
We will build a custom orchestration layer using:
- **APScheduler** for job scheduling
- **NetworkX** for dependency graph management
- **SQLite** for job history and state persistence
- **Custom monitoring** using Prometheus metrics

## Consequences

### Positive
- Full control over orchestration logic
- Lighter weight than Airflow
- No external dependencies
- Easier to customize for our needs

### Negative
- Need to build and maintain custom code
- Less feature-rich than Airflow
- No built-in UI (need to build one)
- Community support limited

## Implementation Details

### Core Components
1. **Scheduler**: APScheduler-based job scheduling
2. **Job Manager**: Tracks execution history and state
3. **Dependency Manager**: Manages job dependencies
4. **Monitoring**: Prometheus metrics and logging

### Key Features
- Cron-based scheduling
- Job dependency resolution
- Failure recovery with checkpoints
- Job history and analytics

## Alternatives Considered
1. **Prefect**: Good option but requires external service
2. **Dagster**: Too heavy for our use case
3. **Luigi**: Limited scheduling capabilities
4. **Cron + Scripts**: Too simplistic, no dependency management
"""
        
        adr_002_file = self.project_root / "docs" / "adr" / "002-orchestration-choice.md"
        with open(adr_002_file, 'w') as f:
            f.write(adr_002)
        
        # ADR 003 - Data Storage Format
        adr_003 = """# ADR-003: Data Storage Format

## Status
Accepted

## Context
We need to choose a storage format for our data layers that balances performance,
compatibility, and storage efficiency.

## Decision
We will use the following storage formats:
- **Bronze Layer**: Parquet files partitioned by ingestion date
- **Silver Layer**: Parquet files partitioned by business date
- **Gold Layer**: Both Parquet and Delta Lake formats

## Rationale

### Parquet for Bronze/Silver
- Columnar format optimized for analytics
- Excellent compression
- Wide tool compatibility
- Schema evolution support

### Delta Lake for Gold
- ACID transactions
- Time travel capabilities
- Update/delete support
- Better for slowly changing dimensions

## Consequences

### Positive
- Optimal query performance
- Reduced storage costs
- Flexibility in tool choice
- Support for both batch and streaming

### Negative
- Need to manage file partitioning
- Potential small file problem
- Delta Lake requires specific tooling

## Implementation Guidelines

### Partitioning Strategy
```python
# Bronze: Partition by ingestion date
/bronze/table_name/year=2024/month=01/day=15/

# Silver: Partition by business date  
/silver/table_name/date=2024-01-15/

# Gold: Partition by relevant business dimension
/gold/sales_summary/year=2024/month=01/
```

### File Size Targets
- Target file size: 128-256 MB
- Use coalesce/repartition as needed
- Implement file compaction jobs

## Alternatives Considered
1. **CSV**: Poor performance and compression
2. **Avro**: Less tool support
3. **ORC**: Similar to Parquet but less adoption
4. **JSON**: Inefficient for analytics
"""
        
        adr_003_file = self.project_root / "docs" / "adr" / "003-data-storage-format.md"
        with open(adr_003_file, 'w') as f:
            f.write(adr_003)
        
        # ADR 004 - Environment Variables for Configuration
        adr_004 = """# ADR-004: Environment Variables for Configuration

## Status
Accepted

## Context
We need a secure, flexible way to manage configuration and credentials across different
environments without hardcoding sensitive information.

## Decision
We will use environment variables exclusively for all configuration, eliminating the need
for separate database configuration files. All credentials and connection details will be
loaded from a `.env` file (in development) or system environment variables (in production).

## Rationale

### Security
- No credentials stored in code or configuration files
- Easy integration with secret management systems
- Clear separation of code and configuration

### Flexibility
- Easy to change configurations without code changes
- Support for different environments (dev, staging, prod)
- Compatible with containerization and cloud deployments

### Simplicity
- Single source of truth for configuration
- Standard approach across the industry
- Native support in all programming languages

## Implementation Details

### Environment Variable Naming Convention
```bash
# Database connections
{DATABASE_TYPE}_{PROPERTY}

# Examples:
ORACLE_HOST=oracle.example.com
ORACLE_USER=myuser
SQLSERVER_SERVER=sqlserver.example.com
MONGO_CONNECTION_STRING=mongodb://localhost:27017/
```

### Loading Order
1. `.env` file (development only)
2. System environment variables
3. Secret management system (if configured)

### Required Variables
Each database connector validates its required environment variables on initialization.

## Consequences

### Positive
- Enhanced security
- Easy deployment configuration
- No configuration files to manage
- Standard DevOps practices

### Negative
- Need to document all environment variables
- Potential for missing configuration errors
- More complex local development setup

## Alternatives Considered
1. **JSON/YAML config files**: Less secure, requires encryption
2. **Hardcoded values**: Major security risk
3. **Database for config**: Circular dependency problem
4. **Mix of files and env vars**: Too complex, inconsistent
"""
        
        adr_004_file = self.project_root / "docs" / "adr" / "004-environment-variables.md"
        with open(adr_004_file, 'w') as f:
            f.write(adr_004)
            
    def _create_requirements(self):
        """Create requirements.txt file."""
        
        requirements = """# Database Connectors
oracledb>=1.4.0
pymongo>=4.5.0
pyodbc>=4.0.39
pandas>=2.0.0

# Databricks
databricks-sdk>=0.12.0
databricks-connect>=13.0.0

# Data Processing
numpy>=1.24.0
pyarrow>=12.0.0

# Orchestration (Airflow alternatives)
apscheduler>=3.10.0
prefect>=2.0.0  # Alternative: lightweight orchestration
networkx>=3.0  # For dependency graphs

# Data Quality
great-expectations>=0.17.0
soda-core>=3.0.0

# Monitoring
prometheus-client>=0.17.0
opentelemetry-api>=1.20.0
structlog>=23.1.0
python-json-logger>=2.0.7

# State Management
redis>=4.6.0
sqlalchemy>=2.0.0

# Secret Management
hvac>=1.2.0  # HashiCorp Vault client
cryptography>=41.0.0

# Utilities
python-dotenv>=1.0.0
pydantic>=2.0.0
pyyaml>=6.0

# Testing
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0

# Development
black>=23.7.0
flake8>=6.1.0
isort>=5.12.0
pre-commit>=3.3.0

# Documentation
mkdocs>=1.5.0
mkdocs-material>=9.0.0
"""
        
        req_file = self.project_root / "requirements.txt"
        with open(req_file, 'w') as f:
            f.write(requirements)
            
    def _create_gitignore(self):
        """Create .gitignore file."""
        
        gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
ENV/
.venv

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Data files
data/
*.csv
*.parquet
*.json
!config/**/*.json

# Logs
logs/
*.log

# Environment variables
.env
.env.local

# Jupyter/Databricks
.ipynb_checkpoints/
*.ipynb

# Testing
.coverage
.pytest_cache/
htmlcov/
.tox/

# OS
.DS_Store
Thumbs.db

# Databricks
.databricks/

# State files
*.db
checkpoints/
cache/

# Security
.encryption_key
vault_token

# Database configuration files (removed)
config/connections/
"""
        
        gitignore_file = self.project_root / ".gitignore"
        with open(gitignore_file, 'w') as f:
            f.write(gitignore_content)
    
    def _create_makefile(self):
        """Create Makefile for common tasks."""
        
        makefile_content = """# Makefile for Data Pipeline Project

.PHONY: help install test lint format run-bronze run-silver run-gold clean test-connections

help:
	@echo "Available commands:"
	@echo "  install         - Install dependencies"
	@echo "  test            - Run tests with coverage"
	@echo "  lint            - Run code linting"
	@echo "  format          - Format code with black"
	@echo "  test-connections - Test all database connections"
	@echo "  run-bronze      - Run bronze pipeline"
	@echo "  run-silver      - Run silver pipeline"
	@echo "  run-gold        - Run gold pipeline"
	@echo "  clean           - Clean up temporary files"

install:
	pip install -r requirements.txt
	pre-commit install

test:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

lint:
	flake8 src/ tests/ --max-line-length=88
	black --check src/ tests/
	isort --check-only src/ tests/

format:
	black src/ tests/
	isort src/ tests/

test-connections:
	python scripts/test_connections.py

run-bronze:
	python -m src.pipelines.bronze.main

run-silver:
	python -m src.pipelines.silver.main

run-gold:
	python -m src.pipelines.gold.main

scheduler-start:
	python -m src.orchestration.scheduler start

scheduler-stop:
	python -m src.orchestration.scheduler stop

quality-check:
	python -m src.quality.validator --layer all

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
"""
        
        makefile_file = self.project_root / "Makefile"
        with open(makefile_file, 'w') as f:
            f.write(makefile_content)
    
    def _create_docker_files(self):
        """Create Docker-related files."""
        
        # Dockerfile
        dockerfile_content = """# Dockerfile for Data Pipeline

FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    unixodbc-dev \\
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p logs data/bronze data/silver data/gold

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "src.main"]
"""
        
        dockerfile_file = self.project_root / "Dockerfile"
        with open(dockerfile_file, 'w') as f:
            f.write(dockerfile_content)
        
        # Docker Compose
        docker_compose_content = """version: '3.8'

services:
  pipeline:
    build: .
    container_name: data_pipeline
    env_file:
      - .env
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    networks:
      - pipeline_network

  scheduler:
    build: .
    container_name: pipeline_scheduler
    command: python -m src.orchestration.scheduler start
    env_file:
      - .env
    volumes:
      - ./logs:/app/logs
      - ./config:/app/config
    depends_on:
      - pipeline
    networks:
      - pipeline_network

  prometheus:
    image: prom/prometheus:latest
    container_name: pipeline_prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./config/prometheus.yml:/etc/prometheus/prometheus.yml
    networks:
      - pipeline_network

  grafana:
    image: grafana/grafana:latest
    container_name: pipeline_grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    networks:
      - pipeline_network

networks:
  pipeline_network:
    driver: bridge
"""
        
        docker_compose_file = self.project_root / "docker-compose.yml"
        with open(docker_compose_file, 'w') as f:
            f.write(docker_compose_content)
        
        # .dockerignore
        dockerignore_content = """# Python
__pycache__/
*.pyc
.pytest_cache/
.coverage
htmlcov/

# Virtual environments
venv/
env/
.venv/

# IDE
.vscode/
.idea/

# Git
.git/
.gitignore

# Documentation
docs/
*.md

# Tests
tests/

# Data (will be mounted as volume)
data/

# Logs (will be mounted as volume)
logs/

# Environment files
.env
.env.*

# Database config files (removed)
config/connections/
"""
        
        dockerignore_file = self.project_root / ".dockerignore"
        with open(dockerignore_file, 'w') as f:
            f.write(dockerignore_content)


def main():
    """Main function to run the project setup."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Create a data pipeline project structure")
    parser.add_argument("--name", default="data_pipeline", help="Project name")
    parser.add_argument("--path", default=None, help="Base path for project creation")
    
    args = parser.parse_args()
    
    # Create project
    setup = ProjectSetup(project_name=args.name, base_path=args.path)
    setup.create_directory_structure()
    
    print("\n📁 Project structure created successfully!")
    print("\n🚀 Next steps:")
    print("1. cd", setup.project_root)
    print("2. python -m venv venv")
    print("3. source venv/bin/activate  # On Windows: venv\\Scripts\\activate")
    print("4. pip install -r requirements.txt")
    print("5. Copy .env.template to .env and update with your credentials")
    print("6. Test your connections: make test-connections")
    print("7. Review the documentation in docs/")
    print("\nHappy coding! 🎉")


if __name__ == "__main__":
    main()
