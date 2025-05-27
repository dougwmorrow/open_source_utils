#!/usr/bin/env python3
"""
Data Pipeline Project Setup Script
Creates a comprehensive directory structure for a data pipeline project
using Medallion architecture with support for multiple data sources.
"""

import os
import json
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
            
            # Configuration files
            "config/connections",
            "config/pipelines",
            "config/databricks",
            
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
            
            # Documentation
            "docs/api",
            "docs/design",
            "docs/guides",
            
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
        
        # Create configuration templates
        self._create_config_templates()
        
        # Create sample files
        self._create_sample_files()
        
        # Create documentation files
        self._create_documentation()
        
        # Create requirements file
        self._create_requirements()
        
        # Create .gitignore
        self._create_gitignore()
        
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
            "tests",
            "tests/unit",
            "tests/integration",
        ]
        
        for dir_name in init_dirs:
            init_file = self.project_root / dir_name / "__init__.py"
            init_file.touch()
            
    def _create_config_templates(self):
        """Create configuration file templates."""
        
        # Database connections config
        db_config = {
            "oracle": {
                "host": "your-oracle-host",
                "port": 1521,
                "service_name": "your-service-name",
                "username": "${ORACLE_USER}",
                "password": "${ORACLE_PASSWORD}"
            },
            "sqlserver": {
                "server": "your-sqlserver-host",
                "database": "your-database",
                "username": "${SQLSERVER_USER}",
                "password": "${SQLSERVER_PASSWORD}",
                "driver": "{ODBC Driver 17 for SQL Server}"
            },
            "mongodb": {
                "connection_string": "mongodb://localhost:27017/",
                "database": "your-database",
                "username": "${MONGO_USER}",
                "password": "${MONGO_PASSWORD}"
            },
            "databricks": {
                "host": "your-databricks-workspace.cloud.databricks.com",
                "token": "${DATABRICKS_TOKEN}",
                "cluster_id": "your-cluster-id"
            }
        }
        
        config_file = self.project_root / "config" / "connections" / "database_config.json"
        with open(config_file, 'w') as f:
            json.dump(db_config, f, indent=4)
            
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
            
    def _create_sample_files(self):
        """Create sample Python files to get started."""
        
        # Create a sample connector
        oracle_connector = '''"""Oracle Database Connector"""
import oracledb
import pandas as pd
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class OracleConnector:
    """Connector for Oracle Database operations."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    def connect(self):
        """Establish connection to Oracle database."""
        try:
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
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
'''
        
        oracle_file = self.project_root / "src" / "connectors" / "oracle" / "oracle_connector.py"
        with open(oracle_file, 'w') as f:
            f.write(oracle_connector)
            
        # Create a sample pipeline
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
│   ├── utils/            # Utility functions
│   └── schemas/          # Data schema definitions
├── config/               # Configuration files
├── tests/               # Unit and integration tests
├── notebooks/           # Databricks/Jupyter notebooks
├── docs/               # Documentation
├── data/               # Local data directory (dev only)
├── logs/               # Application logs
└── scripts/            # Deployment and maintenance scripts
```

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure database connections in `config/connections/database_config.json`

3. Set environment variables for credentials:
   ```bash
   export ORACLE_USER=your_username
   export ORACLE_PASSWORD=your_password
   # ... other credentials
   ```

4. Run a sample pipeline:
   ```python
   from src.connectors.oracle import OracleConnector
   from src.pipelines.bronze import BronzePipeline
   
   # Initialize connector and pipeline
   oracle = OracleConnector(config)
   pipeline = BronzePipeline(oracle, "data/bronze")
   
   # Ingest data
   result = pipeline.ingest_table("your_table_name")
   ```

## Development

- Follow PEP 8 style guidelines
- Write unit tests for new features
- Document all functions and classes
- Use type hints for better code clarity

## Created
{datetime.now().strftime('%Y-%m-%d')}
"""
        
        readme_file = self.project_root / "README.md"
        with open(readme_file, 'w') as f:
            f.write(readme_content)
            
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

# Utilities
python-dotenv>=1.0.0
pydantic>=2.0.0

# Testing
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0

# Logging and Monitoring
structlog>=23.1.0

# Development
black>=23.7.0
flake8>=6.1.0
isort>=5.12.0
pre-commit>=3.3.0
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
"""
        
        gitignore_file = self.project_root / ".gitignore"
        with open(gitignore_file, 'w') as f:
            f.write(gitignore_content)


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
    print("5. Configure your database connections in config/connections/")
    print("\nHappy coding! 🎉")


if __name__ == "__main__":
    main()
