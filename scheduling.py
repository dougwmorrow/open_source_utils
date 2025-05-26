#!/usr/bin/env python3
"""
Pipeline Orchestration Framework
Handles data quality, health checks, and multi-channel notifications
Designed for Cron, Watchdog, or Automic scheduling
"""

import os
import sys
import json
import logging
import smtplib
import requests
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
import pandas as pd
from dataclasses import dataclass, asdict
from enum import Enum


# Configuration Classes
class Severity(Enum):
    """Severity levels for notifications"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class PipelineStatus:
    """Pipeline execution status"""
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str  # SUCCESS, FAILED, RUNNING
    records_processed: int
    errors: List[str]
    warnings: List[str]
    data_quality_score: Optional[float]
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'pipeline_name': self.pipeline_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status,
            'records_processed': self.records_processed,
            'errors': self.errors,
            'warnings': self.warnings,
            'data_quality_score': self.data_quality_score
        }


class NotificationManager:
    """Manages notifications across multiple channels"""
    
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.email_config = self.config.get('email', {})
        self.teams_config = self.config.get('teams', {})
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Setup structured logging"""
        logger = logging.getLogger('PipelineNotifications')
        logger.setLevel(logging.INFO)
        
        # File handler with rotation
        from logging.handlers import RotatingFileHandler
        
        log_dir = Path(self.config.get('log_dir', 'logs/notifications'))
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_dir / 'pipeline_notifications.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        
        # Structured log format
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(pipeline)s | %(message)s | %(extra_data)s',
            defaults={'pipeline': 'SYSTEM', 'extra_data': '{}'}
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def send_notification(self, 
                         severity: Severity,
                         subject: str,
                         message: str,
                         pipeline_status: Optional[PipelineStatus] = None,
                         channels: List[str] = None):
        """Send notification to specified channels"""
        
        if channels is None:
            channels = ['log', 'email', 'teams'] if severity in [Severity.ERROR, Severity.CRITICAL] else ['log']
        
        # Always log
        if 'log' in channels:
            self._log_notification(severity, subject, message, pipeline_status)
        
        # Email for warnings and above
        if 'email' in channels and severity.value in ['WARNING', 'ERROR', 'CRITICAL']:
            self._send_email(severity, subject, message, pipeline_status)
        
        # Teams for errors and critical
        if 'teams' in channels and severity.value in ['ERROR', 'CRITICAL']:
            self._send_teams_notification(severity, subject, message, pipeline_status)
    
    def _log_notification(self, severity, subject, message, pipeline_status):
        """Log notification with structured data"""
        extra_data = {
            'subject': subject,
            'severity': severity.value,
            'pipeline_status': pipeline_status.to_dict() if pipeline_status else {}
        }
        
        log_method = getattr(self.logger, severity.value.lower(), self.logger.info)
        log_method(
            message,
            extra={
                'pipeline': pipeline_status.pipeline_name if pipeline_status else 'SYSTEM',
                'extra_data': json.dumps(extra_data)
            }
        )
    
    def _send_email(self, severity, subject, message, pipeline_status):
        """Send email notification"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[{severity.value}] {subject}"
            msg['From'] = self.email_config['from']
            msg['To'] = ', '.join(self.email_config['to'])
            
            # Create HTML content
            html_content = self._create_email_html(severity, message, pipeline_status)
            msg.attach(MIMEText(html_content, 'html'))
            
            # Send email
            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                if self.email_config.get('use_tls', True):
                    server.starttls()
                if self.email_config.get('username'):
                    server.login(self.email_config['username'], self.email_config['password'])
                server.send_message(msg)
                
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
    
    def _create_email_html(self, severity, message, pipeline_status):
        """Create HTML email content"""
        color_map = {
            'INFO': '#17a2b8',
            'WARNING': '#ffc107',
            'ERROR': '#dc3545',
            'CRITICAL': '#721c24'
        }
        
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="background-color: {color_map.get(severity.value, '#333')}; color: white; padding: 10px;">
                <h2>{severity.value}: Pipeline Notification</h2>
            </div>
            <div style="padding: 20px;">
                <p>{message}</p>
                
                {self._format_pipeline_status_html(pipeline_status) if pipeline_status else ''}
                
                <hr>
                <p style="color: #666; font-size: 12px;">
                    Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                </p>
            </div>
        </body>
        </html>
        """
        return html
    
    def _format_pipeline_status_html(self, status: PipelineStatus):
        """Format pipeline status as HTML table"""
        if not status:
            return ""
            
        return f"""
        <h3>Pipeline Details</h3>
        <table style="border-collapse: collapse; width: 100%;">
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>Pipeline:</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status.pipeline_name}</td>
            </tr>
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>Status:</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status.status}</td>
            </tr>
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>Records Processed:</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status.records_processed:,}</td>
            </tr>
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>Data Quality Score:</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status.data_quality_score:.2%} if status.data_quality_score else 'N/A'}</td>
            </tr>
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;"><strong>Duration:</strong></td>
                <td style="border: 1px solid #ddd; padding: 8px;">{(status.end_time - status.start_time).total_seconds():.1f}s if status.end_time else 'Running'}</td>
            </tr>
        </table>
        
        {self._format_errors_warnings_html(status.errors, status.warnings)}
        """
    
    def _format_errors_warnings_html(self, errors, warnings):
        """Format errors and warnings as HTML"""
        html = ""
        
        if errors:
            html += """
            <h4 style="color: #dc3545;">Errors:</h4>
            <ul style="color: #dc3545;">
            """
            for error in errors:
                html += f"<li>{error}</li>"
            html += "</ul>"
        
        if warnings:
            html += """
            <h4 style="color: #ffc107;">Warnings:</h4>
            <ul style="color: #ffc107;">
            """
            for warning in warnings:
                html += f"<li>{warning}</li>"
            html += "</ul>"
        
        return html
    
    def _send_teams_notification(self, severity, subject, message, pipeline_status):
        """Send Microsoft Teams notification"""
        try:
            webhook_url = self.teams_config['webhook_url']
            
            # Create adaptive card
            card = {
                "@type": "MessageCard",
                "@context": "https://schema.org/extensions",
                "summary": subject,
                "themeColor": self._get_teams_color(severity),
                "title": f"{severity.value}: {subject}",
                "sections": [
                    {
                        "activityTitle": "Pipeline Notification",
                        "activitySubtitle": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "text": message
                    }
                ]
            }
            
            # Add pipeline status if available
            if pipeline_status:
                card["sections"].append({
                    "facts": [
                        {"name": "Pipeline", "value": pipeline_status.pipeline_name},
                        {"name": "Status", "value": pipeline_status.status},
                        {"name": "Records", "value": f"{pipeline_status.records_processed:,}"},
                        {"name": "Quality Score", "value": f"{pipeline_status.data_quality_score:.2%}" if pipeline_status.data_quality_score else "N/A"}
                    ]
                })
            
            response = requests.post(webhook_url, json=card)
            response.raise_for_status()
            
        except Exception as e:
            self.logger.error(f"Failed to send Teams notification: {str(e)}")
    
    def _get_teams_color(self, severity):
        """Get color for Teams card based on severity"""
        return {
            'INFO': '0078D4',
            'WARNING': 'FF8C00',
            'ERROR': 'DC3545',
            'CRITICAL': '8B0000'
        }.get(severity.value, '666666')


class DataQualityChecker:
    """Performs data quality checks on pipeline data"""
    
    def __init__(self):
        self.checks_performed = []
        self.check_results = {}
    
    def check_completeness(self, df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
        """Check if all required columns are present and not null"""
        issues = []
        
        # Check for missing columns
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check for null values in required columns
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_pct = (null_count / len(df)) * 100
                    issues.append(f"Column '{col}' has {null_count} ({null_pct:.1f}%) null values")
        
        self.check_results['completeness'] = len(issues) == 0
        return len(issues) == 0, issues
    
    def check_uniqueness(self, df: pd.DataFrame, unique_columns: List[str]) -> Tuple[bool, List[str]]:
        """Check for duplicate values in columns that should be unique"""
        issues = []
        
        for col in unique_columns:
            if col in df.columns:
                duplicates = df[col].duplicated().sum()
                if duplicates > 0:
                    issues.append(f"Column '{col}' has {duplicates} duplicate values")
        
        # Check for duplicate rows
        duplicate_rows = df.duplicated().sum()
        if duplicate_rows > 0:
            issues.append(f"Found {duplicate_rows} duplicate rows")
        
        self.check_results['uniqueness'] = len(issues) == 0
        return len(issues) == 0, issues
    
    def check_validity(self, df: pd.DataFrame, validation_rules: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Check if data meets validation rules"""
        issues = []
        
        for col, rules in validation_rules.items():
            if col not in df.columns:
                continue
                
            # Check data type
            if 'dtype' in rules:
                expected_dtype = rules['dtype']
                if not df[col].dtype == expected_dtype:
                    issues.append(f"Column '{col}' has dtype {df[col].dtype}, expected {expected_dtype}")
            
            # Check value range
            if 'min' in rules:
                invalid_count = (df[col] < rules['min']).sum()
                if invalid_count > 0:
                    issues.append(f"Column '{col}' has {invalid_count} values below minimum {rules['min']}")
            
            if 'max' in rules:
                invalid_count = (df[col] > rules['max']).sum()
                if invalid_count > 0:
                    issues.append(f"Column '{col}' has {invalid_count} values above maximum {rules['max']}")
            
            # Check allowed values
            if 'allowed_values' in rules:
                invalid_values = ~df[col].isin(rules['allowed_values'])
                invalid_count = invalid_values.sum()
                if invalid_count > 0:
                    issues.append(f"Column '{col}' has {invalid_count} invalid values")
            
            # Check regex pattern
            if 'pattern' in rules and df[col].dtype == 'object':
                invalid_count = ~df[col].str.match(rules['pattern']).sum()
                if invalid_count > 0:
                    issues.append(f"Column '{col}' has {invalid_count} values not matching pattern")
        
        self.check_results['validity'] = len(issues) == 0
        return len(issues) == 0, issues
    
    def check_consistency(self, df: pd.DataFrame, consistency_rules: List[Dict]) -> Tuple[bool, List[str]]:
        """Check cross-field consistency"""
        issues = []
        
        for rule in consistency_rules:
            rule_name = rule.get('name', 'Unnamed rule')
            condition = rule.get('condition')
            
            if condition:
                # Evaluate condition
                try:
                    violations = df.query(f"not ({condition})")
                    if len(violations) > 0:
                        issues.append(f"Consistency rule '{rule_name}' violated by {len(violations)} records")
                except Exception as e:
                    issues.append(f"Error evaluating rule '{rule_name}': {str(e)}")
        
        self.check_results['consistency'] = len(issues) == 0
        return len(issues) == 0, issues
    
    def check_timeliness(self, df: pd.DataFrame, date_column: str, max_age_days: int) -> Tuple[bool, List[str]]:
        """Check if data is recent enough"""
        issues = []
        
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            
            old_records = (df[date_column] < cutoff_date).sum()
            if old_records > 0:
                issues.append(f"Found {old_records} records older than {max_age_days} days")
        
        self.check_results['timeliness'] = len(issues) == 0
        return len(issues) == 0, issues
    
    def calculate_quality_score(self) -> float:
        """Calculate overall data quality score"""
        if not self.check_results:
            return 0.0
        
        passed_checks = sum(1 for result in self.check_results.values() if result)
        total_checks = len(self.check_results)
        
        return passed_checks / total_checks if total_checks > 0 else 0.0


class PipelineHealthMonitor:
    """Monitors pipeline health and performance"""
    
    def __init__(self, metrics_dir: str = "logs/metrics"):
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
    
    def check_pipeline_health(self, pipeline_name: str) -> Dict[str, Any]:
        """Perform health checks on pipeline"""
        health_status = {
            'pipeline_name': pipeline_name,
            'timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        # Check last run time
        last_run = self._get_last_run_time(pipeline_name)
        if last_run:
            hours_since_run = (datetime.now() - last_run).total_seconds() / 3600
            health_status['checks']['last_run'] = {
                'status': 'OK' if hours_since_run < 24 else 'WARNING',
                'value': f"{hours_since_run:.1f} hours ago",
                'threshold': '24 hours'
            }
        
        # Check success rate
        success_rate = self._calculate_success_rate(pipeline_name, days=7)
        health_status['checks']['success_rate'] = {
            'status': 'OK' if success_rate >= 0.95 else 'WARNING' if success_rate >= 0.80 else 'ERROR',
            'value': f"{success_rate:.1%}",
            'threshold': '95%'
        }
        
        # Check average runtime
        avg_runtime = self._calculate_average_runtime(pipeline_name, days=7)
        if avg_runtime:
            health_status['checks']['avg_runtime'] = {
                'status': 'OK',
                'value': f"{avg_runtime:.1f} seconds",
                'threshold': 'N/A'
            }
        
        # Check data volume trends
        volume_trend = self._check_volume_trend(pipeline_name, days=7)
        health_status['checks']['volume_trend'] = volume_trend
        
        return health_status
    
    def _get_last_run_time(self, pipeline_name: str) -> Optional[datetime]:
        """Get the last successful run time"""
        metrics_file = self.metrics_dir / f"{pipeline_name}_metrics.json"
        
        if metrics_file.exists():
            with open(metrics_file, 'r') as f:
                metrics = json.load(f)
                runs = metrics.get('runs', [])
                
                for run in reversed(runs):
                    if run['status'] == 'SUCCESS':
                        return datetime.fromisoformat(run['end_time'])
        
        return None
    
    def _calculate_success_rate(self, pipeline_name: str, days: int) -> float:
        """Calculate success rate over the last N days"""
        metrics_file = self.metrics_dir / f"{pipeline_name}_metrics.json"
        
        if not metrics_file.exists():
            return 0.0
        
        with open(metrics_file, 'r') as f:
            metrics = json.load(f)
            runs = metrics.get('runs', [])
        
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_runs = [
            run for run in runs 
            if datetime.fromisoformat(run['start_time']) > cutoff_date
        ]
        
        if not recent_runs:
            return 0.0
        
        successful_runs = sum(1 for run in recent_runs if run['status'] == 'SUCCESS')
        return successful_runs / len(recent_runs)
    
    def _calculate_average_runtime(self, pipeline_name: str, days: int) -> Optional[float]:
        """Calculate average runtime over the last N days"""
        metrics_file = self.metrics_dir / f"{pipeline_name}_metrics.json"
        
        if not metrics_file.exists():
            return None
        
        with open(metrics_file, 'r') as f:
            metrics = json.load(f)
            runs = metrics.get('runs', [])
        
        cutoff_date = datetime.now() - timedelta(days=days)
        runtimes = []
        
        for run in runs:
            if datetime.fromisoformat(run['start_time']) > cutoff_date and run.get('end_time'):
                start = datetime.fromisoformat(run['start_time'])
                end = datetime.fromisoformat(run['end_time'])
                runtime = (end - start).total_seconds()
                runtimes.append(runtime)
        
        return sum(runtimes) / len(runtimes) if runtimes else None
    
    def _check_volume_trend(self, pipeline_name: str, days: int) -> Dict[str, Any]:
        """Check data volume trends"""
        metrics_file = self.metrics_dir / f"{pipeline_name}_metrics.json"
        
        if not metrics_file.exists():
            return {'status': 'UNKNOWN', 'value': 'No data', 'threshold': 'N/A'}
        
        with open(metrics_file, 'r') as f:
            metrics = json.load(f)
            runs = metrics.get('runs', [])
        
        cutoff_date = datetime.now() - timedelta(days=days)
        volumes = [
            run['records_processed'] 
            for run in runs 
            if datetime.fromisoformat(run['start_time']) > cutoff_date
        ]
        
        if len(volumes) < 2:
            return {'status': 'UNKNOWN', 'value': 'Insufficient data', 'threshold': 'N/A'}
        
        # Calculate trend
        avg_recent = sum(volumes[-3:]) / len(volumes[-3:])
        avg_previous = sum(volumes[:-3]) / len(volumes[:-3])
        
        if avg_previous > 0:
            change_pct = ((avg_recent - avg_previous) / avg_previous) * 100
            
            status = 'OK' if abs(change_pct) < 50 else 'WARNING'
            return {
                'status': status,
                'value': f"{change_pct:+.1f}% change",
                'threshold': '±50%'
            }
        
        return {'status': 'OK', 'value': 'Stable', 'threshold': 'N/A'}
    
    def save_run_metrics(self, pipeline_status: PipelineStatus):
        """Save pipeline run metrics"""
        metrics_file = self.metrics_dir / f"{pipeline_status.pipeline_name}_metrics.json"
        
        # Load existing metrics
        if metrics_file.exists():
            with open(metrics_file, 'r') as f:
                metrics = json.load(f)
        else:
            metrics = {'pipeline_name': pipeline_status.pipeline_name, 'runs': []}
        
        # Add new run
        metrics['runs'].append(pipeline_status.to_dict())
        
        # Keep only last 30 days of data
        cutoff_date = datetime.now() - timedelta(days=30)
        metrics['runs'] = [
            run for run in metrics['runs']
            if datetime.fromisoformat(run['start_time']) > cutoff_date
        ]
        
        # Save updated metrics
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)


class PipelineOrchestrator:
    """Base class for pipeline orchestration with monitoring and notifications"""
    
    def __init__(self, pipeline_name: str, config_path: str):
        self.pipeline_name = pipeline_name
        self.config_path = config_path
        self.notification_manager = NotificationManager(config_path)
        self.health_monitor = PipelineHealthMonitor()
        self.data_quality_checker = DataQualityChecker()
        self.status = None
    
    def run_with_monitoring(self, pipeline_func, *args, **kwargs):
        """Run pipeline with full monitoring and error handling"""
        self.status = PipelineStatus(
            pipeline_name=self.pipeline_name,
            start_time=datetime.now(),
            end_time=None,
            status='RUNNING',
            records_processed=0,
            errors=[],
            warnings=[],
            data_quality_score=None
        )
        
        try:
            # Send start notification
            self.notification_manager.send_notification(
                Severity.INFO,
                f"Pipeline {self.pipeline_name} Started",
                f"Pipeline {self.pipeline_name} has started execution",
                self.status,
                channels=['log']
            )
            
            # Run the pipeline
            result = pipeline_func(*args, **kwargs)
            
            # Extract metrics from result
            if isinstance(result, dict):
                self.status.records_processed = result.get('records_processed', 0)
                
                # Run data quality checks if data is provided
                if 'data' in result and hasattr(result['data'], 'shape'):
                    self._run_quality_checks(result['data'], result.get('quality_config', {}))
            
            # Mark as successful
            self.status.status = 'SUCCESS'
            self.status.end_time = datetime.now()
            
            # Send success notification
            self.notification_manager.send_notification(
                Severity.INFO,
                f"Pipeline {self.pipeline_name} Completed",
                f"Pipeline completed successfully. Processed {self.status.records_processed:,} records.",
                self.status,
                channels=['log']
            )
            
            return result
            
        except Exception as e:
            # Handle pipeline failure
            self.status.status = 'FAILED'
            self.status.end_time = datetime.now()
            self.status.errors.append(f"{type(e).__name__}: {str(e)}")
            self.status.errors.append(traceback.format_exc())
            
            # Send failure notification
            self.notification_manager.send_notification(
                Severity.ERROR,
                f"Pipeline {self.pipeline_name} Failed",
                f"Pipeline failed with error: {str(e)}",
                self.status,
                channels=['log', 'email', 'teams']
            )
            
            raise
            
        finally:
            # Save metrics
            self.health_monitor.save_run_metrics(self.status)
            
            # Run health check
            health_status = self.health_monitor.check_pipeline_health(self.pipeline_name)
            
            # Send health alerts if needed
            for check_name, check_result in health_status['checks'].items():
                if check_result['status'] in ['WARNING', 'ERROR']:
                    self.notification_manager.send_notification(
                        Severity.WARNING if check_result['status'] == 'WARNING' else Severity.ERROR,
                        f"Pipeline Health Alert: {self.pipeline_name}",
                        f"Health check '{check_name}' failed: {check_result['value']} (threshold: {check_result['threshold']})",
                        channels=['log', 'email']
                    )
    
    def _run_quality_checks(self, df, quality_config):
        """Run data quality checks"""
        all_issues = []
        
        # Run completeness check
        if 'required_columns' in quality_config:
            passed, issues = self.data_quality_checker.check_completeness(
                df, quality_config['required_columns']
            )
            all_issues.extend(issues)
        
        # Run uniqueness check
        if 'unique_columns' in quality_config:
            passed, issues = self.data_quality_checker.check_uniqueness(
                df, quality_config['unique_columns']
            )
            all_issues.extend(issues)
        
        # Run validity check
        if 'validation_rules' in quality_config:
            passed, issues = self.data_quality_checker.check_validity(
                df, quality_config['validation_rules']
            )
            all_issues.extend(issues)
        
        # Run consistency check
        if 'consistency_rules' in quality_config:
            passed, issues = self.data_quality_checker.check_consistency(
                df, quality_config['consistency_rules']
            )
            all_issues.extend(issues)
        
        # Run timeliness check
        if 'timeliness' in quality_config:
            passed, issues = self.data_quality_checker.check_timeliness(
                df, 
                quality_config['timeliness']['date_column'],
                quality_config['timeliness']['max_age_days']
            )
            all_issues.extend(issues)
        
        # Calculate quality score
        self.status.data_quality_score = self.data_quality_checker.calculate_quality_score()
        
        # Add issues to warnings
        self.status.warnings.extend(all_issues)
        
        # Send quality alert if score is low
        if self.status.data_quality_score < 0.8:
            self.notification_manager.send_notification(
                Severity.WARNING,
                f"Data Quality Alert: {self.pipeline_name}",
                f"Data quality score: {self.status.data_quality_score:.1%}. Issues found: {len(all_issues)}",
                self.status,
                channels=['log', 'email']
            )


# Example usage functions
def pipeline_runner_decorator(pipeline_name: str, config_path: str):
    """Decorator to add monitoring to any pipeline function"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            orchestrator = PipelineOrchestrator(pipeline_name, config_path)
            return orchestrator.run_with_monitoring(func, *args, **kwargs)
        return wrapper
    return decorator


# Configuration template
def create_notification_config_template(output_path: str = "config/notifications.json"):
    """Create a template configuration file for notifications"""
    config = {
        "log_dir": "logs/notifications",
        "email": {
            "smtp_server": "smtp.office365.com",
            "smtp_port": 587,
            "use_tls": True,
            "from": "data-pipelines@yourcompany.com",
            "to": ["data-team@yourcompany.com", "ops-team@yourcompany.com"],
            "username": "${EMAIL_USERNAME}",
            "password": "${EMAIL_PASSWORD}"
        },
        "teams": {
            "webhook_url": "${TEAMS_WEBHOOK_URL}"
        }
    }
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Created notification config template at: {output_path}")


# Example Cron job entry point
class CronPipelineRunner:
    """Entry point for Cron-scheduled pipelines"""
    
    def __init__(self, config_path: str = "config/notifications.json"):
        self.config_path = config_path
    
    def run_bronze_pipeline(self, source_system: str):
        """Run bronze pipeline for specified source system"""
        
        @pipeline_runner_decorator(f"bronze_{source_system}", self.config_path)
        def pipeline_logic():
            # Your actual pipeline logic here
            # This is just an example
            import time
            time.sleep(2)  # Simulate work
            
            # Return results including quality config
            return {
                'records_processed': 1000,
                'data': pd.DataFrame({'id': range(1000), 'value': range(1000)}),
                'quality_config': {
                    'required_columns': ['id', 'value'],
                    'unique_columns': ['id'],
                    'validation_rules': {
                        'value': {'min': 0, 'max': 1000}
                    }
                }
            }
        
        return pipeline_logic()


# Watchdog integration example
class WatchdogPipelineHandler:
    """File system watcher for triggering pipelines"""
    
    def __init__(self, watch_path: str, config_path: str):
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
        
        self.watch_path = Path(watch_path)
        self.config_path = config_path
        self.observer = Observer()
        
        class PipelineEventHandler(FileSystemEventHandler):
            def __init__(self, parent):
                self.parent = parent
            
            def on_created(self, event):
                if not event.is_directory and event.src_path.endswith('.trigger'):
                    self.parent.process_trigger_file(event.src_path)
        
        self.event_handler = PipelineEventHandler(self)
        self.observer.schedule(self.event_handler, str(self.watch_path), recursive=True)
    
    def start(self):
        """Start watching for trigger files"""
        self.observer.start()
        print(f"Watching {self.watch_path} for trigger files...")
    
    def stop(self):
        """Stop watching"""
        self.observer.stop()
        self.observer.join()
    
    def process_trigger_file(self, trigger_path: str):
        """Process a trigger file to run pipeline"""
        try:
            with open(trigger_path, 'r') as f:
                trigger_config = json.load(f)
            
            pipeline_name = trigger_config['pipeline_name']
            pipeline_params = trigger_config.get('parameters', {})
            
            # Run the appropriate pipeline
            runner = CronPipelineRunner(self.config_path)
            
            if pipeline_name.startswith('bronze_'):
                source_system = pipeline_name.replace('bronze_', '')
                runner.run_bronze_pipeline(source_system)
            
            # Delete trigger file after processing
            os.remove(trigger_path)
            
        except Exception as e:
            print(f"Error processing trigger file {trigger_path}: {str(e)}")


if __name__ == "__main__":
    # Create config template
    create_notification_config_template()
    
    # Example: Run pipeline via Cron
    if len(sys.argv) > 1 and sys.argv[1] == "run_bronze_oracle":
        runner = CronPipelineRunner()
        runner.run_bronze_pipeline("oracle")
    
    # Example: Start Watchdog
    elif len(sys.argv) > 1 and sys.argv[1] == "watchdog":
        handler = WatchdogPipelineHandler("data/triggers", "config/notifications.json")
        try:
            handler.start()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            handler.stop()
