"""
DAG Diagnostics System for MWAA
Combines serialization analysis, performance monitoring, and configuration tracking
Compatible with Apache Airflow v2.7+ and v3.10+
"""

import os
import json
import re
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.configuration import conf

# =============================================================================
# CONFIGURATION PARAMETERS (Parameterized via Airflow Variables)
# =============================================================================

# Core analysis parameters
ANALYSIS_DAYS = Variable.get("dag_diagnostics_analysis_days", default_var=7)
ENV_NAME = Variable.get(
    "dag_diagnostics_env_name", default_var=os.getenv("AIRFLOW_ENV_NAME", "")
)
S3_BUCKET = Variable.get(
    "dag_diagnostics_s3_bucket",
    default_var="",
)

# Thresholds for analysis (configurable)
LARGE_DAG_SIZE_MB = float(
    Variable.get("dag_diagnostics_large_dag_threshold_mb", default_var=5.0)
)
CRITICAL_DAG_SIZE_MB = float(
    Variable.get("dag_diagnostics_critical_dag_threshold_mb", default_var=10.0)
)
MIN_SUCCESS_RATE = float(
    Variable.get("dag_diagnostics_min_success_rate", default_var=95.0)
)
MAX_NESTING_DEPTH = int(
    Variable.get("dag_diagnostics_max_nesting_depth", default_var=20)
)

# DAG configuration
DAG_ID = os.path.basename(__file__).replace(".py", "")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# EXTENSIBLE DIAGNOSTIC QUERIES REGISTRY
# =============================================================================


class DiagnosticQueries:
    """Registry for extensible diagnostic queries"""

    @staticmethod
    def get_serialized_dag_analysis_query(analysis_days: int) -> str:
        """Enhanced serialized DAG analysis query"""
        return f"""
        SELECT 
            dag_id,
            fileloc,
            CASE 
                WHEN data_compressed IS NOT NULL AND LENGTH(data_compressed) > 0 THEN 'Compressed'
                ELSE 'Uncompressed'
            END as compression_status,
            -- More accurate size calculation using JSON representation
            round(octet_length(data::json::text), 2) as serialized_size_bytes,
            round(octet_length(data::json::text) / 1024.0, 2) as serialized_size_kb,
            round(octet_length(data::json::text) / 1024.0 / 1024.0, 2) as serialized_size_mb,
            -- Alternative: use pg_column_size for actual storage size
            pg_column_size(data) as storage_size_bytes,
            round(pg_column_size(data) / 1024.0, 2) as storage_size_kb,
            round(pg_column_size(data) / 1024.0 / 1024.0, 2) as storage_size_mb,
            last_updated,
            EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_updated)) as days_since_update,
            dag_hash,
            processor_subdir,
            -- Enhanced task count estimation
            CASE 
                WHEN data::text LIKE '%"task_id"%' THEN 
                    (LENGTH(data::text) - LENGTH(REPLACE(data::text, '"task_id"', ''))) / LENGTH('"task_id"')
                ELSE 0
            END as estimated_task_count,
            -- Operator type detection
            CASE 
                WHEN data::text LIKE '%BashOperator%' THEN 'BashOperator'
                WHEN data::text LIKE '%PythonOperator%' THEN 'PythonOperator'
                WHEN data::text LIKE '%S3%' THEN 'S3Operations'
                WHEN data::text LIKE '%Glue%' THEN 'GlueOperator'
                WHEN data::text LIKE '%Kubernetes%' THEN 'KubernetesOperator'
                WHEN data::text LIKE '%Docker%' THEN 'DockerOperator'
                ELSE 'Other'
            END as primary_operator_type,
            -- Complexity indicators
            CASE 
                WHEN data::text LIKE '%depends_on_past%' THEN 'Has Dependencies'
                WHEN data::text LIKE '%retries%' THEN 'Has Retries'
                WHEN data::text LIKE '%pool%' THEN 'Uses Pools'
                WHEN data::text LIKE '%sla%' THEN 'Has SLA'
                ELSE 'Standard'
            END as complexity_indicators,
            -- Basic structure info
            LENGTH(data::text) as total_length
        FROM serialized_dag
        WHERE data IS NOT NULL
        ORDER BY LENGTH(data::text) DESC
        """

    @staticmethod
    def get_performance_analysis_query(analysis_days: int) -> str:
        """Enhanced DAG performance analysis query"""
        return f"""
        SELECT 
            dag_id,
            COUNT(*) as total_runs,
            SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successful_runs,
            SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_runs,
            SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END) as running_runs,
            SUM(CASE WHEN state = 'queued' THEN 1 ELSE 0 END) as queued_runs,
            ROUND(
                (SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 
                2
            ) as success_rate_percent,
            AVG(
                CASE 
                    WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (end_date - start_date))
                    ELSE NULL 
                END
            ) as avg_runtime_seconds,
            MAX(
                CASE 
                    WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (end_date - start_date))
                    ELSE NULL 
                END
            ) as max_runtime_seconds,
            MIN(
                CASE 
                    WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (end_date - start_date))
                    ELSE NULL 
                END
            ) as min_runtime_seconds,
            COUNT(DISTINCT DATE(execution_date)) as active_days
        FROM dag_run
        WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        GROUP BY dag_id
        ORDER BY total_runs DESC
        """

    @staticmethod
    def get_task_failure_analysis_query(analysis_days: int) -> str:
        """Enhanced task failure analysis query"""
        return f"""
        SELECT 
            tf.dag_id,
            tf.task_id,
            COUNT(*) as failure_count,
            MAX(tf.start_date) as last_failure_date,
            MIN(tf.start_date) as first_failure_date,
            COUNT(DISTINCT DATE(tf.start_date)) as failure_days,
            AVG(
                CASE 
                    WHEN tf.end_date IS NOT NULL AND tf.start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (tf.end_date - tf.start_date))
                    ELSE NULL 
                END
            ) as avg_failure_duration_seconds
        FROM task_fail tf
        WHERE tf.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
        GROUP BY tf.dag_id, tf.task_id
        ORDER BY failure_count DESC
        LIMIT 50
        """

    @staticmethod
    def get_serialization_instability_query(analysis_days: int) -> str:
        """Query to detect DAGs with frequent serialization changes"""
        return f"""
        SELECT 
            dag_id,
            COUNT(*) as update_frequency,
            MIN(last_updated) as first_seen,
            MAX(last_updated) as last_seen,
            COUNT(DISTINCT dag_hash) as hash_variations,
            AVG(octet_length(data::json::text)) as avg_size_bytes,
            MAX(octet_length(data::json::text)) as max_size_bytes,
            MIN(octet_length(data::json::text)) as min_size_bytes
        FROM serialized_dag
        WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '{analysis_days} days'
        GROUP BY dag_id
        HAVING COUNT(DISTINCT dag_hash) > 3
        ORDER BY hash_variations DESC, update_frequency DESC
        """

    @staticmethod
    def get_traffic_pattern_hourly_query(analysis_days: int) -> str:
        """Query to analyze hourly traffic patterns"""
        return f"""
        WITH hourly_stats AS (
            SELECT 
                EXTRACT(HOUR FROM execution_date) as hour_of_day,
                EXTRACT(DOW FROM execution_date) as day_of_week,
                COUNT(*) as dag_runs,
                COUNT(DISTINCT dag_id) as unique_dags,
                AVG(CASE 
                    WHEN end_date IS NOT NULL AND start_date IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (end_date - start_date))
                    ELSE NULL 
                END) as avg_duration_seconds
            FROM dag_run
            WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            GROUP BY hour_of_day, day_of_week
        )
        SELECT 
            hour_of_day,
            SUM(dag_runs) as total_runs,
            AVG(dag_runs) as avg_runs_per_day,
            MAX(dag_runs) as max_runs_in_hour,
            AVG(unique_dags) as avg_unique_dags,
            AVG(avg_duration_seconds) as avg_duration_seconds
        FROM hourly_stats
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """

    @staticmethod
    def get_traffic_pattern_concurrent_query(analysis_days: int) -> str:
        """Query to analyze concurrent DAG run patterns"""
        return f"""
        WITH time_points AS (
            SELECT DISTINCT 
                start_date as time_point
            FROM dag_run
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            UNION
            SELECT DISTINCT 
                end_date as time_point
            FROM dag_run
            WHERE end_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
        ),
        concurrent_runs AS (
            SELECT 
                tp.time_point,
                COUNT(dr.dag_id) as concurrent_dags
            FROM time_points tp
            LEFT JOIN dag_run dr ON 
                dr.start_date <= tp.time_point 
                AND (dr.end_date >= tp.time_point OR dr.end_date IS NULL)
                AND dr.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            GROUP BY tp.time_point
        )
        SELECT 
            MAX(concurrent_dags) as max_concurrent_dags,
            AVG(concurrent_dags) as avg_concurrent_dags,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY concurrent_dags) as p95_concurrent_dags,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY concurrent_dags) as p99_concurrent_dags
        FROM concurrent_runs
        WHERE concurrent_dags > 0
        """

    @staticmethod
    def get_traffic_pattern_dag_frequency_query(analysis_days: int) -> str:
        """Query to analyze DAG execution frequency patterns"""
        return f"""
        WITH dag_schedule_stats AS (
            SELECT 
                dag_id,
                COUNT(*) as total_runs,
                COUNT(DISTINCT DATE(execution_date)) as active_days,
                MIN(execution_date) as first_run,
                MAX(execution_date) as last_run,
                AVG(EXTRACT(EPOCH FROM (end_date - start_date))) as avg_duration_seconds,
                STDDEV(EXTRACT(EPOCH FROM (end_date - start_date))) as stddev_duration_seconds
            FROM dag_run
            WHERE execution_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
                AND start_date IS NOT NULL
            GROUP BY dag_id
        )
        SELECT 
            dag_id,
            total_runs,
            active_days,
            ROUND(total_runs::numeric / NULLIF(active_days, 0), 2) as avg_runs_per_day,
            ROUND(avg_duration_seconds, 2) as avg_duration_seconds,
            ROUND(stddev_duration_seconds, 2) as stddev_duration_seconds,
            CASE 
                WHEN stddev_duration_seconds > avg_duration_seconds * 0.5 THEN 'High Variance'
                WHEN stddev_duration_seconds > avg_duration_seconds * 0.3 THEN 'Medium Variance'
                ELSE 'Low Variance'
            END as duration_variance,
            EXTRACT(EPOCH FROM (last_run - first_run)) / 3600 as time_span_hours
        FROM dag_schedule_stats
        WHERE total_runs > 1
        ORDER BY total_runs DESC
        """

    @staticmethod
    def get_traffic_pattern_task_concurrency_query(analysis_days: int) -> str:
        """Query to analyze task-level concurrency patterns"""
        return f"""
        WITH task_time_points AS (
            SELECT DISTINCT 
                start_date as time_point
            FROM task_instance
            WHERE start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
            UNION
            SELECT DISTINCT 
                end_date as time_point
            FROM task_instance
            WHERE end_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND end_date IS NOT NULL
        ),
        concurrent_tasks AS (
            SELECT 
                tp.time_point,
                COUNT(ti.task_id) as concurrent_tasks
            FROM task_time_points tp
            LEFT JOIN task_instance ti ON 
                ti.start_date <= tp.time_point 
                AND (ti.end_date >= tp.time_point OR ti.end_date IS NULL)
                AND ti.start_date >= CURRENT_DATE - INTERVAL '{analysis_days} days'
                AND ti.state IN ('running', 'queued')
            GROUP BY tp.time_point
        )
        SELECT 
            MAX(concurrent_tasks) as max_concurrent_tasks,
            AVG(concurrent_tasks) as avg_concurrent_tasks,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY concurrent_tasks) as p95_concurrent_tasks,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY concurrent_tasks) as p99_concurrent_tasks,
            COUNT(*) as sample_points
        FROM concurrent_tasks
        WHERE concurrent_tasks > 0
        """


# =============================================================================
# CONFIGURATION ANALYSIS HELPERS
# =============================================================================


def analyze_config_setting(
    section: str, setting: str, value: str, setting_info: Dict[str, Any]
) -> str:
    """Analyze a configuration setting and return status"""

    if value == "NOT_SET":
        return "🔶 Default"

    # Specific analysis for critical settings
    if section == "core":
        if setting == "executor" and "celery" not in value.lower():
            return "⚠️ Non-Celery"
        elif setting == "parallelism":
            try:
                parallelism = int(value)
                if parallelism > 100:
                    return "⚠️ High"
                elif parallelism < 10:
                    return "⚠️ Low"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting == "dag_file_processor_timeout":
            try:
                timeout = int(value)
                if timeout < 30:
                    return "⚠️ Too Low"
                elif timeout > 300:
                    return "⚠️ Too High"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting in ["store_serialized_dags", "compress_serialized_dags"]:
            if value.lower() in ["true", "1", "yes"]:
                return "✅ Enabled"
            else:
                return "⚠️ Disabled"

    elif section == "scheduler":
        if setting == "dag_dir_list_interval":
            try:
                interval = int(value)
                if interval < 30:
                    return "⚠️ Too Frequent"
                elif interval > 300:
                    return "⚠️ Too Slow"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting == "parsing_processes":
            try:
                processes = int(value)
                if processes < 1:
                    return "❌ Too Low"
                elif processes > 8:
                    return "⚠️ High"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"

    elif section == "celery":
        if setting == "worker_concurrency":
            try:
                concurrency = int(value)
                if concurrency < 4:
                    return "⚠️ Low"
                elif concurrency > 32:
                    return "⚠️ High"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"
        elif setting in ["broker_url", "result_backend"]:
            if "redis" in value.lower():
                return "✅ Redis"
            elif "sqs" in value.lower():
                return "✅ SQS"
            elif "rabbitmq" in value.lower():
                return "✅ RabbitMQ"
            else:
                return "🔶 Other"

    elif section == "database":
        if setting == "sql_alchemy_pool_size":
            try:
                pool_size = int(value)
                if pool_size < 5:
                    return "⚠️ Small"
                elif pool_size > 50:
                    return "⚠️ Large"
                else:
                    return "✅ Good"
            except ValueError:
                return "❌ Invalid"

    # Default status for configured values
    return "✅ Set"


def analyze_configuration_issues(config_analysis: Dict[str, Any]) -> List[str]:
    """Analyze configuration for potential stability issues and conflicts"""
    issues = []

    try:
        # Extract configuration sections
        core_config = config_analysis.get("core", {})
        scheduler_config = config_analysis.get("scheduler", {})
        celery_config = config_analysis.get("celery", {})
        db_config = config_analysis.get("database", {})

        # =================================================================
        # CRITICAL: DAG Processing Timeout Conflicts
        # =================================================================
        
        # Issue: dagbag_import_timeout must be LESS than dag_file_processor_timeout
        # The file processor timeout is the overall timeout for processing a DAG file,
        # while dagbag_import_timeout is for importing the DAG itself
        try:
            dag_file_processor_timeout = int(core_config.get("dag_file_processor_timeout", {}).get("value", "50"))
            dagbag_import_timeout = int(core_config.get("dagbag_import_timeout", {}).get("value", "30"))
            
            if dagbag_import_timeout >= dag_file_processor_timeout:
                issues.append(
                    f"🚨 CRITICAL: dagbag_import_timeout ({dagbag_import_timeout}s) >= "
                    f"dag_file_processor_timeout ({dag_file_processor_timeout}s). "
                    f"DAG imports will timeout before file processing completes. "
                    f"Recommendation: dagbag_import_timeout should be 20-30% less than dag_file_processor_timeout"
                )
            elif dagbag_import_timeout > (dag_file_processor_timeout * 0.8):
                issues.append(
                    f"⚠️ WARNING: dagbag_import_timeout ({dagbag_import_timeout}s) is too close to "
                    f"dag_file_processor_timeout ({dag_file_processor_timeout}s). "
                    f"Leave more buffer to prevent timeout race conditions"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # CRITICAL: Scheduler File Processing Interval Conflicts
        # =================================================================
        
        # Issue: min_file_process_interval should be LESS than dag_dir_list_interval
        # The scheduler lists the DAG directory at dag_dir_list_interval, but won't
        # process files more frequently than min_file_process_interval
        try:
            dag_dir_list_interval = int(scheduler_config.get("dag_dir_list_interval", {}).get("value", "300"))
            min_file_process_interval = int(scheduler_config.get("min_file_process_interval", {}).get("value", "30"))
            
            if min_file_process_interval > dag_dir_list_interval:
                issues.append(
                    f"🚨 CRITICAL: min_file_process_interval ({min_file_process_interval}s) > "
                    f"dag_dir_list_interval ({dag_dir_list_interval}s). "
                    f"Scheduler will list DAG directory more frequently than it can process files, "
                    f"causing resource contention and processing delays. "
                    f"Recommendation: min_file_process_interval should be less than dag_dir_list_interval"
                )
            elif min_file_process_interval == dag_dir_list_interval:
                issues.append(
                    f"⚠️ WARNING: min_file_process_interval ({min_file_process_interval}s) equals "
                    f"dag_dir_list_interval ({dag_dir_list_interval}s). "
                    f"This may cause timing conflicts. Set min_file_process_interval lower"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Parallelism and Concurrency Conflicts
        # =================================================================
        
        # Check if serialized DAGs are enabled
        if core_config.get("store_serialized_dags", {}).get("value") not in ["True", "true", "1"]:
            issues.append("⚠️ Serialized DAGs not enabled - may impact performance and scheduler stability")

        # Global parallelism vs max_active_tasks_per_dag
        try:
            parallelism = int(core_config.get("parallelism", {}).get("value", "32"))
            max_active_tasks = int(core_config.get("max_active_tasks_per_dag", {}).get("value", "16"))
            max_active_runs = int(core_config.get("max_active_runs_per_dag", {}).get("value", "16"))
            
            if parallelism < max_active_tasks:
                issues.append(
                    f"⚠️ Global parallelism ({parallelism}) < max_active_tasks_per_dag ({max_active_tasks}). "
                    f"DAGs cannot reach their max task concurrency"
                )
            
            # Check if parallelism is too low for the number of potential concurrent tasks
            potential_concurrent_tasks = max_active_tasks * max_active_runs
            if parallelism < potential_concurrent_tasks:
                issues.append(
                    f"⚠️ Global parallelism ({parallelism}) may be insufficient for "
                    f"max_active_tasks_per_dag ({max_active_tasks}) × max_active_runs_per_dag ({max_active_runs}) = "
                    f"{potential_concurrent_tasks} potential concurrent tasks"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Celery Worker Configuration Conflicts
        # =================================================================
        
        try:
            worker_concurrency = int(celery_config.get("worker_concurrency", {}).get("value", "16"))
            parallelism = int(core_config.get("parallelism", {}).get("value", "32"))
            
            if worker_concurrency > parallelism:
                issues.append(
                    f"⚠️ Celery worker_concurrency ({worker_concurrency}) > core parallelism ({parallelism}). "
                    f"Workers may accept more tasks than the system can handle"
                )
            
            # Check if worker_concurrency is too high for typical worker resources
            if worker_concurrency > 32:
                issues.append(
                    f"⚠️ Very high worker_concurrency ({worker_concurrency}). "
                    f"Ensure workers have sufficient CPU/memory resources"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Database Connection Pool Conflicts
        # =================================================================
        
        try:
            pool_size = int(db_config.get("sql_alchemy_pool_size", {}).get("value", "5"))
            max_overflow = int(db_config.get("sql_alchemy_max_overflow", {}).get("value", "10"))
            parallelism = int(core_config.get("parallelism", {}).get("value", "32"))
            parsing_processes = int(scheduler_config.get("parsing_processes", {}).get("value", "2"))
            
            # Get executor and webserver configuration
            executor = core_config.get("executor", {}).get("value", "SequentialExecutor")
            webserver_workers = int(config_analysis.get("webserver", {}).get("workers", {}).get("value", "4"))
            worker_concurrency = int(celery_config.get("worker_concurrency", {}).get("value", "16"))
            
            # Calculate estimated connection requirements
            # NOTE: Each Airflow process (scheduler, webserver worker, celery worker, etc.) 
            # maintains its own connection pool of size pool_size + max_overflow
            
            # 1. Scheduler components
            scheduler_connections = parsing_processes  # Each parsing process needs connections
            scheduler_connections += 1  # Scheduler main loop
            scheduler_connections += 1  # DagFileProcessorManager
            
            # 2. Webserver
            webserver_connections = webserver_workers  # Each gunicorn worker
            
            # 3. Executor-specific
            executor_connections = 0
            if "Celery" in executor:
                # CeleryExecutor: Each worker process needs connections
                # Estimate: Assume 2-4 worker processes per environment (conservative)
                estimated_celery_workers = 2  # Conservative estimate
                executor_connections = estimated_celery_workers
            elif "Local" in executor:
                # LocalExecutor: Runs tasks in scheduler process, minimal extra connections
                executor_connections = 1
            elif "Kubernetes" in executor:
                # KubernetesExecutor: Minimal connections (pods connect separately)
                executor_connections = 1
            
            # 4. Triggerer (if async tasks are used)
            triggerer_connections = 1
            
            # 5. Buffer for transient connections
            buffer_connections = 2
            
            # Total estimated connections needed across ALL processes
            # IMPORTANT: This is total connections to the database, not per-process pool size
            total_estimated_connections = (
                scheduler_connections + 
                webserver_connections + 
                executor_connections + 
                triggerer_connections + 
                buffer_connections
            )
            
            # Per-process pool calculation
            # Each process has pool_size base + max_overflow burst capacity
            total_pool_per_process = pool_size + max_overflow
            
            # Estimate total possible connections if all processes max out
            # This is a worst-case scenario
            num_processes = (
                1 +  # Scheduler
                parsing_processes +  # DAG processors
                webserver_workers +  # Webserver workers
                executor_connections +  # Executor workers
                1  # Triggerer
            )
            worst_case_connections = num_processes * total_pool_per_process
            
            # Generate warnings based on analysis
            if pool_size < 5:
                issues.append(
                    f"⚠️ Very small database connection pool ({pool_size}). "
                    f"May cause connection exhaustion. Recommended: >= 5"
                )
            
            # Check if pool size is reasonable for the number of processes
            # Rule of thumb: pool_size should be at least 5-10 per major component
            recommended_pool_size = max(5, parsing_processes + 3)
            if pool_size < recommended_pool_size:
                issues.append(
                    f"⚠️ Database pool size ({pool_size}) may be small for {parsing_processes} parsing processes. "
                    f"Each Airflow process maintains its own pool. Recommended: >= {recommended_pool_size}"
                )
            
            # Check total pool capacity per process
            if total_pool_per_process < 10:
                issues.append(
                    f"⚠️ Total pool capacity per process ({pool_size} + {max_overflow} = {total_pool_per_process}) is small. "
                    f"Under load, processes may exhaust connections. Recommended: total >= 10"
                )
            
            # Warn about worst-case scenario
            if worst_case_connections > 100:
                issues.append(
                    f"⚠️ Worst-case database connections: {worst_case_connections} "
                    f"({num_processes} processes × {total_pool_per_process} pool). "
                    f"Verify database max_connections can handle this. "
                    f"Note: Actual usage is typically much lower."
                )
            
            # Check if pool is too large (can cause database connection limits)
            if pool_size > 50:
                issues.append(
                    f"⚠️ Very large connection pool ({pool_size}) per process. "
                    f"With {num_processes} processes, worst-case is {worst_case_connections} connections. "
                    f"Verify database can handle this load."
                )
            
            # Executor-specific warnings
            if "Celery" in executor and pool_size < 10:
                issues.append(
                    f"⚠️ CeleryExecutor with small pool size ({pool_size}). "
                    f"Each Celery worker maintains its own pool. Consider increasing to >= 10."
                )
            
            # Provide context in a separate info message (not an issue, just FYI)
            # This helps users understand the calculation
            connection_breakdown = (
                f"Database Connection Estimate: "
                f"Scheduler={scheduler_connections}, "
                f"Webserver={webserver_connections}, "
                f"Executor={executor_connections}, "
                f"Other={triggerer_connections + buffer_connections}. "
                f"Total estimated: {total_estimated_connections} concurrent connections. "
                f"Per-process pool: {total_pool_per_process}. "
                f"Worst-case (all processes maxed): {worst_case_connections}."
            )
            # Note: We don't add this as an "issue" but it could be logged or included in detailed output
            
        except (ValueError, TypeError, KeyError) as e:
            # Silently skip if we can't parse the values
            pass

        # =================================================================
        # Scheduler Performance Conflicts
        # =================================================================
        
        try:
            parsing_processes = int(scheduler_config.get("parsing_processes", {}).get("value", "2"))
            
            if parsing_processes < 2:
                issues.append(
                    f"⚠️ Low number of DAG parsing processes ({parsing_processes}). "
                    f"May cause DAG processing delays. Recommended: >= 2"
                )
            elif parsing_processes > 8:
                issues.append(
                    f"⚠️ High number of parsing processes ({parsing_processes}). "
                    f"May cause CPU contention and database connection pressure"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # Check scheduler loop intervals
        try:
            dag_dir_list_interval = int(scheduler_config.get("dag_dir_list_interval", {}).get("value", "300"))
            
            if dag_dir_list_interval < 30:
                issues.append(
                    f"⚠️ Very frequent DAG directory scanning ({dag_dir_list_interval}s). "
                    f"May cause excessive I/O and CPU usage"
                )
            elif dag_dir_list_interval > 600:
                issues.append(
                    f"⚠️ Infrequent DAG directory scanning ({dag_dir_list_interval}s). "
                    f"New DAGs may take a long time to be detected"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Task Execution Timeout Conflicts
        # =================================================================
        
        try:
            killed_task_cleanup_time = int(core_config.get("killed_task_cleanup_time", {}).get("value", "60"))
            
            if killed_task_cleanup_time < 30:
                issues.append(
                    f"⚠️ Very short killed_task_cleanup_time ({killed_task_cleanup_time}s). "
                    f"Tasks may be cleaned up before they can gracefully shutdown"
                )
            elif killed_task_cleanup_time > 300:
                issues.append(
                    f"⚠️ Long killed_task_cleanup_time ({killed_task_cleanup_time}s). "
                    f"Zombie tasks may consume resources for extended periods"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # Serialization Configuration Conflicts
        # =================================================================
        
        try:
            min_serialized_dag_update_interval = int(core_config.get("min_serialized_dag_update_interval", {}).get("value", "30"))
            min_serialized_dag_fetch_interval = int(core_config.get("min_serialized_dag_fetch_interval", {}).get("value", "10"))
            dag_dir_list_interval = int(scheduler_config.get("dag_dir_list_interval", {}).get("value", "300"))
            
            # Update interval should be less than or equal to dir list interval
            if min_serialized_dag_update_interval > dag_dir_list_interval:
                issues.append(
                    f"⚠️ min_serialized_dag_update_interval ({min_serialized_dag_update_interval}s) > "
                    f"dag_dir_list_interval ({dag_dir_list_interval}s). "
                    f"Serialized DAGs may not update as frequently as directory is scanned"
                )
            
            # Fetch interval should be less than update interval
            if min_serialized_dag_fetch_interval >= min_serialized_dag_update_interval:
                issues.append(
                    f"⚠️ min_serialized_dag_fetch_interval ({min_serialized_dag_fetch_interval}s) >= "
                    f"min_serialized_dag_update_interval ({min_serialized_dag_update_interval}s). "
                    f"Webserver may fetch stale DAG data"
                )
        except (ValueError, TypeError, KeyError):
            pass

        # =================================================================
        # DagRun Creation Limits
        # =================================================================
        
        try:
            max_dagruns_to_create = int(scheduler_config.get("max_dagruns_to_create_per_loop", {}).get("value", "10"))
            max_dagruns_to_schedule = int(scheduler_config.get("max_dagruns_per_loop_to_schedule", {}).get("value", "20"))
            
            if max_dagruns_to_create > max_dagruns_to_schedule:
                issues.append(
                    f"⚠️ max_dagruns_to_create_per_loop ({max_dagruns_to_create}) > "
                    f"max_dagruns_per_loop_to_schedule ({max_dagruns_to_schedule}). "
                    f"Scheduler may create more DagRuns than it can schedule"
                )
        except (ValueError, TypeError, KeyError):
            pass

    except Exception as e:
        issues.append(f"❌ Error analyzing configuration: {str(e)}")

    return issues


# =============================================================================
# CORE DIAGNOSTIC FUNCTIONS
# =============================================================================


@task()
def create_postgres_connection():
    """Create PostgreSQL connection from MWAA environment variables"""
    from airflow.models import Connection
    from airflow import settings

    # Try both possible environment variable names for compatibility
    sql_alchemy_conn = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN") or os.getenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    )

    if sql_alchemy_conn is None:
        raise ValueError("SQL_ALCHEMY_CONN environment variable is not set.")

    # Parse connection string
    s = sql_alchemy_conn.split("@")
    s2 = s[1].split("?")[0]
    host_port_db = s2.split("/")
    host_port = host_port_db[0]
    database = host_port_db[1] if len(host_port_db) > 1 else "airflow"

    if ":" in host_port:
        host = host_port.split(":")[0]
        port = int(host_port.split(":")[1])
    else:
        host = host_port
        port = 5432

    c = s[0].split("//")[1].split(":")
    username = c[0]
    password = c[1]

    env_name_lower = ENV_NAME.lower().replace(" ", "_")
    conn_id = f"{env_name_lower}_postgres_diagnostics"

    session = settings.Session()
    existing_conn = (
        session.query(Connection).filter(Connection.conn_id == conn_id).first()
    )

    if existing_conn:
        session.close()
        return {"conn_id": conn_id, "status": "exists"}

    try:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=host,
            port=port,
            schema=database,
            login=username,
            password=password,
            description=f"MWAA PostgreSQL diagnostics connection for {ENV_NAME}",
        )
        session.add(new_conn)
        session.commit()
        print(f"Created PostgreSQL connection: {conn_id}")
        status = "created"
    except Exception as e:
        session.rollback()
        print(f"Failed to create connection: {e}")
        status = "failed"
        raise
    finally:
        session.close()

    return {"conn_id": conn_id, "status": status}


@task()
def capture_airflow_configuration(**context):
    """Capture stability-critical Airflow configuration settings"""

    print("\n" + "=" * 80)
    print("AIRFLOW STABILITY CONFIGURATION ANALYSIS")
    print("=" * 80)

    # Define stability-critical configuration settings to capture
    stability_settings = {
        "core": [
            "executor",
            "parallelism",
            "max_active_tasks_per_dag",
            "max_active_runs_per_dag",
            "dagbag_import_timeout",
            "dag_file_processor_timeout",
            "killed_task_cleanup_time",
            "dag_discovery_safe_mode",
            "store_dag_code",
            "store_serialized_dags",
            "compress_serialized_dags",
            "min_serialized_dag_update_interval",
            "min_serialized_dag_fetch_interval",
        ],
        "scheduler": [
            "dag_dir_list_interval",
            "min_file_process_interval",
            "catchup_by_default",
            "max_dagruns_to_create_per_loop",
            "max_dagruns_per_loop_to_schedule",
            "pool_metrics_interval",
            "orphaned_tasks_check_interval",
            "child_process_log_directory",
            "parsing_processes",
            "file_parsing_sort_mode",
            "use_row_level_locking",
            "max_tis_per_query",
        ],
        "celery": [
            "worker_concurrency",
            "broker_url",
            "result_backend",
            "flower_host",
            "flower_port",
            "default_queue",
            "sync",
            "pool",
            "worker_precheck",
            "task_track_started",
            "task_publish_retry",
            "worker_enable_remote_control",
        ],
        "webserver": [
            "workers",
            "worker_timeout",
            "worker_refresh_batch_size",
            "reload_on_plugin_change",
        ],
        "database": [
            "sql_alchemy_pool_size",
            "sql_alchemy_max_overflow",
            "sql_alchemy_pool_recycle",
            "sql_alchemy_pool_pre_ping",
            "sql_alchemy_schema",
        ],
    }

    # Impact levels for display
    impact_levels = {
        "executor": "HIGH",
        "parallelism": "HIGH",
        "max_active_tasks_per_dag": "HIGH",
        "max_active_runs_per_dag": "HIGH",
        "dagbag_import_timeout": "HIGH",
        "dag_file_processor_timeout": "HIGH",
        "store_serialized_dags": "HIGH",
        "min_serialized_dag_update_interval": "HIGH",
        "min_serialized_dag_fetch_interval": "HIGH",
        "dag_dir_list_interval": "HIGH",
        "min_file_process_interval": "HIGH",
        "parsing_processes": "HIGH",
        "worker_concurrency": "HIGH",
        "broker_url": "HIGH",
        "result_backend": "HIGH",
        "sql_alchemy_pool_size": "HIGH",
        "sql_alchemy_max_overflow": "HIGH",
        "max_dagruns_to_create_per_loop": "MEDIUM",
        "max_dagruns_per_loop_to_schedule": "MEDIUM",
    }

    # Sensitive settings to mask
    sensitive_settings = ["broker_url", "result_backend"]

    # Capture configuration values
    config_data = {}

    print(f"{'Setting':<40} {'Value':<25} {'Impact':<8} {'Status':<15}")
    print("-" * 95)

    for section_name, settings in stability_settings.items():
        config_data[section_name] = {}

        for setting_name in settings:
            try:
                # Get the configuration value
                value = conf.get(section_name, setting_name, fallback="NOT_SET")

                # Determine impact level
                impact = impact_levels.get(setting_name, "MEDIUM")

                # Determine status
                if value == "NOT_SET":
                    status = "🔶 Default"
                elif setting_name in sensitive_settings:
                    status = "✅ Set"
                else:
                    status = analyze_config_setting(
                        section_name, setting_name, value, {"stability_impact": impact}
                    )

                # Store actual value (mask sensitive ones)
                if setting_name in sensitive_settings and value != "NOT_SET":
                    config_data[section_name][setting_name] = "***MASKED***"
                    display_value = "***MASKED***"
                else:
                    config_data[section_name][setting_name] = str(value)
                    display_value = str(value)
                    if len(display_value) > 22:
                        display_value = display_value[:19] + "..."

                # Display in table format
                full_name = f"{section_name}.{setting_name}"
                print(f"{full_name:<40} {display_value:<25} {impact:<8} {status:<15}")

            except Exception as e:
                config_data[section_name][setting_name] = f"ERROR: {str(e)}"
                print(
                    f"{section_name}.{setting_name:<35} ERROR: {str(e):<20} {impact_levels.get(setting_name, 'MEDIUM'):<8} ❌ Error"
                )

    # Add environment information
    try:
        import airflow

        import platform
        
        config_data["environment_info"] = {
            "airflow_version": airflow.__version__,
            "python_version": platform.python_version(),
            "executor": conf.get("core", "executor", fallback="Unknown"),
            "dags_folder": conf.get("core", "dags_folder", fallback="Unknown"),
        }

        print(f"\n📋 ENVIRONMENT INFO")
        print(f"Airflow Version: {airflow.__version__}")
        print(f"Python Version: {config_data['environment_info']['python_version']}")
        print(f"Executor: {config_data['environment_info']['executor']}")
        print(f"DAGs Folder: {config_data['environment_info']['dags_folder']}")

    except Exception as e:
        config_data["environment_info"] = {"error": str(e)}

    # Create configuration fingerprint for change detection
    config_json = json.dumps(config_data, sort_keys=True)
    config_hash = hashlib.md5(config_json.encode()).hexdigest()

    # Get runtime parameters for environment name
    dag_run = context.get("dag_run")
    params = context.get("params", {})

    if dag_run and dag_run.conf and "env_name" in dag_run.conf:
        env_name = dag_run.conf["env_name"]
    else:
        env_name = params.get("env_name", ENV_NAME)

    # Get previous configuration hash from Variable (if exists)
    previous_hash = Variable.get(
        f"dag_diagnostics_config_hash_{env_name}", default_var=None
    )
    config_changed = previous_hash is not None and previous_hash != config_hash

    # Update stored hash
    Variable.set(f"dag_diagnostics_config_hash_{env_name}", config_hash)

    print(f"\n🔍 CONFIGURATION CHANGE DETECTION")
    print(f"Configuration hash: {config_hash}")
    print(f"Previous hash: {previous_hash}")
    print(f"Configuration changed: {'YES' if config_changed else 'NO'}")

    # Analyze configuration for conflicts and issues
    print(f"\n🔍 CONFIGURATION CONFLICT ANALYSIS")
    print("-" * 80)
    
    # Prepare config analysis structure for the analyzer
    config_analysis_input = {
        "core": {setting: {"value": value} for setting, value in config_data.get("core", {}).items()},
        "scheduler": {setting: {"value": value} for setting, value in config_data.get("scheduler", {}).items()},
        "celery": {setting: {"value": value} for setting, value in config_data.get("celery", {}).items()},
        "database": {setting: {"value": value} for setting, value in config_data.get("database", {}).items()},
    }
    
    config_issues = analyze_configuration_issues(config_analysis_input)
    
    if config_issues:
        print(f"Found {len(config_issues)} configuration issues:\n")
        for issue in config_issues:
            print(f"  {issue}")
    else:
        print("✅ No configuration conflicts detected")

    return {
        "timestamp": datetime.now().isoformat(),
        "config_hash": config_hash,
        "previous_hash": previous_hash,
        "config_changed": config_changed,
        "configuration": config_data,
        "issues": config_issues,
        "total_settings": sum(
            len(section)
            for section in config_data.values()
            if isinstance(section, dict)
        ),
    }


@task()
def analyze_serialized_dags(**context):
    """Serialized DAG analysis"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("SERIALIZED DAG ANALYSIS")
    print("=" * 80)

    # Execute main analysis query
    query = DiagnosticQueries.get_serialized_dag_analysis_query(ANALYSIS_DAYS)

    try:
        records = postgres_hook.get_records(query)
    except Exception as e:
        print(f"Primary query failed: {e}")
        # Fallback to simpler query for compatibility
        simple_query = """
        SELECT dag_id, fileloc, octet_length(data::json::text) as size_bytes,
               round(octet_length(data::json::text) / 1024.0 / 1024.0, 2) AS size_mb,
               last_updated, dag_hash
        FROM serialized_dag WHERE data IS NOT NULL
        ORDER BY octet_length(data::json::text) DESC
        """
        records = postgres_hook.get_records(simple_query)

    if not records:
        return {"total_dags": 0, "message": "No serialized DAG data found"}

    # Process results
    total_dags = len(records)
    suspicious_dags = []
    size_distribution = {
        "<50KB": 0,
        "50-500KB": 0,
        "500KB-1MB": 0,
        "1-5MB": 0,
        ">5MB": 0,
    }

    print(f"\nAnalyzing {total_dags} DAGs...")
    print("NOTE: Size refers to serialized DAG data in database, not source file size")
    print("JSON Size = Actual serialized content size, Storage Size = Database storage overhead")
    print(f"{'DAG ID':<35} {'JSON (MB)':<10} {'Storage (MB)':<12} {'Status':<15} {'Issues':<30}")
    print("-" * 110)

    for record in records:
        dag_id = record[0]
        # Use JSON size (more accurate) - column 5, fallback to storage size - column 8
        if len(record) > 8:
            json_size_mb = float(record[5]) if record[5] is not None else 0
            storage_size_mb = float(record[8]) if record[8] is not None else 0
            size_mb = json_size_mb  # Use JSON size as primary
        elif len(record) > 5:
            size_mb = float(record[5]) if record[5] is not None else 0
            storage_size_mb = None
        else:
            size_mb = float(record[3]) if len(record) > 3 else 0
            storage_size_mb = None

        # Categorize by size
        if size_mb < 0.05:  # 50KB
            size_distribution["<50KB"] += 1
        elif size_mb < 0.5:  # 500KB
            size_distribution["50-500KB"] += 1
        elif size_mb < 1:
            size_distribution["500KB-1MB"] += 1
        elif size_mb < 5:
            size_distribution["1-5MB"] += 1
        else:
            size_distribution[">5MB"] += 1

        # Detect issues
        issues = []
        status = "✅ Normal"

        if size_mb > CRITICAL_DAG_SIZE_MB:
            issues.append("CRITICAL_SIZE")
            status = "🚨 Critical"
        elif size_mb > LARGE_DAG_SIZE_MB:
            issues.append("LARGE_SIZE")
            status = "⚠️ Warning"

        # Additional checks for large serialized DAGs
        if len(record) > 10:
            try:
                # Check for various size thresholds and potential causes
                if size_mb > 50:  # Extremely large
                    issues.append("EXTREMELY_LARGE")
                    status = "🚨 Critical"
                elif size_mb > 20:  # Very large
                    issues.append("VERY_LARGE")
                    status = "⚠️ Warning"
                elif size_mb > 10:  # Large
                    issues.append("LARGE_SERIALIZATION")
                    status = "⚠️ Warning"

                # Try to estimate task count for context
                if len(record) > 10:
                    estimated_tasks = record[10] if record[10] else 0
                    if size_mb > 5 and estimated_tasks < 10:
                        issues.append("LARGE_WITH_FEW_TASKS")
                        status = "⚠️ Warning"

            except Exception:
                pass

        issues_str = ", ".join(issues) if issues else "None"
        storage_size_display = f"{storage_size_mb:.2f}" if storage_size_mb is not None else "N/A"
        print(f"{dag_id:<35} {size_mb:<10.2f} {storage_size_display:<12} {status:<15} {issues_str:<30}")

        if issues:
            suspicious_dags.append(
                {
                    "dag_id": dag_id,
                    "size_mb": size_mb,
                    "issues": issues,
                    "fileloc": record[1] if len(record) > 1 else "Unknown",
                }
            )

    # Calculate statistics
    sizes_mb = []
    for record in records:
        if len(record) > 8:
            json_size_mb = float(record[5]) if record[5] is not None else 0
            sizes_mb.append(json_size_mb)
        elif len(record) > 5:
            sizes_mb.append(float(record[5]) if record[5] is not None else 0)
        else:
            sizes_mb.append(float(record[3]) if len(record) > 3 else 0)
    avg_size_mb = sum(sizes_mb) / len(sizes_mb)
    max_size_mb = max(sizes_mb)

    print(f"\n📊 SUMMARY STATISTICS")
    print(f"Total DAGs: {total_dags}")
    print(f"Average size: {avg_size_mb:.2f} MB")
    print(f"Largest DAG: {max_size_mb:.2f} MB")
    print(f"Suspicious DAGs: {len(suspicious_dags)}")

    return {
        "total_dags": total_dags,
        "avg_size_mb": avg_size_mb,
        "max_size_mb": max_size_mb,
        "suspicious_dags": suspicious_dags,
        "size_distribution": size_distribution,
        "all_dags_by_size": [
            {
                "dag_id": record[0],
                "size_mb": (
                    float(record[5]) if len(record) > 8 and record[5] is not None
                    else float(record[5]) if len(record) > 5 and record[5] is not None
                    else float(record[3]) if len(record) > 3 else 0
                ),
                "storage_size_mb": (
                    float(record[8]) if len(record) > 8 and record[8] is not None else None
                ),
                "fileloc": record[1] if len(record) > 1 else "Unknown",
            }
            for record in records  # Include ALL DAGs, not just top 10
        ],
        "top_10_largest": [
            {
                "dag_id": record[0],
                "size_mb": (
                    float(record[5]) if len(record) > 8 and record[5] is not None
                    else float(record[5]) if len(record) > 5 and record[5] is not None
                    else float(record[3]) if len(record) > 3 else 0
                ),
                "storage_size_mb": (
                    float(record[8]) if len(record) > 8 and record[8] is not None else None
                ),
                "fileloc": record[1] if len(record) > 1 else "Unknown",
            }
            for record in records[:10]  # Keep top 10 for backward compatibility
        ],
    }


@task()
def analyze_dag_performance(**context):
    """DAG performance analysis"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("DAG PERFORMANCE ANALYSIS")
    print("=" * 80)

    query = DiagnosticQueries.get_performance_analysis_query(ANALYSIS_DAYS)
    records = postgres_hook.get_records(query)

    if not records:
        return {"active_dags": 0, "message": "No performance data available"}

    active_dags = len(records)
    performance_issues = []

    print(f"Analyzing {active_dags} active DAGs over last {ANALYSIS_DAYS} days...")
    print(
        f"{'DAG ID':<35} {'Runs':<8} {'Success %':<10} {'Avg Runtime':<12} {'Status':<15}"
    )
    print("-" * 85)

    for record in records[:20]:  # Top 20 most active
        dag_id = record[0]
        total_runs = record[1]
        success_rate = float(record[6]) if record[6] is not None else 0
        avg_runtime = float(record[7]) if record[7] is not None else 0

        # Determine status
        status = "✅ Healthy"
        issues = []

        if success_rate < MIN_SUCCESS_RATE:
            issues.append("LOW_SUCCESS_RATE")
            status = "🚨 Critical"
        elif success_rate < 98:
            status = "⚠️ Warning"

        if avg_runtime > 3600:  # > 1 hour
            issues.append("LONG_RUNTIME")
            if status == "✅ Healthy":
                status = "⚠️ Warning"

        runtime_str = (
            f"{avg_runtime/60:.1f}m"
            if avg_runtime < 3600
            else f"{avg_runtime/3600:.1f}h"
        )

        print(
            f"{dag_id:<35} {total_runs:<8} {success_rate:<10.1f} {runtime_str:<12} {status:<15}"
        )

        if issues:
            performance_issues.append(
                {
                    "dag_id": dag_id,
                    "success_rate": success_rate,
                    "avg_runtime_seconds": avg_runtime,
                    "total_runs": total_runs,
                    "issues": issues,
                }
            )

    # Calculate overall statistics
    success_rates = [float(record[6]) for record in records if record[6] is not None]
    avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0

    return {
        "active_dags": active_dags,
        "avg_success_rate": avg_success_rate,
        "performance_issues": performance_issues,
        "performance_summary": [
            {
                "dag_id": record[0],
                "total_runs": int(record[1]),
                "success_rate_percent": float(record[6]) if record[6] else 0,
                "avg_runtime_seconds": float(record[7]) if record[7] else 0,
            }
            for record in records[:10]
        ],
    }


@task()
def analyze_task_failures(**context):
    """task failure analysis"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("TASK FAILURE ANALYSIS")
    print("=" * 80)

    query = DiagnosticQueries.get_task_failure_analysis_query(ANALYSIS_DAYS)
    records = postgres_hook.get_records(query)

    if not records:
        print("No task failures found in the analysis period!")
        return {"total_failing_tasks": 0, "top_failures": [], "dag_failure_summary": {}}

    print(f"Found {len(records)} failing task types in last {ANALYSIS_DAYS} days")
    print(
        f"{'DAG ID':<30} {'Task ID':<25} {'Failures':<10} {'Days':<6} {'Last Failure':<20}"
    )
    print("-" * 95)

    dag_failure_counts = {}
    top_failures = []

    for record in records[:20]:  # Top 20 failing tasks
        dag_id = record[0]
        task_id = record[1]
        failure_count = record[2]
        last_failure = record[3]
        failure_days = record[5]

        print(
            f"{dag_id:<30} {task_id:<25} {failure_count:<10} {failure_days:<6} {str(last_failure)[:19]:<20}"
        )

        top_failures.append(
            {
                "dag_id": dag_id,
                "task_id": task_id,
                "failure_count": int(failure_count),
                "failure_days": int(failure_days),
                "last_failure_date": str(last_failure),
            }
        )

        # Aggregate by DAG
        dag_failure_counts[dag_id] = dag_failure_counts.get(dag_id, 0) + failure_count

    return {
        "total_failing_tasks": len(records),
        "top_failures": top_failures,
        "dag_failure_summary": dict(
            sorted(dag_failure_counts.items(), key=lambda x: x[1], reverse=True)
        ),
    }


@task()
def analyze_serialization_instability(**context):
    """Analyze DAGs with frequent serialization changes"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("SERIALIZATION INSTABILITY ANALYSIS")
    print("=" * 80)

    query = DiagnosticQueries.get_serialization_instability_query(ANALYSIS_DAYS)

    try:
        records = postgres_hook.get_records(query)

        if not records:
            print("No DAGs with serialization instability detected.")
            return {"unstable_dags": 0, "unstable_dag_list": []}

        print(f"Found {len(records)} DAGs with serialization instability")
        print(f"{'DAG ID':<35} {'Hash Vars':<10} {'Updates':<10} {'Avg Size (MB)':<15}")
        print("-" * 75)

        unstable_dags = []
        for record in records:
            dag_id = record[0]
            hash_variations = record[4]
            update_frequency = record[1]
            avg_size_mb = float(record[5]) / 1048576 if record[5] else 0

            print(
                f"{dag_id:<35} {hash_variations:<10} {update_frequency:<10} {avg_size_mb:<15.2f}"
            )

            unstable_dags.append(
                {
                    "dag_id": dag_id,
                    "hash_variations": int(hash_variations),
                    "update_frequency": int(update_frequency),
                    "avg_size_mb": avg_size_mb,
                }
            )

        return {"unstable_dags": len(records), "unstable_dag_list": unstable_dags}

    except Exception as e:
        print(f"Could not analyze serialization instability: {e}")
        return {"unstable_dags": 0, "unstable_dag_list": [], "error": str(e)}


@task()
def analyze_traffic_patterns(**context):
    """Analyze DAG execution traffic patterns to identify scheduling hotspots and resource utilization"""
    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    print("\n" + "=" * 80)
    print("TRAFFIC PATTERN ANALYSIS")
    print("=" * 80)

    traffic_analysis = {
        "hourly_patterns": {},
        "concurrent_patterns": {},
        "dag_frequency": [],
        "task_concurrency": {},
        "insights": []
    }

    # 1. Hourly Traffic Patterns
    print("\n📊 HOURLY TRAFFIC PATTERNS")
    print("-" * 40)
    
    try:
        hourly_query = DiagnosticQueries.get_traffic_pattern_hourly_query(ANALYSIS_DAYS)
        hourly_records = postgres_hook.get_records(hourly_query)
        
        if hourly_records:
            print(f"{'Hour':<6} {'Total Runs':<12} {'Avg/Day':<10} {'Max/Hour':<10} {'Avg Duration':<15}")
            print("-" * 60)
            
            hourly_data = []
            peak_hour = None
            peak_runs = 0
            quiet_hours = []
            
            for record in hourly_records:
                hour = int(record[0])
                total_runs = int(record[1])
                avg_runs = float(record[2]) if record[2] else 0
                max_runs = int(record[3]) if record[3] else 0
                avg_duration = float(record[5]) if record[5] else 0
                
                duration_str = f"{avg_duration/60:.1f}m" if avg_duration < 3600 else f"{avg_duration/3600:.1f}h"
                
                print(f"{hour:02d}:00  {total_runs:<12} {avg_runs:<10.1f} {max_runs:<10} {duration_str:<15}")
                
                hourly_data.append({
                    "hour": hour,
                    "total_runs": total_runs,
                    "avg_runs_per_day": avg_runs,
                    "max_runs_in_hour": max_runs,
                    "avg_duration_seconds": avg_duration
                })
                
                # Track peak hour
                if total_runs > peak_runs:
                    peak_runs = total_runs
                    peak_hour = hour
                
                # Track quiet hours (less than 10% of peak)
                if total_runs < peak_runs * 0.1 and total_runs > 0:
                    quiet_hours.append(hour)
            
            traffic_analysis["hourly_patterns"] = {
                "data": hourly_data,
                "peak_hour": peak_hour,
                "peak_runs": peak_runs,
                "quiet_hours": quiet_hours
            }
            
            print(f"\n🔥 Peak Hour: {peak_hour:02d}:00 with {peak_runs} total runs")
            if quiet_hours:
                quiet_hours_str = ", ".join([f"{h:02d}:00" for h in quiet_hours[:5]])
                print(f"😴 Quiet Hours: {quiet_hours_str}")
                if len(quiet_hours) > 5:
                    print(f"   ... and {len(quiet_hours) - 5} more")
            
            # Generate insights
            if peak_runs > 100:
                traffic_analysis["insights"].append(
                    f"⚠️ High traffic during peak hour ({peak_hour:02d}:00) with {peak_runs} runs - consider load balancing"
                )
            
            if len(quiet_hours) > 8:
                traffic_analysis["insights"].append(
                    f"💡 {len(quiet_hours)} quiet hours detected - opportunity to shift non-critical workloads"
                )
                
    except Exception as e:
        print(f"Error analyzing hourly patterns: {e}")
        traffic_analysis["hourly_patterns"]["error"] = str(e)

    # 2. Concurrent DAG Run Patterns
    print("\n🔄 CONCURRENT DAG RUN PATTERNS")
    print("-" * 40)
    
    try:
        concurrent_query = DiagnosticQueries.get_traffic_pattern_concurrent_query(ANALYSIS_DAYS)
        concurrent_records = postgres_hook.get_records(concurrent_query)
        
        if concurrent_records and concurrent_records[0]:
            record = concurrent_records[0]
            max_concurrent = int(record[0]) if record[0] else 0
            avg_concurrent = float(record[1]) if record[1] else 0
            p95_concurrent = float(record[2]) if record[2] else 0
            p99_concurrent = float(record[3]) if record[3] else 0
            
            print(f"Max Concurrent DAGs: {max_concurrent}")
            print(f"Avg Concurrent DAGs: {avg_concurrent:.1f}")
            print(f"P95 Concurrent DAGs: {p95_concurrent:.1f}")
            print(f"P99 Concurrent DAGs: {p99_concurrent:.1f}")
            
            traffic_analysis["concurrent_patterns"] = {
                "max_concurrent_dags": max_concurrent,
                "avg_concurrent_dags": avg_concurrent,
                "p95_concurrent_dags": p95_concurrent,
                "p99_concurrent_dags": p99_concurrent
            }
            
            # Check against parallelism settings
            try:
                parallelism = int(conf.get("core", "parallelism", fallback="32"))
                max_active_runs = int(conf.get("core", "max_active_runs_per_dag", fallback="16"))
                
                if max_concurrent > max_active_runs * 0.8:
                    traffic_analysis["insights"].append(
                        f"⚠️ Peak concurrent DAGs ({max_concurrent}) approaching max_active_runs_per_dag ({max_active_runs})"
                    )
                
                if p95_concurrent > max_active_runs * 0.6:
                    traffic_analysis["insights"].append(
                        f"💡 P95 concurrent DAGs ({p95_concurrent:.0f}) is high - consider increasing max_active_runs_per_dag"
                    )
            except Exception:
                pass
                
    except Exception as e:
        print(f"Error analyzing concurrent patterns: {e}")
        traffic_analysis["concurrent_patterns"]["error"] = str(e)

    # 3. DAG Frequency Analysis
    print("\n📈 DAG EXECUTION FREQUENCY")
    print("-" * 40)
    
    try:
        frequency_query = DiagnosticQueries.get_traffic_pattern_dag_frequency_query(ANALYSIS_DAYS)
        frequency_records = postgres_hook.get_records(frequency_query)
        
        if frequency_records:
            print(f"{'DAG ID':<35} {'Total Runs':<12} {'Runs/Day':<10} {'Variance':<15}")
            print("-" * 75)
            
            high_frequency_dags = []
            variable_duration_dags = []
            
            for record in frequency_records[:15]:  # Top 15
                dag_id = record[0]
                total_runs = int(record[1])
                runs_per_day = float(record[3]) if record[3] else 0
                variance = record[6] if record[6] else "Unknown"
                
                print(f"{dag_id:<35} {total_runs:<12} {runs_per_day:<10.1f} {variance:<15}")
                
                traffic_analysis["dag_frequency"].append({
                    "dag_id": dag_id,
                    "total_runs": total_runs,
                    "runs_per_day": runs_per_day,
                    "duration_variance": variance
                })
                
                # Track high frequency DAGs
                if runs_per_day > 50:
                    high_frequency_dags.append((dag_id, runs_per_day))
                
                # Track variable duration DAGs
                if variance == "High Variance":
                    variable_duration_dags.append(dag_id)
            
            if high_frequency_dags:
                print(f"\n🔥 High Frequency DAGs ({len(high_frequency_dags)}):")
                for dag_id, rpd in high_frequency_dags[:3]:
                    print(f"   - {dag_id}: {rpd:.1f} runs/day")
                traffic_analysis["insights"].append(
                    f"⚠️ {len(high_frequency_dags)} DAGs running >50 times/day - verify scheduling is intentional"
                )
            
            if variable_duration_dags:
                print(f"\n⚠️ Variable Duration DAGs ({len(variable_duration_dags)}):")
                for dag_id in variable_duration_dags[:3]:
                    print(f"   - {dag_id}")
                traffic_analysis["insights"].append(
                    f"💡 {len(variable_duration_dags)} DAGs have high duration variance - investigate performance inconsistencies"
                )
                
    except Exception as e:
        print(f"Error analyzing DAG frequency: {e}")
        traffic_analysis["dag_frequency"] = {"error": str(e)}

    # 4. Task Concurrency Patterns
    print("\n⚙️ TASK CONCURRENCY PATTERNS")
    print("-" * 40)
    
    try:
        task_query = DiagnosticQueries.get_traffic_pattern_task_concurrency_query(ANALYSIS_DAYS)
        task_records = postgres_hook.get_records(task_query)
        
        if task_records and task_records[0]:
            record = task_records[0]
            max_concurrent_tasks = int(record[0]) if record[0] else 0
            avg_concurrent_tasks = float(record[1]) if record[1] else 0
            p95_concurrent_tasks = float(record[2]) if record[2] else 0
            p99_concurrent_tasks = float(record[3]) if record[3] else 0
            
            print(f"Max Concurrent Tasks: {max_concurrent_tasks}")
            print(f"Avg Concurrent Tasks: {avg_concurrent_tasks:.1f}")
            print(f"P95 Concurrent Tasks: {p95_concurrent_tasks:.1f}")
            print(f"P99 Concurrent Tasks: {p99_concurrent_tasks:.1f}")
            
            traffic_analysis["task_concurrency"] = {
                "max_concurrent_tasks": max_concurrent_tasks,
                "avg_concurrent_tasks": avg_concurrent_tasks,
                "p95_concurrent_tasks": p95_concurrent_tasks,
                "p99_concurrent_tasks": p99_concurrent_tasks
            }
            
            # Check against parallelism
            try:
                parallelism = int(conf.get("core", "parallelism", fallback="32"))
                
                utilization = (avg_concurrent_tasks / parallelism) * 100
                peak_utilization = (max_concurrent_tasks / parallelism) * 100
                
                print(f"\nParallelism Utilization:")
                print(f"  Average: {utilization:.1f}%")
                print(f"  Peak: {peak_utilization:.1f}%")
                
                if peak_utilization > 90:
                    traffic_analysis["insights"].append(
                        f"🚨 Peak task concurrency ({max_concurrent_tasks}) exceeds 90% of parallelism ({parallelism}) - increase parallelism"
                    )
                elif peak_utilization > 75:
                    traffic_analysis["insights"].append(
                        f"⚠️ Peak task concurrency ({max_concurrent_tasks}) exceeds 75% of parallelism ({parallelism}) - monitor closely"
                    )
                elif utilization < 30:
                    traffic_analysis["insights"].append(
                        f"💡 Low average utilization ({utilization:.1f}%) - parallelism ({parallelism}) may be over-provisioned"
                    )
            except Exception:
                pass
                
    except Exception as e:
        print(f"Error analyzing task concurrency: {e}")
        traffic_analysis["task_concurrency"]["error"] = str(e)

    # Summary
    print(f"\n📊 TRAFFIC ANALYSIS SUMMARY")
    print("-" * 40)
    print(f"Insights Generated: {len(traffic_analysis['insights'])}")
    if traffic_analysis["insights"]:
        print("\nKey Insights:")
        for insight in traffic_analysis["insights"]:
            print(f"  {insight}")
    else:
        print("✅ No significant traffic pattern issues detected")

    return traffic_analysis


@task()
def collect_system_metrics(**context):
    """Collect system metrics like disk space, memory, CPU, and container statistics"""
    import psutil
    import shutil
    import platform
    import subprocess
    import os
    from datetime import datetime
    
    print("\n" + "=" * 80)
    print("SYSTEM METRICS COLLECTION")
    print("=" * 80)
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "collection_errors": []
    }
    
    try:
        # System Information
        print("📋 SYSTEM INFORMATION")
        print("-" * 40)
        
        system_info = {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "hostname": platform.node(),
            "python_version": platform.python_version()
        }
        
        metrics["system_info"] = system_info
        
        for key, value in system_info.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        
    except Exception as e:
        error_msg = f"Error collecting system info: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # CPU Information
        print(f"\n🖥️ CPU METRICS")
        print("-" * 40)
        
        cpu_info = {
            "cpu_count_logical": psutil.cpu_count(logical=True),
            "cpu_count_physical": psutil.cpu_count(logical=False),
            "cpu_percent_current": psutil.cpu_percent(interval=1),
            "cpu_percent_per_core": psutil.cpu_percent(interval=1, percpu=True),
            "load_average": os.getloadavg() if hasattr(os, 'getloadavg') else None,
            "cpu_freq": psutil.cpu_freq()._asdict() if psutil.cpu_freq() else None
        }
        
        metrics["cpu_info"] = cpu_info
        
        print(f"Logical CPUs: {cpu_info['cpu_count_logical']}")
        print(f"Physical CPUs: {cpu_info['cpu_count_physical']}")
        print(f"CPU Usage: {cpu_info['cpu_percent_current']:.1f}%")
        if cpu_info['load_average']:
            print(f"Load Average: {cpu_info['load_average']}")
        if cpu_info['cpu_freq']:
            print(f"CPU Frequency: {cpu_info['cpu_freq']['current']:.0f} MHz")
        
    except Exception as e:
        error_msg = f"Error collecting CPU metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Memory Information
        print(f"\n💾 MEMORY METRICS")
        print("-" * 40)
        
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        memory_info = {
            "total_gb": round(memory.total / (1024**3), 2),
            "available_gb": round(memory.available / (1024**3), 2),
            "used_gb": round(memory.used / (1024**3), 2),
            "percent_used": memory.percent,
            "swap_total_gb": round(swap.total / (1024**3), 2),
            "swap_used_gb": round(swap.used / (1024**3), 2),
            "swap_percent": swap.percent
        }
        
        metrics["memory_info"] = memory_info
        
        print(f"Total Memory: {memory_info['total_gb']} GB")
        print(f"Available Memory: {memory_info['available_gb']} GB")
        print(f"Used Memory: {memory_info['used_gb']} GB ({memory_info['percent_used']:.1f}%)")
        print(f"Swap Total: {memory_info['swap_total_gb']} GB")
        print(f"Swap Used: {memory_info['swap_used_gb']} GB ({memory_info['swap_percent']:.1f}%)")
        
    except Exception as e:
        error_msg = f"Error collecting memory metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Disk Information
        print(f"\n💽 DISK METRICS")
        print("-" * 40)
        
        # Get disk usage for key directories
        disk_info = {}
        key_paths = [
            "/",  # Root filesystem
            "/usr/local/airflow",  # Airflow directory
            "/tmp",  # Temp directory
            "/var/log"  # Log directory
        ]
        
        for path in key_paths:
            try:
                if os.path.exists(path):
                    usage = shutil.disk_usage(path)
                    disk_info[path] = {
                        "total_gb": round(usage.total / (1024**3), 2),
                        "used_gb": round(usage.used / (1024**3), 2),
                        "free_gb": round(usage.free / (1024**3), 2),
                        "percent_used": round((usage.used / usage.total) * 100, 1)
                    }
                    
                    print(f"{path}:")
                    print(f"  Total: {disk_info[path]['total_gb']} GB")
                    print(f"  Used: {disk_info[path]['used_gb']} GB ({disk_info[path]['percent_used']}%)")
                    print(f"  Free: {disk_info[path]['free_gb']} GB")
            except Exception as e:
                disk_info[path] = {"error": str(e)}
                print(f"  Error accessing {path}: {str(e)}")
        
        metrics["disk_info"] = disk_info
        
    except Exception as e:
        error_msg = f"Error collecting disk metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Process Information
        print(f"\n🔄 PROCESS METRICS")
        print("-" * 40)
        
        # Current process info
        current_process = psutil.Process()
        
        process_info = {
            "current_pid": current_process.pid,
            "current_memory_mb": round(current_process.memory_info().rss / (1024**2), 2),
            "current_cpu_percent": current_process.cpu_percent(),
            "total_processes": len(psutil.pids()),
            "airflow_processes": []
        }
        
        # Find Airflow-related processes
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info', 'cpu_percent']):
            try:
                if proc.info['cmdline'] and any('airflow' in arg.lower() for arg in proc.info['cmdline']):
                    process_info["airflow_processes"].append({
                        "pid": proc.info['pid'],
                        "name": proc.info['name'],
                        "memory_mb": round(proc.info['memory_info'].rss / (1024**2), 2),
                        "cpu_percent": proc.info['cpu_percent']
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        metrics["process_info"] = process_info
        
        print(f"Current Process PID: {process_info['current_pid']}")
        print(f"Current Process Memory: {process_info['current_memory_mb']} MB")
        print(f"Total Processes: {process_info['total_processes']}")
        print(f"Airflow Processes: {len(process_info['airflow_processes'])}")
        
        for proc in process_info["airflow_processes"][:5]:  # Show top 5
            print(f"  PID {proc['pid']}: {proc['memory_mb']} MB, {proc['cpu_percent']}% CPU")
        
    except Exception as e:
        error_msg = f"Error collecting process metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Network Information (basic)
        print(f"\n🌐 NETWORK METRICS")
        print("-" * 40)
        
        network_info = {
            "network_interfaces": len(psutil.net_if_addrs()),
            "network_connections": len(psutil.net_connections()),
        }
        
        # Get network I/O stats
        net_io = psutil.net_io_counters()
        if net_io:
            network_info.update({
                "bytes_sent_mb": round(net_io.bytes_sent / (1024**2), 2),
                "bytes_recv_mb": round(net_io.bytes_recv / (1024**2), 2),
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv
            })
        
        metrics["network_info"] = network_info
        
        print(f"Network Interfaces: {network_info['network_interfaces']}")
        print(f"Active Connections: {network_info['network_connections']}")
        if 'bytes_sent_mb' in network_info:
            print(f"Bytes Sent: {network_info['bytes_sent_mb']} MB")
            print(f"Bytes Received: {network_info['bytes_recv_mb']} MB")
        
    except Exception as e:
        error_msg = f"Error collecting network metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # DAG Folder Analysis
        print(f"\n� DAGT FOLDER ANALYSIS")
        print("-" * 40)
        
        dag_folder_info = {}
        
        # Get DAG folder path
        dags_folder = conf.get("core", "dags_folder", fallback="/usr/local/airflow/dags")
        dag_folder_info["dags_folder_path"] = dags_folder
        
        if os.path.exists(dags_folder):
            print(f"DAGs Folder: {dags_folder}")
            
            # Check for .airflowignore file
            airflowignore_path = os.path.join(dags_folder, ".airflowignore")
            dag_folder_info["has_airflowignore"] = os.path.exists(airflowignore_path)
            
            if dag_folder_info["has_airflowignore"]:
                print("✅ .airflowignore file found")
                try:
                    with open(airflowignore_path, 'r') as f:
                        ignore_content = f.read().strip()
                        dag_folder_info["airflowignore_lines"] = len(ignore_content.split('\n')) if ignore_content else 0
                        dag_folder_info["airflowignore_size"] = len(ignore_content)
                        print(f"  Lines: {dag_folder_info['airflowignore_lines']}")
                        print(f"  Size: {dag_folder_info['airflowignore_size']} bytes")
                except Exception as e:
                    dag_folder_info["airflowignore_error"] = str(e)
                    print(f"  Error reading .airflowignore: {str(e)}")
            else:
                print("⚠️ No .airflowignore file found")
            
            # Scan for files in DAG folder
            try:
                all_files = []
                python_files = []
                non_python_files = []
                total_size_bytes = 0
                python_size_bytes = 0
                non_python_size_bytes = 0
                
                for root, dirs, files in os.walk(dags_folder):
                    # Skip hidden directories and __pycache__
                    dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
                    
                    for file in files:
                        if file.startswith('.'):
                            continue  # Skip hidden files
                        
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, dags_folder)
                        
                        # Get file size
                        try:
                            file_size = os.path.getsize(file_path)
                            total_size_bytes += file_size
                        except OSError:
                            file_size = 0
                        
                        all_files.append({
                            "path": relative_path,
                            "size_bytes": file_size
                        })
                        
                        if file.endswith('.py'):
                            python_files.append(relative_path)
                            python_size_bytes += file_size
                        else:
                            non_python_files.append(relative_path)
                            non_python_size_bytes += file_size
                
                dag_folder_info["total_files"] = len(all_files)
                dag_folder_info["python_files_count"] = len(python_files)
                dag_folder_info["non_python_files_count"] = len(non_python_files)
                dag_folder_info["non_python_files"] = non_python_files[:20]  # Limit to first 20
                
                # Size information
                dag_folder_info["total_size_bytes"] = total_size_bytes
                dag_folder_info["total_size_mb"] = round(total_size_bytes / (1024 * 1024), 2)
                dag_folder_info["python_size_bytes"] = python_size_bytes
                dag_folder_info["python_size_mb"] = round(python_size_bytes / (1024 * 1024), 2)
                dag_folder_info["non_python_size_bytes"] = non_python_size_bytes
                dag_folder_info["non_python_size_mb"] = round(non_python_size_bytes / (1024 * 1024), 2)
                
                print(f"Total files: {len(all_files)} ({dag_folder_info['total_size_mb']} MB)")
                print(f"Python files: {len(python_files)} ({dag_folder_info['python_size_mb']} MB)")
                print(f"Non-Python files: {len(non_python_files)} ({dag_folder_info['non_python_size_mb']} MB)")
                
                if non_python_files:
                    print("Non-Python files found:")
                    for file in non_python_files[:10]:  # Show first 10
                        print(f"  - {file}")
                    if len(non_python_files) > 10:
                        print(f"  ... and {len(non_python_files) - 10} more")
                
                # Categorize non-Python files by extension
                file_extensions = {}
                for file in non_python_files:
                    ext = os.path.splitext(file)[1].lower() or 'no_extension'
                    file_extensions[ext] = file_extensions.get(ext, 0) + 1
                
                dag_folder_info["file_extensions"] = file_extensions
                
                # Find largest files
                largest_files = sorted(all_files, key=lambda x: x['size_bytes'], reverse=True)[:10]
                dag_folder_info["largest_files"] = [
                    {
                        "path": f["path"],
                        "size_bytes": f["size_bytes"],
                        "size_mb": round(f["size_bytes"] / (1024 * 1024), 2)
                    }
                    for f in largest_files
                ]
                
                if file_extensions:
                    print("File types found:")
                    for ext, count in sorted(file_extensions.items(), key=lambda x: x[1], reverse=True):
                        print(f"  {ext}: {count} files")
                
                # Show largest files if there are any significant ones
                if dag_folder_info["largest_files"]:
                    large_files = [f for f in dag_folder_info["largest_files"] if f["size_mb"] > 0.1]  # > 100KB
                    if large_files:
                        print("Largest files:")
                        for file_info in large_files[:5]:
                            print(f"  - {file_info['path']}: {file_info['size_mb']} MB")
                
            except Exception as e:
                dag_folder_info["scan_error"] = str(e)
                print(f"Error scanning DAG folder: {str(e)}")
        else:
            dag_folder_info["folder_exists"] = False
            print(f"❌ DAGs folder not found: {dags_folder}")
        
        metrics["dag_folder_info"] = dag_folder_info
        
    except Exception as e:
        error_msg = f"Error analyzing DAG folder: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    try:
        # Container/Environment specific information
        print(f"\n🐳 CONTAINER METRICS")
        print("-" * 40)
        
        container_info = {}
        
        # Check if running in container
        if os.path.exists('/.dockerenv'):
            container_info["is_container"] = True
            container_info["container_type"] = "Docker"
        elif os.path.exists('/proc/1/cgroup'):
            with open('/proc/1/cgroup', 'r') as f:
                cgroup_content = f.read()
                if 'docker' in cgroup_content or 'containerd' in cgroup_content:
                    container_info["is_container"] = True
                    container_info["container_type"] = "Container"
                else:
                    container_info["is_container"] = False
        else:
            container_info["is_container"] = False
        
        # Environment variables of interest
        env_vars = {}
        interesting_vars = [
            'AIRFLOW_HOME', 'AIRFLOW__CORE__DAGS_FOLDER', 'AIRFLOW__CORE__EXECUTOR',
            'PYTHONPATH', 'PATH', 'HOSTNAME', 'USER'
        ]
        
        for var in interesting_vars:
            env_vars[var] = os.getenv(var, "Not Set")
        
        container_info["environment_variables"] = env_vars
        
        metrics["container_info"] = container_info
        
        print(f"Running in Container: {container_info.get('is_container', 'Unknown')}")
        if container_info.get('container_type'):
            print(f"Container Type: {container_info['container_type']}")
        
        print("Key Environment Variables:")
        for var, value in env_vars.items():
            display_value = value[:50] + "..." if len(str(value)) > 50 else value
            print(f"  {var}: {display_value}")
        
    except Exception as e:
        error_msg = f"Error collecting container metrics: {str(e)}"
        metrics["collection_errors"].append(error_msg)
        print(f"❌ {error_msg}")
    
    # Summary
    print(f"\n📊 COLLECTION SUMMARY")
    print("-" * 40)
    print(f"Metrics collected: {len([k for k in metrics.keys() if k not in ['timestamp', 'collection_errors']])}")
    print(f"Collection errors: {len(metrics['collection_errors'])}")
    
    # DAG folder summary
    if "dag_folder_info" in metrics:
        dag_folder = metrics["dag_folder_info"]
        total_size_mb = dag_folder.get("total_size_mb", 0)
        print(f"DAG folder analysis:")
        print(f"  .airflowignore: {'✅ Present' if dag_folder.get('has_airflowignore') else '⚠️ Missing'}")
        print(f"  Total size: {total_size_mb} MB")
        non_python = dag_folder.get("non_python_files_count", 0)
        if non_python > 0:
            print(f"  Non-Python files: ⚠️ {non_python} found")
        else:
            print(f"  Non-Python files: ✅ None found")
    
    if metrics['collection_errors']:
        print("Errors encountered:")
        for error in metrics['collection_errors']:
            print(f"  - {error}")
    
    return metrics


@task()
def generate_complete_report(**context):
    """Generate diagnostics report"""

    # Get runtime parameters
    dag_run = context.get("dag_run")
    params = context.get("params", {})

    # Get parameters from DAG run config or fallback to params/defaults
    if dag_run and dag_run.conf:
        analysis_days = dag_run.conf.get(
            "analysis_days", params.get("analysis_days", ANALYSIS_DAYS)
        )
        env_name = dag_run.conf.get("env_name", params.get("env_name", ENV_NAME))
        s3_bucket = dag_run.conf.get("s3_bucket", params.get("s3_bucket", S3_BUCKET))
    else:
        analysis_days = params.get("analysis_days", ANALYSIS_DAYS)
        env_name = params.get("env_name", ENV_NAME)
        s3_bucket = params.get("s3_bucket", S3_BUCKET)

    # Collect all analysis results
    config_analysis = context["task_instance"].xcom_pull(
        task_ids="capture_airflow_configuration"
    )
    dag_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_serialized_dags"
    )
    performance_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_dag_performance"
    )
    failure_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_task_failures"
    )
    instability_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_serialization_instability"
    )
    traffic_analysis = context["task_instance"].xcom_pull(
        task_ids="analyze_traffic_patterns"
    )
    system_metrics = context["task_instance"].xcom_pull(
        task_ids="collect_system_metrics"
    )

    # Generate diagnostics report
    report_lines = []
    report_lines.append("=" * 100)
    report_lines.append("DAG DIAGNOSTICS REPORT")
    report_lines.append("=" * 100)
    report_lines.append(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    report_lines.append(f"Environment: {env_name}")
    report_lines.append(f"Analysis Period: {analysis_days} days")
    report_lines.append(f"S3 Bucket: {s3_bucket}")
    
    # Add runtime environment info to header
    try:
        if system_metrics and "system_info" in system_metrics:
            python_version = system_metrics["system_info"].get("python_version", "Unknown")
            platform_info = system_metrics["system_info"].get("platform", "Unknown")
            report_lines.append(f"Runtime: Python {python_version} on {platform_info}")
        else:
            import platform
            python_version = platform.python_version()
            platform_info = platform.platform()
            report_lines.append(f"Runtime: Python {python_version} on {platform_info}")
    except Exception as e:
        report_lines.append(f"Runtime: Error retrieving - {str(e)}")

    # Add Airflow and Python version to header for prominence
    try:
        config = config_analysis.get("configuration", {})
        env_info = config.get("environment_info", {})
        airflow_version = env_info.get("airflow_version", "Unknown")
        report_lines.append(f"Airflow Version: {airflow_version}")
    except Exception as e:
        report_lines.append(f"Airflow Version: Error retrieving - {str(e)}")

    # Add Python version from system metrics
    try:
        if system_metrics and "system_info" in system_metrics:
            python_version = system_metrics["system_info"].get("python_version", "Unknown")
            report_lines.append(f"Python Version: {python_version}")
        else:
            # Fallback to direct detection
            import platform
            python_version = platform.python_version()
            report_lines.append(f"Python Version: {python_version}")
    except Exception as e:
        report_lines.append(f"Python Version: Error retrieving - {str(e)}")

    report_lines.append("")

    # Configuration Analysis Section
    report_lines.append("🔧 AIRFLOW CONFIGURATION ANALYSIS")
    report_lines.append("-" * 50)

    # Configuration change detection
    if config_analysis["config_changed"]:
        report_lines.append("⚠️ CONFIGURATION HAS CHANGED SINCE LAST RUN")
        report_lines.append(f"Previous hash: {config_analysis['previous_hash']}")
        report_lines.append(f"Current hash: {config_analysis['config_hash']}")
    else:
        report_lines.append("✅ No configuration changes detected since last run")
    
    # Configuration conflicts and issues (PROMINENT SECTION)
    config_issues = config_analysis.get("issues", [])
    if config_issues:
        report_lines.append(f"\n🚨 CONFIGURATION CONFLICTS AND ISSUES ({len(config_issues)}):")
        report_lines.append("-" * 50)
        for issue in config_issues:
            # Add indentation for readability
            report_lines.append(f"{issue}")
        report_lines.append("")
    else:
        report_lines.append("\n✅ No configuration conflicts detected")
        report_lines.append("")

    # Environment information
    try:
        config = config_analysis.get("configuration", {})
        env_info = config.get("environment_info", {})

        # Debug: Print what we're getting
        print(f"DEBUG - config keys: {list(config.keys()) if config else 'None'}")
        print(f"DEBUG - env_info: {env_info}")

        airflow_version = env_info.get("airflow_version", "Unknown")
        executor = env_info.get("executor", "Unknown")
        dags_folder = env_info.get("dags_folder", "Unknown")

        report_lines.append(f"\nEnvironment Information:")
        report_lines.append(f"  Airflow Version: {airflow_version}")
        report_lines.append(f"  Executor: {executor}")
        report_lines.append(f"  DAGs Folder: {dags_folder}")
        
        # Add Python version to environment info
        try:
            if system_metrics and "system_info" in system_metrics:
                python_version = system_metrics["system_info"].get("python_version", "Unknown")
            else:
                import platform
                python_version = platform.python_version()
            report_lines.append(f"  Python Version: {python_version}")
        except Exception:
            report_lines.append(f"  Python Version: Unknown")
    except Exception as e:
        report_lines.append(
            f"\nEnvironment Information: Error retrieving info - {str(e)}"
        )
        print(f"DEBUG - Environment info error: {str(e)}")

    # Detailed configuration table
    report_lines.append(f"\nDetailed Configuration Analysis:")
    report_lines.append(f"{'Setting':<40} {'Value':<25} {'Impact':<8} {'Status':<15}")
    report_lines.append("-" * 95)

    try:
        config = config_analysis.get("configuration", {})

        # Define filtered settings for report (excluding sensitive/irrelevant ones)
        stability_settings = {
            "core": [
                "executor",
                "parallelism",
                "max_active_tasks_per_dag",
                "max_active_runs_per_dag",
                "dagbag_import_timeout",
                "dag_file_processor_timeout",
                "killed_task_cleanup_time",
                "dag_discovery_safe_mode",
                "compress_serialized_dags",
                "min_serialized_dag_update_interval",
                "min_serialized_dag_fetch_interval",
            ],
            "scheduler": [
                "dag_dir_list_interval",
                "min_file_process_interval",
                "catchup_by_default",
                "max_dagruns_to_create_per_loop",
                "max_dagruns_per_loop_to_schedule",
                "pool_metrics_interval",
                "orphaned_tasks_check_interval",
                "parsing_processes",
                "file_parsing_sort_mode",
                "use_row_level_locking",
                "max_tis_per_query",
            ],
            "celery": [
                "worker_concurrency",
                "task_track_started",
                "task_publish_retry",
                "worker_enable_remote_control",
            ],
            "webserver": [
                "workers",
                "worker_timeout",
                "worker_refresh_batch_size",
                "reload_on_plugin_change",
            ],
            "database": [
                "sql_alchemy_pool_size",
                "sql_alchemy_max_overflow",
                "sql_alchemy_pool_recycle",
                "sql_alchemy_pool_pre_ping",
            ],
        }

        # Impact levels for display (only for settings included in report)
        impact_levels = {
            "executor": "HIGH",
            "parallelism": "HIGH",
            "max_active_tasks_per_dag": "HIGH",
            "max_active_runs_per_dag": "HIGH",
            "dagbag_import_timeout": "HIGH",
            "dag_file_processor_timeout": "HIGH",
            "min_serialized_dag_update_interval": "HIGH",
            "min_serialized_dag_fetch_interval": "HIGH",
            "dag_dir_list_interval": "HIGH",
            "min_file_process_interval": "HIGH",
            "parsing_processes": "HIGH",
            "worker_concurrency": "HIGH",
            "sql_alchemy_pool_size": "HIGH",
            "sql_alchemy_max_overflow": "MEDIUM",
        }

        # No sensitive settings in the filtered report
        sensitive_settings = []

        for section_name, settings in stability_settings.items():
            section_config = config.get(section_name, {})

            for setting_name in settings:
                value = section_config.get(setting_name, "NOT_SET")
                impact = impact_levels.get(setting_name, "MEDIUM")

                # Determine status (same logic as in capture function)
                if value == "NOT_SET":
                    status = "🔶 Default"
                elif setting_name in sensitive_settings:
                    status = "✅ Set"
                else:
                    status = analyze_config_setting(
                        section_name, setting_name, value, {"stability_impact": impact}
                    )

                # Format display value
                if setting_name in sensitive_settings and value != "NOT_SET":
                    display_value = "***MASKED***"
                else:
                    display_value = str(value)
                    if len(display_value) > 22:
                        display_value = display_value[:19] + "..."

                # Add to report
                full_name = f"{section_name}.{setting_name}"
                report_lines.append(
                    f"{full_name:<40} {display_value:<25} {impact:<8} {status:<15}"
                )

        # Summary statistics
        report_lines.append("")
        report_lines.append("Configuration Summary:")

        # Count settings by status
        status_counts = {
            "✅ Good": 0,
            "✅ Set": 0,
            "🔶 Default": 0,
            "⚠️ Warning": 0,
            "❌ Error": 0,
        }

        for section_name, settings in stability_settings.items():
            section_config = config.get(section_name, {})
            for setting_name in settings:
                value = section_config.get(setting_name, "NOT_SET")
                impact = impact_levels.get(setting_name, "MEDIUM")

                if value == "NOT_SET":
                    status_counts["🔶 Default"] += 1
                elif setting_name in sensitive_settings:
                    status_counts["✅ Set"] += 1
                else:
                    status = analyze_config_setting(
                        section_name, setting_name, value, {"stability_impact": impact}
                    )
                    if "Good" in status:
                        status_counts["✅ Good"] += 1
                    elif "Set" in status:
                        status_counts["✅ Set"] += 1
                    elif "Default" in status:
                        status_counts["🔶 Default"] += 1
                    elif "⚠️" in status:
                        status_counts["⚠️ Warning"] += 1
                    else:
                        status_counts["❌ Error"] += 1

        for status, count in status_counts.items():
            if count > 0:
                report_lines.append(f"  {status}: {count} settings")

    except Exception as e:
        report_lines.append(f"  Error generating configuration table: {str(e)}")

    report_lines.append(
        f"\nTotal settings analyzed: {config_analysis['total_settings']}"
    )
    report_lines.append("")

    # DAG Analysis Section
    report_lines.append("📊 DAG SERIALIZATION ANALYSIS")
    report_lines.append("-" * 50)
    report_lines.append(
        "NOTE: Sizes refer to serialized DAG data in database, not source file sizes"
    )
    report_lines.append(f"Total DAGs: {dag_analysis['total_dags']}")
    report_lines.append(
        f"Average serialized size: {dag_analysis['avg_size_mb']:.2f} MB"
    )
    report_lines.append(f"Largest serialized DAG: {dag_analysis['max_size_mb']:.2f} MB")
    report_lines.append(f"Suspicious DAGs: {len(dag_analysis['suspicious_dags'])}")

    if dag_analysis["suspicious_dags"]:
        report_lines.append("\n🚨 SUSPICIOUS DAGs:")
        for dag in dag_analysis["suspicious_dags"][:5]:
            report_lines.append(
                f"  - {dag['dag_id']}: {dag['size_mb']:.2f} MB ({', '.join(dag['issues'])})"
            )

    # Size distribution
    report_lines.append("\nSize Distribution:")
    for size_range, count in dag_analysis["size_distribution"].items():
        report_lines.append(f"  {size_range}: {count} DAGs")
    
    # Complete DAG list with sizes
    report_lines.append(f"\nAll DAGs by Size:")
    report_lines.append(f"{'DAG ID':<40} {'JSON (MB)':<10} {'Storage (MB)':<12} {'File Location':<50}")
    report_lines.append("-" * 120)
    
    # Get all DAGs sorted by size (largest first)
    try:
        # Use the complete DAG list from the analysis
        all_dags = dag_analysis.get("all_dags_by_size", dag_analysis.get("top_10_largest", []))
        
        # DAGs are already sorted by size in the query (ORDER BY octet_length(data::text) DESC)
        sorted_dags = all_dags
        
        for dag_info in sorted_dags:
            dag_id = dag_info["dag_id"]
            size_mb = dag_info["size_mb"]
            storage_size_mb = dag_info.get("storage_size_mb")
            fileloc = dag_info.get("fileloc", "Unknown")
            
            # Truncate file location if too long
            if len(fileloc) > 47:
                fileloc = "..." + fileloc[-44:]
            
            storage_display = f"{storage_size_mb:.2f}" if storage_size_mb is not None else "N/A"
            report_lines.append(f"{dag_id:<40} {size_mb:<10.2f} {storage_display:<12} {fileloc:<50}")
            
    except Exception as e:
        report_lines.append(f"Error displaying complete DAG list: {str(e)}")
    
    report_lines.append("")

    # Performance Analysis Section
    report_lines.append("⚡ PERFORMANCE ANALYSIS")
    report_lines.append("-" * 50)
    report_lines.append(f"Active DAGs: {performance_analysis['active_dags']}")
    report_lines.append(
        f"Average success rate: {performance_analysis['avg_success_rate']:.1f}%"
    )

    if performance_analysis["performance_issues"]:
        report_lines.append(
            f"\n⚠️ PERFORMANCE ISSUES ({len(performance_analysis['performance_issues'])}):"
        )
        for issue in performance_analysis["performance_issues"][:5]:
            report_lines.append(
                f"  - {issue['dag_id']}: {issue['success_rate']:.1f}% success ({', '.join(issue['issues'])})"
            )
    report_lines.append("")

    # Failure Analysis Section
    report_lines.append("❌ FAILURE ANALYSIS")
    report_lines.append("-" * 50)
    report_lines.append(
        f"Failing task types: {failure_analysis['total_failing_tasks']}"
    )

    if failure_analysis["top_failures"]:
        report_lines.append("\nTop failing tasks:")
        for failure in failure_analysis["top_failures"][:5]:
            report_lines.append(
                f"  - {failure['dag_id']}.{failure['task_id']}: {failure['failure_count']} failures"
            )
    report_lines.append("")

    # Instability Analysis Section
    report_lines.append("🔄 SERIALIZATION INSTABILITY")
    report_lines.append("-" * 50)
    report_lines.append(f"Unstable DAGs: {instability_analysis['unstable_dags']}")

    if instability_analysis["unstable_dag_list"]:
        report_lines.append("\nMost unstable DAGs:")
        for unstable in instability_analysis["unstable_dag_list"][:5]:
            report_lines.append(
                f"  - {unstable['dag_id']}: {unstable['hash_variations']} hash changes"
            )
    report_lines.append("")

    # Traffic Pattern Analysis Section
    report_lines.append("📊 TRAFFIC PATTERN ANALYSIS")
    report_lines.append("-" * 50)
    
    try:
        if traffic_analysis and isinstance(traffic_analysis, dict):
            # Hourly patterns
            hourly = traffic_analysis.get("hourly_patterns", {})
            if hourly and isinstance(hourly, dict) and "peak_hour" in hourly:
                report_lines.append(f"Peak Hour: {hourly['peak_hour']:02d}:00 ({hourly['peak_runs']} runs)")
                if hourly.get("quiet_hours"):
                    quiet_count = len(hourly["quiet_hours"])
                    report_lines.append(f"Quiet Hours: {quiet_count} hours with minimal activity")
            
            # Concurrent patterns
            concurrent = traffic_analysis.get("concurrent_patterns", {})
            if concurrent and isinstance(concurrent, dict) and "max_concurrent_dags" in concurrent:
                report_lines.append(f"\nConcurrency:")
                report_lines.append(f"  Max Concurrent DAGs: {concurrent['max_concurrent_dags']}")
                report_lines.append(f"  Avg Concurrent DAGs: {concurrent['avg_concurrent_dags']:.1f}")
                report_lines.append(f"  P95 Concurrent DAGs: {concurrent['p95_concurrent_dags']:.1f}")
            
            # Task concurrency
            task_conc = traffic_analysis.get("task_concurrency", {})
            if task_conc and isinstance(task_conc, dict) and "max_concurrent_tasks" in task_conc:
                report_lines.append(f"\nTask Concurrency:")
                report_lines.append(f"  Max Concurrent Tasks: {task_conc['max_concurrent_tasks']}")
                report_lines.append(f"  Avg Concurrent Tasks: {task_conc['avg_concurrent_tasks']:.1f}")
                report_lines.append(f"  P95 Concurrent Tasks: {task_conc['p95_concurrent_tasks']:.1f}")
                
                # Calculate utilization if possible
                try:
                    parallelism = int(conf.get("core", "parallelism", fallback="32"))
                    avg_util = (task_conc['avg_concurrent_tasks'] / parallelism) * 100
                    peak_util = (task_conc['max_concurrent_tasks'] / parallelism) * 100
                    report_lines.append(f"  Avg Utilization: {avg_util:.1f}%")
                    report_lines.append(f"  Peak Utilization: {peak_util:.1f}%")
                except Exception:
                    pass
            
            # High frequency DAGs
            dag_freq = traffic_analysis.get("dag_frequency", [])
            if dag_freq and isinstance(dag_freq, list):
                high_freq = [d for d in dag_freq if isinstance(d, dict) and d.get("runs_per_day", 0) > 50]
                if high_freq:
                    report_lines.append(f"\nHigh Frequency DAGs ({len(high_freq)}):")
                    for dag in high_freq[:5]:
                        report_lines.append(f"  - {dag['dag_id']}: {dag['runs_per_day']:.1f} runs/day")
            
            # Traffic insights
            insights = traffic_analysis.get("insights", [])
            if insights and isinstance(insights, list):
                report_lines.append(f"\nTraffic Insights:")
                for insight in insights:
                    report_lines.append(f"  {insight}")
        else:
            report_lines.append("Traffic analysis not available")
    except Exception as e:
        report_lines.append(f"Error displaying traffic analysis: {str(e)}")
    
    report_lines.append("")

    # System Metrics Section
    report_lines.append("🖥️ SYSTEM STATISTICS")
    report_lines.append("-" * 50)
    
    try:
        if system_metrics and "collection_errors" in system_metrics:
            # System Information
            if "system_info" in system_metrics:
                sys_info = system_metrics["system_info"]
                report_lines.append(f"Platform: {sys_info.get('platform', 'Unknown')}")
                report_lines.append(f"Hostname: {sys_info.get('hostname', 'Unknown')}")
                report_lines.append(f"Python Version: {sys_info.get('python_version', 'Unknown')}")
                report_lines.append(f"System: {sys_info.get('system', 'Unknown')} {sys_info.get('release', '')}")
                report_lines.append(f"Machine: {sys_info.get('machine', 'Unknown')}")
            
            # CPU Metrics
            if "cpu_info" in system_metrics:
                cpu = system_metrics["cpu_info"]
                report_lines.append(f"\nCPU:")
                report_lines.append(f"  Logical CPUs: {cpu.get('cpu_count_logical', 'Unknown')}")
                report_lines.append(f"  Physical CPUs: {cpu.get('cpu_count_physical', 'Unknown')}")
                report_lines.append(f"  CPU Usage: {cpu.get('cpu_percent_current', 'Unknown')}%")
                if cpu.get('load_average'):
                    load_avg = cpu['load_average']
                    report_lines.append(f"  Load Average: {load_avg[0]:.2f}, {load_avg[1]:.2f}, {load_avg[2]:.2f}")
            
            # Memory Metrics
            if "memory_info" in system_metrics:
                mem = system_metrics["memory_info"]
                report_lines.append(f"\nMemory:")
                report_lines.append(f"  Total: {mem.get('total_gb', 'Unknown')} GB")
                report_lines.append(f"  Used: {mem.get('used_gb', 'Unknown')} GB ({mem.get('percent_used', 'Unknown')}%)")
                report_lines.append(f"  Available: {mem.get('available_gb', 'Unknown')} GB")
                if mem.get('swap_total_gb', 0) > 0:
                    report_lines.append(f"  Swap Used: {mem.get('swap_used_gb', 'Unknown')} GB ({mem.get('swap_percent', 'Unknown')}%)")
            
            # Disk Metrics
            if "disk_info" in system_metrics:
                disk = system_metrics["disk_info"]
                report_lines.append(f"\nDisk Usage:")
                for path, info in disk.items():
                    if isinstance(info, dict) and "total_gb" in info:
                        report_lines.append(f"  {path}: {info['used_gb']} GB / {info['total_gb']} GB ({info['percent_used']}% used)")
                    elif isinstance(info, dict) and "error" in info:
                        report_lines.append(f"  {path}: Error - {info['error']}")
            
            # Process Metrics
            if "process_info" in system_metrics:
                proc = system_metrics["process_info"]
                report_lines.append(f"\nProcesses:")
                report_lines.append(f"  Total Processes: {proc.get('total_processes', 'Unknown')}")
                report_lines.append(f"  Airflow Processes: {len(proc.get('airflow_processes', []))}")
                report_lines.append(f"  Current Process Memory: {proc.get('current_memory_mb', 'Unknown')} MB")
                
                # Show top memory-consuming Airflow processes
                airflow_procs = proc.get('airflow_processes', [])
                if airflow_procs:
                    sorted_procs = sorted(airflow_procs, key=lambda x: x.get('memory_mb', 0), reverse=True)
                    report_lines.append(f"  Top Airflow Processes by Memory:")
                    for p in sorted_procs[:3]:
                        report_lines.append(f"    PID {p['pid']}: {p['memory_mb']} MB")
            
            # Container Information
            if "container_info" in system_metrics:
                container = system_metrics["container_info"]
                report_lines.append(f"\nContainer Environment:")
                report_lines.append(f"  Running in Container: {container.get('is_container', 'Unknown')}")
                if container.get('container_type'):
                    report_lines.append(f"  Container Type: {container['container_type']}")
                
                # Key environment variables
                env_vars = container.get('environment_variables', {})
                if env_vars:
                    report_lines.append(f"  Key Environment Variables:")
                    for var in ['AIRFLOW_HOME', 'AIRFLOW__CORE__EXECUTOR']:
                        if var in env_vars and env_vars[var] != "Not Set":
                            value = env_vars[var][:50] + "..." if len(str(env_vars[var])) > 50 else env_vars[var]
                            report_lines.append(f"    {var}: {value}")
            
            # DAG Folder Analysis
            if "dag_folder_info" in system_metrics:
                dag_folder = system_metrics["dag_folder_info"]
                report_lines.append(f"\nDAG Folder Analysis:")
                report_lines.append(f"  DAGs Folder: {dag_folder.get('dags_folder_path', 'Unknown')}")
                
                # .airflowignore status
                if dag_folder.get("has_airflowignore"):
                    lines = dag_folder.get("airflowignore_lines", 0)
                    size = dag_folder.get("airflowignore_size", 0)
                    report_lines.append(f"  .airflowignore: ✅ Present ({lines} lines, {size} bytes)")
                else:
                    report_lines.append(f"  .airflowignore: ⚠️ Missing")
                
                # File analysis
                total_files = dag_folder.get("total_files", 0)
                python_files = dag_folder.get("python_files_count", 0)
                non_python_files = dag_folder.get("non_python_files_count", 0)
                
                # Size information
                total_size_mb = dag_folder.get("total_size_mb", 0)
                python_size_mb = dag_folder.get("python_size_mb", 0)
                non_python_size_mb = dag_folder.get("non_python_size_mb", 0)
                
                report_lines.append(f"  Total Files: {total_files} ({total_size_mb} MB)")
                report_lines.append(f"  Python Files: {python_files} ({python_size_mb} MB)")
                report_lines.append(f"  Non-Python Files: {non_python_files} ({non_python_size_mb} MB)")
                
                # Non-Python files details
                if non_python_files > 0:
                    report_lines.append(f"  ⚠️ Non-Python files detected:")
                    
                    # Show file extensions
                    file_extensions = dag_folder.get("file_extensions", {})
                    for ext, count in sorted(file_extensions.items(), key=lambda x: x[1], reverse=True):
                        report_lines.append(f"    {ext}: {count} files")
                    
                    # Show some example files
                    example_files = dag_folder.get("non_python_files", [])
                    if example_files:
                        report_lines.append(f"  Examples:")
                        for file in example_files[:5]:  # Show first 5
                            report_lines.append(f"    - {file}")
                        if len(example_files) > 5:
                            report_lines.append(f"    ... and {len(example_files) - 5} more")
                
                # Show largest files if folder is substantial
                if total_size_mb > 1:  # Only show if folder is > 1MB
                    largest_files = dag_folder.get("largest_files", [])
                    if largest_files:
                        report_lines.append(f"  Largest Files:")
                        for file_info in largest_files[:5]:  # Show top 5
                            size_mb = file_info["size_mb"]
                            if size_mb > 0.01:  # Only show files > 10KB
                                report_lines.append(f"    - {file_info['path']}: {size_mb} MB")
                        if len([f for f in largest_files if f["size_mb"] > 0.01]) > 5:
                            report_lines.append(f"    ... and more")
            
            # Collection Errors
            if system_metrics.get("collection_errors"):
                report_lines.append(f"\nCollection Issues:")
                for error in system_metrics["collection_errors"][:3]:  # Show first 3 errors
                    report_lines.append(f"  - {error}")
        else:
            report_lines.append("System metrics not available")
            
    except Exception as e:
        report_lines.append(f"Error displaying system metrics: {str(e)}")
    
    report_lines.append("")

    # Recommendations Section
    report_lines.append("💡 RECOMMENDATIONS")
    report_lines.append("-" * 50)

    recommendations = []

    # Prioritize configuration issues
    config_issues = config_analysis.get("issues", [])
    critical_config_issues = [issue for issue in config_issues if "🚨 CRITICAL" in issue]
    warning_config_issues = [issue for issue in config_issues if "⚠️ WARNING" in issue and "🚨 CRITICAL" not in issue]
    
    if critical_config_issues:
        recommendations.append(
            f"🚨 URGENT: Fix {len(critical_config_issues)} critical configuration conflicts that may cause DAG processing failures"
        )
    
    if warning_config_issues:
        recommendations.append(
            f"Review {len(warning_config_issues)} configuration warnings to optimize performance"
        )

    if config_analysis["config_changed"]:
        recommendations.append(
            "Review recent Airflow configuration changes for potential impact"
        )

    if dag_analysis["max_size_mb"] > CRITICAL_DAG_SIZE_MB:
        recommendations.append(
            f"Optimize large DAGs - largest is {dag_analysis['max_size_mb']:.1f}MB"
        )

    if performance_analysis["avg_success_rate"] < MIN_SUCCESS_RATE:
        recommendations.append(
            f"Investigate DAG failures - success rate is {performance_analysis['avg_success_rate']:.1f}%"
        )

    if failure_analysis["total_failing_tasks"] > 10:
        recommendations.append(
            f"Review {failure_analysis['total_failing_tasks']} failing task types"
        )

    if instability_analysis["unstable_dags"] > 0:
        recommendations.append(
            f"Investigate {instability_analysis['unstable_dags']} DAGs with serialization instability"
        )

    # Traffic pattern recommendations
    if traffic_analysis and isinstance(traffic_analysis, dict):
        traffic_insights = traffic_analysis.get("insights", [])
        if isinstance(traffic_insights, list):
            critical_traffic = [i for i in traffic_insights if "🚨" in i]
            warning_traffic = [i for i in traffic_insights if "⚠️" in i and "🚨" not in i]
            
            if critical_traffic:
                recommendations.append(
                    f"Address {len(critical_traffic)} critical traffic/concurrency issues"
                )
            
            if warning_traffic:
                recommendations.append(
                    f"Review {len(warning_traffic)} traffic pattern warnings for optimization opportunities"
                )
        
        # Check for specific patterns
        concurrent = traffic_analysis.get("concurrent_patterns", {})
        if isinstance(concurrent, dict) and concurrent.get("max_concurrent_dags", 0) > 50:
            recommendations.append(
                "High concurrent DAG execution detected - verify scheduler and worker capacity"
            )
        
        task_conc = traffic_analysis.get("task_concurrency", {})
        if isinstance(task_conc, dict) and task_conc.get("max_concurrent_tasks", 0) > 100:
            recommendations.append(
                "High task concurrency detected - ensure adequate parallelism configuration"
            )

    # DAG folder recommendations
    if system_metrics and "dag_folder_info" in system_metrics:
        dag_folder = system_metrics["dag_folder_info"]
        
        if not dag_folder.get("has_airflowignore"):
            recommendations.append(
                "Create .airflowignore file to exclude non-DAG files from parsing"
            )
        
        non_python_count = dag_folder.get("non_python_files_count", 0)
        if non_python_count > 0:
            recommendations.append(
                f"Review {non_python_count} non-Python files in DAGs folder - consider moving or ignoring them"
            )
        
        # Check for large DAG folder
        total_size_mb = dag_folder.get("total_size_mb", 0)
        if total_size_mb > 100:  # > 100MB
            recommendations.append(
                f"DAGs folder is large ({total_size_mb} MB) - consider cleanup or archiving old files"
            )
        
        # Check for large non-Python files
        non_python_size_mb = dag_folder.get("non_python_size_mb", 0)
        if non_python_size_mb > 10:  # > 10MB of non-Python files
            recommendations.append(
                f"Non-Python files consume {non_python_size_mb} MB - consider moving large data files elsewhere"
            )

    if not recommendations:
        recommendations.append("System appears healthy - continue monitoring")

    for i, rec in enumerate(recommendations, 1):
        report_lines.append(f"{i}. {rec}")

    report_lines.append("")
    report_lines.append("=" * 100)

    report = "\n".join(report_lines)
    print("\n" + report)

    # Save to S3
    s3_result = save_report_to_s3(report, context, "dag_diagnostics")

    return {
        "report": report,
        "s3_location": s3_result,
        "summary": {
            "total_dags": dag_analysis["total_dags"],
            "suspicious_dags": len(dag_analysis["suspicious_dags"]),
            "performance_issues": len(performance_analysis["performance_issues"]),
            "failing_tasks": failure_analysis["total_failing_tasks"],
            "unstable_dags": instability_analysis["unstable_dags"],
            "config_changed": config_analysis["config_changed"],
            "traffic_insights": len(traffic_analysis.get("insights", [])) if traffic_analysis else 0,
            "peak_concurrent_dags": traffic_analysis.get("concurrent_patterns", {}).get("max_concurrent_dags", 0) if traffic_analysis else 0,
            "peak_concurrent_tasks": traffic_analysis.get("task_concurrency", {}).get("max_concurrent_tasks", 0) if traffic_analysis else 0,
            "recommendations_count": len(recommendations),
        },
    }


def save_report_to_s3(
    report_content: str, context: Dict[str, Any], report_type: str
) -> Dict[str, Any]:
    """Save diagnostics report to S3"""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        # Get S3 bucket from DAG params or fallback to Variable/default
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "s3_bucket" in dag_run.conf:
            s3_bucket = dag_run.conf["s3_bucket"]
        else:
            # Fallback to DAG params or Variable
            params = context.get("params", {})
            s3_bucket = params.get("s3_bucket", S3_BUCKET)

        # Get environment name from params or fallback
        if dag_run and dag_run.conf and "env_name" in dag_run.conf:
            env_name = dag_run.conf["env_name"]
        else:
            env_name = params.get("env_name", ENV_NAME)

        s3_hook = S3Hook(aws_conn_id="aws_default")

        execution_date = context["ds"]
        timestamp = context["ts"].replace(":", "-").replace("+", "_")

        s3_key = f"mwaa_diagnostics/{env_name}/{execution_date}/{report_type}_{timestamp}.txt"

        s3_hook.load_string(
            string_data=report_content, key=s3_key, bucket_name=s3_bucket, replace=True
        )

        s3_location = f"s3://{s3_bucket}/{s3_key}"
        print(f"\n📄 Report saved to S3: {s3_location}")

        return {
            "status": "success",
            "location": s3_location,
            "bucket": s3_bucket,
            "key": s3_key,
        }

    except Exception as e:
        print(f"❌ Failed to save report to S3: {e}")
        return {"status": "failed", "error": str(e)}


@task()
def cleanup_postgres_connection(**context):
    """Clean up the diagnostic connection"""
    from airflow.models import Connection
    from airflow import settings

    conn_info = context["task_instance"].xcom_pull(
        task_ids="create_postgres_connection"
    )
    conn_id = conn_info["conn_id"]

    session = settings.Session()
    try:
        conn_to_delete = (
            session.query(Connection).filter(Connection.conn_id == conn_id).first()
        )
        if conn_to_delete:
            session.delete(conn_to_delete)
            session.commit()
            print(f"Deleted diagnostic connection: {conn_id}")
        return {"status": "deleted"}
    except Exception as e:
        session.rollback()
        print(f"Failed to delete connection: {e}")
        return {"status": "failed"}
    finally:
        session.close()


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description="DAG diagnostics system with configuration tracking and extensible analysis",
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["diagnostics", "mwaa", "monitoring", "configuration"],
    params={"analysis_days": 7, "env_name": ENV_NAME, "s3_bucket": S3_BUCKET},
    doc_md="""
    ## DAG Diagnostics
    
    This DAG provides comprehensive diagnostics for MWAA environments including:
    - Airflow configuration change detection and conflict analysis
    - DAG serialization analysis
    - Performance monitoring
    - Failure pattern analysis
    - Serialization instability detection
    - Traffic pattern and concurrency analysis
    - System metrics collection
    
    ### Parameters:
    - **analysis_days**: Number of days to analyze (default: 7)
    - **env_name**: Environment name for reports (default: from Variable)
    - **s3_bucket**: S3 bucket for saving reports (default: from Variable)
    
    ### Usage:
    Trigger with custom parameters:
    ```json
    {
        "analysis_days": 14,
        "env_name": "MyEnvironment",
        "s3_bucket": "my-diagnostics-bucket"
    }
    ```
    """,
)
def dag_diagnostics_dag():
    """DAG diagnostics workflow"""

    # Setup
    conn_setup = create_postgres_connection()

    # Configuration analysis
    config_capture = capture_airflow_configuration()

    # Core analyses (can run in parallel)
    dag_analysis = analyze_serialized_dags()
    performance_analysis = analyze_dag_performance()
    failure_analysis = analyze_task_failures()
    instability_analysis = analyze_serialization_instability()
    traffic_analysis = analyze_traffic_patterns()
    system_metrics = collect_system_metrics()

    # Generate diagnostics report
    report = generate_complete_report()

    # Cleanup
    cleanup = cleanup_postgres_connection()

    # Set dependencies
    conn_setup >> [
        config_capture,
        dag_analysis,
        performance_analysis,
        failure_analysis,
        instability_analysis,
        traffic_analysis,
        system_metrics,
    ]
    (
        [
            config_capture,
            dag_analysis,
            performance_analysis,
            failure_analysis,
            instability_analysis,
            traffic_analysis,
            system_metrics,
        ]
        >> report
        >> cleanup
    )


# Instantiate the DAG
dag_diagnostics_dag_instance = dag_diagnostics_dag()
