import logging
import os

import snowflake.connector

logger = logging.getLogger(__name__)

ENV = os.getenv("ENVIRONMENT", "DEV").upper()
DATABASE = f"fl_data_{ENV.lower()}"


def get_snowflake_connection():
    """Return a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "DATA_ENGINEER"),
        database=DATABASE,
    )


def execute_snowflake_query(query, params=None):
    """Execute a query without returning results."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        logger.info("Executed query: %s...", query[:100])
    finally:
        conn.close()


def execute_and_fetch_snowflake_query(query, params=None):
    """Execute a query and return all rows."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        logger.info("Fetched %d rows", len(results))
        return results
    finally:
        conn.close()


def copy_into_snowflake(schema, table, s3_path, file_format, bucket=None):
    """Run COPY INTO from S3 into a Snowflake table."""
    bucket = bucket or os.getenv("S3_BUCKET")
    query = f"""
        COPY INTO {DATABASE}.{schema}.{table}
        FROM @{DATABASE}.{schema}.s3_stage/{s3_path}
        FILE_FORMAT = (FORMAT_NAME = '{DATABASE}.{schema}.{file_format}')
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """
    execute_snowflake_query(query)
    logger.info("COPY INTO %s.%s.%s completed", DATABASE, schema, table)


def table_exists(schema, table):
    """Check if a table exists in Snowflake."""
    query = f"""
        SELECT COUNT(*) FROM {DATABASE}.information_schema.tables
        WHERE table_schema = '{schema.upper()}'
        AND table_name = '{table.upper()}'
    """
    result = execute_and_fetch_snowflake_query(query)
    return result[0][0] > 0
