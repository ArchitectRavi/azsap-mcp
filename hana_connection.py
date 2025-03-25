"""
SAP HANA Connection Management for MCP Server

This module provides a clean abstraction for managing connections to SAP HANA databases,
supporting both System DB and Tenant DB connections.
"""

import os
import logging
from typing import Optional, Dict, Any, Union
from contextlib import contextmanager
import hdbcli.dbapi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
def get_env_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """Get environment variable with optional default."""
    return os.environ.get(name, default)

class HanaConnection:
    """HANA Connection Manager for System and Tenant databases."""
    
    # Connection pool to reuse connections
    _connections: Dict[str, Any] = {}
    
    @classmethod
    def get_connection_params(cls, use_system_db: bool = False) -> Dict[str, Any]:
        """Get connection parameters based on environment variables."""
        host = get_env_var("HANA_HOST")
        
        if use_system_db:
            port = get_env_var("HANA_SYSTEM_PORT", get_env_var("HANA_PORT"))
            user = get_env_var("HANA_SYSTEM_USER", get_env_var("HANA_USER"))
            password = get_env_var("HANA_SYSTEM_PASSWORD", get_env_var("HANA_PASSWORD"))
            schema = get_env_var("HANA_SYSTEM_SCHEMA", "SYSTEMDB")
        else:
            port = get_env_var("HANA_PORT")
            user = get_env_var("HANA_USER")
            password = get_env_var("HANA_PASSWORD")
            schema = get_env_var("HANA_SCHEMA")
        
        return {
            "address": host,
            "port": int(port) if port else None,
            "user": user,
            "password": password,
            "currentSchema": schema
        }
    
    @classmethod
    def get_connection(cls, use_system_db: bool = False) -> Any:
        """Get a connection to the HANA database."""
        conn_type = "system" if use_system_db else "tenant"
        
        if conn_type not in cls._connections or not cls._connections[conn_type]:
            params = cls.get_connection_params(use_system_db)
            
            if not params["address"] or not params["port"]:
                logger.error(f"Missing connection parameters for {conn_type} DB")
                return None
            
            try:
                connection = hdbcli.dbapi.connect(
                    address=params["address"],
                    port=params["port"],
                    user=params["user"],
                    password=params["password"],
                    currentSchema=params["currentSchema"]
                )
                cls._connections[conn_type] = connection
                logger.info(f"Established new connection to {conn_type} DB")
            except Exception as e:
                logger.error(f"Error connecting to {conn_type} DB: {str(e)}")
                return None
        
        return cls._connections[conn_type]
    
    @classmethod
    def close_connection(cls, conn_type: str = None):
        """Close a specific or all database connections."""
        if conn_type:
            if conn_type in cls._connections and cls._connections[conn_type]:
                cls._connections[conn_type].close()
                cls._connections[conn_type] = None
                logger.info(f"Closed connection to {conn_type} DB")
        else:
            for conn_type in cls._connections:
                if cls._connections[conn_type]:
                    cls._connections[conn_type].close()
                    cls._connections[conn_type] = None
            logger.info("Closed all DB connections")

@contextmanager
def hana_connection(use_system_db: bool = False):
    """Context manager for HANA connection."""
    conn = HanaConnection.get_connection(use_system_db)
    try:
        yield conn
    finally:
        # We don't close the connection here to allow connection pooling
        pass

def execute_query(
    conn, 
    query: str, 
    params: list = None, 
    max_rows: int = 1000
) -> Union[list, Dict[str, Any]]:
    """Execute a SQL query on the database connection."""
    if not conn:
        return {"error": "No database connection"}
    
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        result = []
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchmany(max_rows)
            
            for row in rows:
                result.append(dict(zip(columns, row)))
        
        cursor.close()
        return result
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error executing query: {error_msg}")
        return {"error": error_msg}

def get_table_schema(conn, table_name: str, schema_name: str = None) -> Dict[str, Any]:
    """Get schema information for a table."""
    if not conn:
        return {"error": "No database connection"}
    
    try:
        query = """
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE_NAME,
            LENGTH,
            SCALE,
            IS_NULLABLE,
            POSITION,
            COLUMN_ID,
            COMMENTS,
            DEFAULT_VALUE
        FROM 
            TABLE_COLUMNS 
        WHERE 
            TABLE_NAME = ?
        """
        
        params = [table_name]
        if schema_name:
            query += " AND SCHEMA_NAME = ?"
            params.append(schema_name)
            
        query += " ORDER BY POSITION"
        
        columns = execute_query(conn, query, params)
        
        if isinstance(columns, dict) and "error" in columns:
            return columns
        
        # Get primary key information
        pk_query = """
        SELECT 
            i.INDEX_NAME,
            INDEX_TYPE,
            ic.COLUMN_NAME
        FROM 
            INDEXES i
        JOIN
            INDEX_COLUMNS ic ON i.SCHEMA_NAME = ic.SCHEMA_NAME AND i.INDEX_NAME = ic.INDEX_NAME
        WHERE 
            i.TABLE_NAME = ? AND i.INDEX_TYPE = 'PRIMARY KEY'
        """
        
        pk_params = [table_name]
        if schema_name:
            pk_query += " AND i.SCHEMA_NAME = ?"
            pk_params.append(schema_name)
        
        primary_keys = execute_query(conn, pk_query, pk_params)
        pk_columns = [pk["COLUMN_NAME"] for pk in primary_keys] if not isinstance(primary_keys, dict) else []
        
        # Build the schema information
        schema_info = {
            "table_name": table_name,
            "schema_name": schema_name,
            "columns": columns,
            "primary_keys": pk_columns
        }
        
        return schema_info
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error getting table schema: {error_msg}")
        return {"error": error_msg}
