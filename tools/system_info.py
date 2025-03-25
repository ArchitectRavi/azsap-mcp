"""
SAP HANA System Information Tools

This module provides tools for retrieving various system information from SAP HANA,
including backup catalog, system configuration, and other administrative information.
"""

import logging
import json
import decimal
from typing import Any, Dict, List

from hana_connection import hana_connection, execute_query

# Configure logging
logger = logging.getLogger(__name__)

# Custom JSON encoder for handling Decimal objects (copied from utils to avoid circular imports)
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

# Format utilities (copied from utils to avoid circular imports)
def format_result_content(result: Any) -> List[Dict[str, Any]]:
    """Format result into MCP content format."""
    if isinstance(result, str):
        return [{"type": "text", "text": result}]
    elif isinstance(result, dict):
        if "error" in result:
            return [{"type": "text", "text": f"Error: {result['error']}"}]
        else:
            # Format dictionary as markdown table
            return [{"type": "text", "text": json.dumps(result, indent=2, cls=DecimalEncoder)}]
    elif isinstance(result, list):
        if not result:
            return [{"type": "text", "text": "No results found."}]
        
        if isinstance(result[0], dict):
            # Format list of dictionaries as markdown table
            table_str = "| " + " | ".join(result[0].keys()) + " |\n"
            table_str += "| " + " | ".join(["---"] * len(result[0].keys())) + " |\n"
            
            for row in result:
                # Convert any Decimal values to float
                formatted_values = []
                for val in row.values():
                    if isinstance(val, decimal.Decimal):
                        formatted_values.append(str(float(val)))
                    else:
                        formatted_values.append(str(val))
                
                table_str += "| " + " | ".join(formatted_values) + " |\n"
            
            return [{"type": "text", "text": table_str}]
        else:
            # Format list as bullet points
            bullet_list = "\n".join([f"* {item}" for item in result])
            return [{"type": "text", "text": bullet_list}]
    else:
        # Default formatting for other types
        return [{"type": "text", "text": str(result)}]

async def get_backup_catalog(use_system_db: bool = True) -> Dict[str, Any]:
    """Get the backup catalog information from SAP HANA.
    
    This uses the M_BACKUP_CATALOG system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        with hana_connection(use_system_db) as conn:
            if conn is None:
                return {
                    "content": [{"type": "text", "text": "Error: Database connection failed. Check credentials."}],
                    "isError": True
                }
            
            # First, check what columns are actually available in the view
            try:
                # Check M_BACKUP_CATALOG columns
                catalog_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_BACKUP_CATALOG'
                """
                catalog_columns = execute_query(conn, catalog_columns_query)
                catalog_column_names = [col['COLUMN_NAME'] for col in catalog_columns]
                logger.info(f"Available columns in M_BACKUP_CATALOG: {catalog_column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                catalog_column_names = ["ENTRY_ID", "BACKUP_ID", "SID", "DATABASE_NAME", "HOST", 
                                       "START_TIME", "END_TIME", "STATE", "COMMENT", "BACKUP_SIZE"]
            
            # Check if ENTRY_ID column exists (it might be ENTRY_ID or ENTERY_ID due to typo in query)
            has_entry_id = "ENTRY_ID" in catalog_column_names
            has_entery_id = "ENTERY_ID" in catalog_column_names
            
            # Build the query based on available columns
            if has_entry_id:
                order_by_column = "ENTRY_ID"
            elif has_entery_id:
                order_by_column = "ENTERY_ID"
            else:
                # If neither column exists, try to use START_TIME or fall back to no ordering
                order_by_column = "START_TIME" if "START_TIME" in catalog_column_names else None
            
            # Build the query
            if order_by_column:
                backup_catalog_query = f"""
                SELECT * FROM SYS.M_BACKUP_CATALOG ORDER BY {order_by_column} DESC
                """
            else:
                backup_catalog_query = """
                SELECT * FROM SYS.M_BACKUP_CATALOG
                """
            
            try:
                backup_catalog = execute_query(conn, backup_catalog_query)
                logger.info(f"Successfully retrieved backup catalog: {len(backup_catalog)} rows")
            except Exception as e:
                logger.error(f"Error querying backup catalog: {str(e)}")
                # Try a simpler query as fallback
                try:
                    backup_catalog = execute_query(conn, "SELECT * FROM SYS.M_BACKUP_CATALOG")
                    logger.info(f"Retrieved backup catalog with basic query: {len(backup_catalog)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback backup catalog query: {str(e2)}")
                    backup_catalog = [{"error": f"Failed to retrieve backup catalog: {str(e)}"}]
            
            return {
                "content": format_result_content(backup_catalog),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting backup catalog: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting backup catalog: {str(e)}"}],
            "isError": True
        }

async def get_db_info(use_system_db: bool = True) -> Dict[str, Any]:
    """Get database information from SAP HANA.
    
    This uses the M_DATABASE system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        with hana_connection(use_system_db) as conn:
            if conn is None:
                return {
                    "content": [{"type": "text", "text": "Error: Database connection failed. Check credentials."}],
                    "isError": True
                }
            
            # First, check what columns are actually available in the view
            try:
                # Check M_DATABASE columns
                db_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_DATABASE'
                """
                db_columns = execute_query(conn, db_columns_query)
                db_column_names = [col['COLUMN_NAME'] for col in db_columns]
                logger.info(f"Available columns in M_DATABASE: {db_column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                db_column_names = ["DATABASE_NAME", "DESCRIPTION", "ACTIVE_STATUS", "HOST", 
                                  "START_TIME", "VERSION", "USAGE", "SYSTEM_ID"]
            
            # Build the query
            db_info_query = """
            SELECT * FROM SYS.M_DATABASE
            """
            
            try:
                db_info = execute_query(conn, db_info_query)
                logger.info(f"Successfully retrieved database information: {len(db_info)} rows")
            except Exception as e:
                logger.error(f"Error querying database information: {str(e)}")
                # Try a more specific query as fallback
                try:
                    # Try to select only columns that are likely to exist
                    fallback_columns = []
                    for col in ["DATABASE_NAME", "DESCRIPTION", "ACTIVE_STATUS", "HOST"]:
                        if col in db_column_names:
                            fallback_columns.append(col)
                    
                    if fallback_columns:
                        fallback_query = f"""
                        SELECT {', '.join(fallback_columns)} FROM SYS.M_DATABASE
                        """
                        db_info = execute_query(conn, fallback_query)
                        logger.info(f"Retrieved database information with fallback query: {len(db_info)} rows")
                    else:
                        # Last resort - try with just one column
                        db_info = execute_query(conn, "SELECT DATABASE_NAME FROM SYS.M_DATABASE")
                        logger.info(f"Retrieved basic database information: {len(db_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback database query: {str(e2)}")
                    db_info = [{"error": f"Failed to retrieve database information: {str(e)}"}]
            
            return {
                "content": format_result_content(db_info),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting database information: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting database information: {str(e)}"}],
            "isError": True
        }

async def get_failed_backups(use_system_db: bool = True) -> Dict[str, Any]:
    """Get information about failed or canceled backups from SAP HANA.
    
    This uses the M_BACKUP_CATALOG and M_BACKUP_CATALOG_FILES system views.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        with hana_connection(use_system_db) as conn:
            if conn is None:
                return {
                    "content": [{"type": "text", "text": "Error: Database connection failed. Check credentials."}],
                    "isError": True
                }
            
            # First, check what columns are actually available in the views
            try:
                # Check M_BACKUP_CATALOG columns
                catalog_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_BACKUP_CATALOG'
                """
                catalog_columns = execute_query(conn, catalog_columns_query)
                catalog_column_names = [col['COLUMN_NAME'] for col in catalog_columns]
                logger.info(f"Available columns in M_BACKUP_CATALOG: {catalog_column_names}")
                
                # Check M_BACKUP_CATALOG_FILES columns
                files_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_BACKUP_CATALOG_FILES'
                """
                files_columns = execute_query(conn, files_columns_query)
                files_column_names = [col['COLUMN_NAME'] for col in files_columns]
                logger.info(f"Available columns in M_BACKUP_CATALOG_FILES: {files_column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                catalog_column_names = ["ENTRY_ID", "BACKUP_ID", "ENTRY_TYPE_NAME", "SYS_START_TIME", 
                                      "SYS_END_TIME", "STATE_NAME", "MESSAGE"]
                files_column_names = ["ENTRY_ID", "SERVICE_TYPE_NAME", "SOURCE_ID", "SOURCE_TYPE_NAME", 
                                     "DESTINATION_PATH"]
            
            # Check if required columns exist for the join
            has_entry_id_catalog = "ENTRY_ID" in catalog_column_names
            has_entry_id_files = "ENTRY_ID" in files_column_names
            
            # Check if we have the state column for filtering
            has_state_name = "STATE_NAME" in catalog_column_names
            
            # Build the query based on available columns
            if has_entry_id_catalog and has_entry_id_files and has_state_name:
                # We can perform the full query with join and filtering
                query = """
                SELECT C.BACKUP_ID, 
                       C.ENTRY_TYPE_NAME AS BACKUP_TYPE, 
                       C.SYS_START_TIME, 
                       C.SYS_END_TIME AS SYS_STOP_TIME, 
                       C.STATE_NAME AS STATE, 
                       C.MESSAGE AS ADDITIONAL_INFORMATION, 
                       F.SERVICE_TYPE_NAME AS SERVICE, 
                       F.SOURCE_ID, 
                       F.SOURCE_TYPE_NAME AS SOURCE_TYPE, 
                       F.DESTINATION_PATH 
                FROM SYS.M_BACKUP_CATALOG C, SYS.M_BACKUP_CATALOG_FILES F 
                WHERE C.ENTRY_ID = F.ENTRY_ID 
                  AND (C.STATE_NAME = 'failed' OR C.STATE_NAME = 'canceled') 
                ORDER BY C.BACKUP_ID DESC
                """
            elif has_state_name:
                # We can at least filter for failed backups from the catalog
                query = """
                SELECT BACKUP_ID, 
                       ENTRY_TYPE_NAME AS BACKUP_TYPE, 
                       SYS_START_TIME, 
                       SYS_END_TIME AS SYS_STOP_TIME, 
                       STATE_NAME AS STATE, 
                       MESSAGE AS ADDITIONAL_INFORMATION
                FROM SYS.M_BACKUP_CATALOG 
                WHERE STATE_NAME = 'failed' OR STATE_NAME = 'canceled'
                ORDER BY BACKUP_ID DESC
                """
            else:
                # Fallback to just getting all backups from catalog
                query = """
                SELECT * FROM SYS.M_BACKUP_CATALOG
                ORDER BY BACKUP_ID DESC
                """
            
            try:
                failed_backups = execute_query(conn, query)
                logger.info(f"Successfully retrieved failed backups: {len(failed_backups)} rows")
            except Exception as e:
                logger.error(f"Error querying failed backups: {str(e)}")
                # Try a simpler query as fallback
                try:
                    # Try to get just the basic information without joins
                    fallback_query = """
                    SELECT * FROM SYS.M_BACKUP_CATALOG
                    """
                    failed_backups = execute_query(conn, fallback_query)
                    logger.info(f"Retrieved backup catalog with fallback query: {len(failed_backups)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback query: {str(e2)}")
                    failed_backups = [{"error": f"Failed to retrieve failed backups: {str(e)}"}]
            
            return {
                "content": format_result_content(failed_backups),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting failed backups: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting failed backups: {str(e)}"}],
            "isError": True
        }

async def get_tablesize_on_disk(use_system_db: bool = True) -> Dict[str, Any]:
    """Get table sizes on disk from SAP HANA.
    
    This uses the PUBLIC.M_TABLE_PERSISTENCE_STATISTICS system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        with hana_connection(use_system_db) as conn:
            if conn is None:
                return {
                    "content": [{"type": "text", "text": "Error: Database connection failed. Check credentials."}],
                    "isError": True
                }
            
            # First, check what columns are actually available in the view
            try:
                # Check M_TABLE_PERSISTENCE_STATISTICS columns
                columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = 'M_TABLE_PERSISTENCE_STATISTICS'
                """
                columns = execute_query(conn, columns_query)
                column_names = [col['COLUMN_NAME'] for col in columns]
                logger.info(f"Available columns in PUBLIC.M_TABLE_PERSISTENCE_STATISTICS: {column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                column_names = ["SCHEMA_NAME", "TABLE_NAME", "DISK_SIZE"]
            
            # Check if required columns exist
            has_schema_name = "SCHEMA_NAME" in column_names
            has_table_name = "TABLE_NAME" in column_names
            has_disk_size = "DISK_SIZE" in column_names
            
            # Build the query based on available columns
            if has_schema_name and has_table_name and has_disk_size:
                # We can perform the full query with only the essential columns
                # Use CAST to ensure proper numeric formatting and add calculated columns
                query = """
                SELECT 
                    SCHEMA_NAME, 
                    TABLE_NAME, 
                    CAST(DISK_SIZE AS DECIMAL(38,2)) AS DISK_SIZE,
                    ROUND(DISK_SIZE / 1024 / 1024 / 1024, 2) AS DISK_SIZE_GB,
                    ROUND(DISK_SIZE / 1024 / 1024, 2) AS DISK_SIZE_MB
                FROM PUBLIC.M_TABLE_PERSISTENCE_STATISTICS 
                WHERE DISK_SIZE > 0
                ORDER BY DISK_SIZE DESC
                """
            elif has_schema_name and has_table_name:
                # We can at least get schema and table names
                query = """
                SELECT SCHEMA_NAME, TABLE_NAME 
                FROM PUBLIC.M_TABLE_PERSISTENCE_STATISTICS
                """
            else:
                # Fallback to just getting all columns
                query = """
                SELECT * FROM PUBLIC.M_TABLE_PERSISTENCE_STATISTICS
                """
            
            try:
                table_sizes = execute_query(conn, query)
                logger.info(f"Successfully retrieved table sizes: {len(table_sizes)} rows")
            except Exception as e:
                logger.error(f"Error querying table sizes: {str(e)}")
                # Try a simpler query as fallback
                try:
                    # Try to get just the basic information
                    fallback_query = """
                    SELECT 
                        SCHEMA_NAME, 
                        TABLE_NAME, 
                        CAST(DISK_SIZE AS DECIMAL(38,2)) AS DISK_SIZE
                    FROM PUBLIC.M_TABLE_PERSISTENCE_STATISTICS
                    WHERE DISK_SIZE > 0
                    ORDER BY DISK_SIZE DESC
                    """
                    table_sizes = execute_query(conn, fallback_query)
                    logger.info(f"Retrieved table sizes with fallback query: {len(table_sizes)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback query: {str(e2)}")
                    table_sizes = [{"error": f"Failed to retrieve table sizes: {str(e)}"}]
            
            return {
                "content": format_result_content(table_sizes),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting table sizes: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting table sizes: {str(e)}"}],
            "isError": True
        }

async def get_table_used_memory(use_system_db: bool = True) -> Dict[str, Any]:
    """Get memory usage by table type (column vs row) from SAP HANA.
    
    This uses the SYS.M_TABLES system view.
    
    Args:
        use_system_db: Whether to use the system database (recommended for administration)
    """
    try:
        with hana_connection(use_system_db) as conn:
            if conn is None:
                return {
                    "content": [{"type": "text", "text": "Error: Database connection failed. Check credentials."}],
                    "isError": True
                }
            
            # First, check what columns are actually available in the view
            try:
                # Check M_TABLES columns
                columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_TABLES'
                """
                columns = execute_query(conn, columns_query)
                column_names = [col['COLUMN_NAME'] for col in columns]
                logger.info(f"Available columns in SYS.M_TABLES: {column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                column_names = ["TABLE_NAME", "SCHEMA_NAME", "TABLE_TYPE", "TABLE_SIZE"]
            
            # Check if required columns exist
            has_table_type = "TABLE_TYPE" in column_names
            has_table_size = "TABLE_SIZE" in column_names
            
            # Build the query based on available columns
            if has_table_type and has_table_size:
                # We can perform the full query
                query = """
                SELECT 
                    C AS "Used Storage for Column Tables in [MB]", 
                    R AS "Used Storage for Row Tables in [MB]" 
                FROM 
                    (SELECT ROUND(SUM(TABLE_SIZE)/1024/1024) AS "C" FROM SYS.M_TABLES WHERE TABLE_TYPE = 'COLUMN'), 
                    (SELECT ROUND(SUM(TABLE_SIZE)/1024/1024) AS "R" FROM SYS.M_TABLES WHERE TABLE_TYPE = 'ROW')
                """
            else:
                # Fallback to a more detailed query that doesn't rely on the specific calculation
                query = """
                SELECT TABLE_TYPE, COUNT(*) AS TABLE_COUNT, SUM(TABLE_SIZE) AS TOTAL_SIZE 
                FROM SYS.M_TABLES 
                GROUP BY TABLE_TYPE
                """
            
            try:
                memory_usage = execute_query(conn, query)
                logger.info(f"Successfully retrieved table memory usage: {len(memory_usage)} rows")
                
                # If we used the fallback query, convert the results to a more readable format
                if not (has_table_type and has_table_size) and memory_usage:
                    # Process the results to add MB values
                    for row in memory_usage:
                        if 'TOTAL_SIZE' in row and row['TOTAL_SIZE'] is not None:
                            try:
                                # Add size in MB for readability
                                row['TOTAL_SIZE_MB'] = round(float(row['TOTAL_SIZE']) / (1024 * 1024), 2)
                            except (ValueError, TypeError):
                                # In case of conversion error, just skip adding the MB column
                                pass
            except Exception as e:
                logger.error(f"Error querying table memory usage: {str(e)}")
                # Try a simpler query as fallback
                try:
                    # Try to get just the basic information
                    fallback_query = """
                    SELECT TABLE_TYPE, COUNT(*) AS TABLE_COUNT 
                    FROM SYS.M_TABLES 
                    GROUP BY TABLE_TYPE
                    """
                    memory_usage = execute_query(conn, fallback_query)
                    logger.info(f"Retrieved table counts with fallback query: {len(memory_usage)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback query: {str(e2)}")
                    memory_usage = [{"error": f"Failed to retrieve table memory usage: {str(e)}"}]
            
            return {
                "content": format_result_content(memory_usage),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting table memory usage: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting table memory usage: {str(e)}"}],
            "isError": True
        }
