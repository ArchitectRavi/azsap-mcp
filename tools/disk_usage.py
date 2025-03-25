"""
SAP HANA Disk Usage Tool

This module provides tools for retrieving disk usage information from SAP HANA,
including volume sizes, data files, and log files.
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

async def get_disk_usage(use_system_db: bool = True) -> Dict[str, Any]:
    """Get disk usage information for the SAP HANA system, including volume sizes,
    data files, and log files.
    
    This uses the M_VOLUME_FILES, M_DISKS, and M_DATA_VOLUMES system views.
    
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
            
            # First, check what columns are actually available in these views
            try:
                # Check M_VOLUME_FILES columns
                volume_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_VOLUME_FILES'
                """
                volume_columns = execute_query(conn, volume_columns_query)
                volume_column_names = [col['COLUMN_NAME'] for col in volume_columns]
                logger.info(f"Available columns in M_VOLUME_FILES: {volume_column_names}")
                
                # Check M_DISKS columns
                disks_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_DISKS'
                """
                disks_columns = execute_query(conn, disks_columns_query)
                disks_column_names = [col['COLUMN_NAME'] for col in disks_columns]
                logger.info(f"Available columns in M_DISKS: {disks_column_names}")
                
                # Check M_DATA_VOLUMES columns
                data_volumes_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_DATA_VOLUMES'
                """
                data_volumes_columns = execute_query(conn, data_volumes_columns_query)
                data_volumes_column_names = [col['COLUMN_NAME'] for col in data_volumes_columns]
                logger.info(f"Available columns in M_DATA_VOLUMES: {data_volumes_column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                volume_column_names = ["HOST", "USAGE_TYPE", "TOTAL_SIZE", "USED_SIZE", "PATH"]
                disks_column_names = ["HOST", "DISK_ID", "USAGE_TYPE", "DISK_SIZE", "USED_SIZE", "DISK_TYPE"]
                data_volumes_column_names = ["HOST", "VOLUME_ID", "FILE_NAME", "TOTAL_SIZE", "USED_SIZE"]
            
            # Build dynamic queries based on available columns
            # Volume information query
            volume_select_columns = []
            for col in ["HOST", "USAGE_TYPE", "TOTAL_SIZE", "USED_SIZE", "PATH"]:
                if col in volume_column_names:
                    if col in ["TOTAL_SIZE", "USED_SIZE"]:
                        volume_select_columns.append(f"ROUND({col}/1024/1024/1024, 2) as {col.replace('SIZE', '')}_SIZE_GB")
                    else:
                        volume_select_columns.append(col)
            
            if not volume_select_columns:
                # If no columns match, try with a basic query
                volume_query = """
                SELECT HOST 
                FROM SYS.M_VOLUME_FILES
                """
            else:
                volume_query = f"""
                SELECT {', '.join(volume_select_columns)}
                FROM SYS.M_VOLUME_FILES
                ORDER BY HOST
                """
            
            try:
                volume_info = execute_query(conn, volume_query)
                logger.info(f"Successfully retrieved volume information: {len(volume_info)} rows")
            except Exception as e:
                logger.error(f"Error querying volume information: {str(e)}")
                # Try a simpler query as fallback
                try:
                    volume_info = execute_query(conn, "SELECT HOST FROM SYS.M_VOLUME_FILES")
                    logger.info(f"Retrieved basic volume information: {len(volume_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback volume query: {str(e2)}")
                    volume_info = [{"error": f"Failed to retrieve volume information: {str(e)}"}]
            
            # Disk information query
            disk_select_columns = []
            for col in ["HOST", "DISK_ID", "USAGE_TYPE", "DISK_SIZE", "USED_SIZE", "DISK_TYPE"]:
                if col in disks_column_names:
                    if col in ["DISK_SIZE", "USED_SIZE"]:
                        disk_select_columns.append(f"ROUND({col}/1024/1024/1024, 2) as {col.replace('SIZE', '')}_GB")
                    else:
                        disk_select_columns.append(col)
            
            if not disk_select_columns:
                # If no columns match, try with a basic query
                disk_query = """
                SELECT HOST 
                FROM SYS.M_DISKS
                """
            else:
                disk_query = f"""
                SELECT {', '.join(disk_select_columns)}
                FROM SYS.M_DISKS
                ORDER BY HOST
                """
            
            try:
                disk_info = execute_query(conn, disk_query)
                logger.info(f"Successfully retrieved disk information: {len(disk_info)} rows")
            except Exception as e:
                logger.error(f"Error querying disk information: {str(e)}")
                # Try a simpler query as fallback
                try:
                    disk_info = execute_query(conn, "SELECT HOST FROM SYS.M_DISKS")
                    logger.info(f"Retrieved basic disk information: {len(disk_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback disk query: {str(e2)}")
                    disk_info = [{"error": f"Failed to retrieve disk information: {str(e)}"}]
            
            # Data volume information query
            data_volume_select_columns = []
            for col in ["HOST", "VOLUME_ID", "FILE_NAME", "TOTAL_SIZE", "USED_SIZE"]:
                if col in data_volumes_column_names:
                    if col in ["TOTAL_SIZE", "USED_SIZE"]:
                        data_volume_select_columns.append(f"ROUND({col}/1024/1024/1024, 2) as {col.replace('SIZE', '')}_GB")
                    else:
                        data_volume_select_columns.append(col)
            
            if not data_volume_select_columns:
                # If no columns match, try with a basic query
                data_volume_query = """
                SELECT HOST 
                FROM SYS.M_DATA_VOLUMES
                """
            else:
                data_volume_query = f"""
                SELECT {', '.join(data_volume_select_columns)}
                FROM SYS.M_DATA_VOLUMES
                ORDER BY HOST
                """
            
            try:
                data_volume_info = execute_query(conn, data_volume_query)
                logger.info(f"Successfully retrieved data volume information: {len(data_volume_info)} rows")
            except Exception as e:
                logger.error(f"Error querying data volume information: {str(e)}")
                # Try a simpler query as fallback
                try:
                    data_volume_info = execute_query(conn, "SELECT HOST FROM SYS.M_DATA_VOLUMES")
                    logger.info(f"Retrieved basic data volume information: {len(data_volume_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback data volume query: {str(e2)}")
                    data_volume_info = [{"error": f"Failed to retrieve data volume information: {str(e)}"}]
            
            # Compile the results
            result = {
                "volume_files": volume_info,
                "disks": disk_info,
                "data_volumes": data_volume_info
            }
            
            return {
                "content": format_result_content(result),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting disk usage: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting disk usage: {str(e)}"}],
            "isError": True
        }
