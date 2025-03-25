"""
SAP HANA System Overview Tool

This module provides tools for retrieving system overview information from SAP HANA,
including host information, service status, and memory usage.
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

async def get_system_overview(use_system_db: bool = True) -> Dict[str, Any]:
    """Get an overview of the SAP HANA system status, including host information,
    service status, and system resource usage.
    
    This uses the M_HOST_INFORMATION, M_SERVICES, and M_SERVICE_MEMORY system views.
    
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
                # Check M_HOST_INFORMATION columns
                host_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_HOST_INFORMATION'
                """
                host_columns = execute_query(conn, host_columns_query)
                host_column_names = [col['COLUMN_NAME'] for col in host_columns]
                logger.info(f"Available columns in M_HOST_INFORMATION: {host_column_names}")
                
                # Check M_SERVICES columns
                services_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_SERVICES'
                """
                services_columns = execute_query(conn, services_columns_query)
                services_column_names = [col['COLUMN_NAME'] for col in services_columns]
                logger.info(f"Available columns in M_SERVICES: {services_column_names}")
                
                # Check M_SERVICE_MEMORY columns
                memory_columns_query = """
                SELECT COLUMN_NAME 
                FROM SYS.TABLE_COLUMNS 
                WHERE SCHEMA_NAME = 'SYS' AND TABLE_NAME = 'M_SERVICE_MEMORY'
                """
                memory_columns = execute_query(conn, memory_columns_query)
                memory_column_names = [col['COLUMN_NAME'] for col in memory_columns]
                logger.info(f"Available columns in M_SERVICE_MEMORY: {memory_column_names}")
            except Exception as e:
                logger.error(f"Error checking available columns: {str(e)}")
                # Fall back to standard column names from SAP HANA documentation
                host_column_names = ["HOST", "HOST_ACTIVE", "HOST_STATUS", "PRODUCT_VERSION", 
                                    "INSTANCE_ID", "COORDINATOR_TYPE"]
                services_column_names = ["HOST", "PORT", "SERVICE_NAME", "SERVICE_STATUS", 
                                        "PROCESS_ID", "DETAIL"]
                memory_column_names = ["HOST", "PORT", "SERVICE_NAME", "PROCESS_ID", 
                                      "LOGICAL_MEMORY_SIZE", "PHYSICAL_MEMORY_SIZE"]
            
            # Build dynamic queries based on available columns
            # Host information query
            host_select_columns = []
            for col in ["HOST", "HOST_ACTIVE", "HOST_STATUS", "PRODUCT_VERSION", "INSTANCE_ID", "COORDINATOR_TYPE"]:
                if col in host_column_names:
                    host_select_columns.append(col)
            
            if not host_select_columns:
                # If no columns match, try with a basic query
                host_query = """
                SELECT HOST 
                FROM SYS.M_HOST_INFORMATION
                """
            else:
                host_query = f"""
                SELECT {', '.join(host_select_columns)}
                FROM SYS.M_HOST_INFORMATION
                """
            
            try:
                host_info = execute_query(conn, host_query)
                logger.info(f"Successfully retrieved host information: {len(host_info)} rows")
            except Exception as e:
                logger.error(f"Error querying host information: {str(e)}")
                # Try a simpler query as fallback
                try:
                    host_info = execute_query(conn, "SELECT HOST FROM SYS.M_HOST_INFORMATION")
                    logger.info(f"Retrieved basic host information: {len(host_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback host query: {str(e2)}")
                    host_info = [{"error": f"Failed to retrieve host information: {str(e)}"}]
            
            # Service status query
            service_select_columns = []
            for col in ["HOST", "PORT", "SERVICE_NAME", "SERVICE_STATUS", "PROCESS_ID", "DETAIL"]:
                if col in services_column_names:
                    service_select_columns.append(col)
            
            if not service_select_columns:
                # If no columns match, try with a basic query
                service_query = """
                SELECT SERVICE_NAME 
                FROM SYS.M_SERVICES
                """
            else:
                service_query = f"""
                SELECT {', '.join(service_select_columns)}
                FROM SYS.M_SERVICES
                """
            
            try:
                service_info = execute_query(conn, service_query)
                logger.info(f"Successfully retrieved service information: {len(service_info)} rows")
            except Exception as e:
                logger.error(f"Error querying service information: {str(e)}")
                # Try a simpler query as fallback
                try:
                    service_info = execute_query(conn, "SELECT SERVICE_NAME FROM SYS.M_SERVICES")
                    logger.info(f"Retrieved basic service information: {len(service_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback service query: {str(e2)}")
                    service_info = [{"error": f"Failed to retrieve service information: {str(e)}"}]
            
            # Memory usage query
            memory_select_columns = []
            for col in ["HOST", "PORT", "SERVICE_NAME", "LOGICAL_MEMORY_SIZE", "PHYSICAL_MEMORY_SIZE"]:
                if col in memory_column_names:
                    if col in ["LOGICAL_MEMORY_SIZE", "PHYSICAL_MEMORY_SIZE"]:
                        memory_select_columns.append(f"{col}/1024/1024/1024 as {col}_GB")
                    else:
                        memory_select_columns.append(col)
            
            if not memory_select_columns:
                # If no columns match, try with a basic query
                memory_query = """
                SELECT SERVICE_NAME 
                FROM SYS.M_SERVICE_MEMORY
                """
            else:
                memory_query = f"""
                SELECT {', '.join(memory_select_columns)}
                FROM SYS.M_SERVICE_MEMORY
                """
            
            try:
                memory_info = execute_query(conn, memory_query)
                logger.info(f"Successfully retrieved memory information: {len(memory_info)} rows")
            except Exception as e:
                logger.error(f"Error querying memory information: {str(e)}")
                # Try a simpler query as fallback
                try:
                    memory_info = execute_query(conn, "SELECT SERVICE_NAME FROM SYS.M_SERVICE_MEMORY")
                    logger.info(f"Retrieved basic memory information: {len(memory_info)} rows")
                except Exception as e2:
                    logger.error(f"Error with fallback memory query: {str(e2)}")
                    memory_info = [{"error": f"Failed to retrieve memory information: {str(e)}"}]
            
            # Compile the results
            result = {
                "host_information": host_info,
                "service_status": service_info,
                "memory_usage": memory_info
            }
            
            return {
                "content": format_result_content(result),
                "isError": False
            }
    except Exception as e:
        logger.error(f"Error getting system overview: {str(e)}", exc_info=True)
        return {
            "content": [{"type": "text", "text": f"Error getting system overview: {str(e)}"}],
            "isError": True
        }
