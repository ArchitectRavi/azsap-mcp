"""
Utility functions for SAP HANA MCP tools.

This module provides common utility functions used across different tools.
"""

import json
import decimal
from typing import Any, Dict, List

# Custom JSON encoder for handling Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

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
