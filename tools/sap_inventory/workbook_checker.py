#!/usr/bin/env python3
"""
SAP Workbook Checker Tool

This module executes specific KQL queries derived from the SAP on Azure 
inventory checks workbook using Azure Resource Graph.
"""
import logging
from typing import Dict, Any, List, Optional
from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryOptions, ResultFormat
from azure.core.exceptions import HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential,
    get_subscription_id
    # Assuming get_resource_group might be needed later if queries are RG specific
)

# Configure logging
logger = logging.getLogger(__name__)

# --- Placeholder for KQL Queries Extracted from Workbook ---
# These should be populated with actual KQL from sap-inventory-checks.json
# We need to carefully handle parameter substitution (e.g., {current_vis}, {Subscriptions})
WORKBOOK_KQL_QUERIES = {
    "vm_details_for_vis": """
        // Placeholder: Query to get VM details associated with a VIS ID
        resources
        | where type =~ 'microsoft.workloads/sapvirtualinstances/centralinstances' 
           or type =~ 'microsoft.workloads/sapvirtualinstances/databaseinstances'
           or type =~ 'microsoft.workloads/sapvirtualinstances/applicationinstances'
        | where id contains '{vis_id}' // Placeholder for VIS ID
        | mv-expand vm = properties.vmDetails
        | extend vmId = tostring(vm.virtualMachineId)
        | project visId = id, vmId, instanceType = type
        // Join with VM details if needed... requires more complex query
        | limit 100 // Example limit
    """,
    "advisor_recommendations_for_vis": """
        // Placeholder: Query to get Advisor recommendations for resources under a VIS
        advisorresources
        | where type =~ 'microsoft.advisor/recommendations'
        | where properties.lastUpdated >= ago(7d) // Example filter
        | where id contains '{vis_id}' // Placeholder for VIS ID
        | project recommendationId = name, resourceId = tostring(properties.resourceMetadata.resourceId), 
                  impact = properties.impact, description = properties.shortDescription.solution, 
                  category = properties.category, lastUpdated = properties.lastUpdated
        | limit 100 // Example limit
    """
    # Add more queries here as needed...
}

async def run_sap_workbook_check(
    check_name: str,
    vis_id: Optional[str] = None, # Virtual Instance for SAP solutions resource ID
    subscription_ids: Optional[List[str]] = None, # Target subscription(s)
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Runs a specific KQL query check from the SAP Inventory Workbook definition.

    Args:
        check_name (str): The key of the query to run (must exist in WORKBOOK_KQL_QUERIES).
        vis_id (str, optional): The Azure Resource ID of the VIS to scope the query.
        subscription_ids (List[str], optional): List of subscription IDs to query. 
                                                 Uses default from context/config if None.
        auth_context (Dict[str, Any], optional): Authentication context (potentially for permissions).

    Returns:
        Dict[str, Any]: Dictionary with 'status' ('success' or 'error') and 'data' or 'message'.
    """
    if check_name not in WORKBOOK_KQL_QUERIES:
        return {"status": "error", "message": f"Unknown check name: {check_name}"}

    kql_query_template = WORKBOOK_KQL_QUERIES[check_name]

    # --- Parameter Substitution ---
    # Basic substitution for vis_id. More complex parameter handling might be needed
    # for things like subscription lists used within the KQL itself.
    if '{vis_id}' in kql_query_template:
        if not vis_id:
             return {"status": "error", "message": f"Check '{check_name}' requires a vis_id parameter."}
        kql_query = kql_query_template.replace('{vis_id}', vis_id)
    else:
        kql_query = kql_query_template
        
    # Handle subscriptions
    target_subscriptions = subscription_ids
    if not target_subscriptions:
        try:
            # Attempt to get default subscription ID if none provided
            default_sub = get_subscription_id() 
            if default_sub:
                target_subscriptions = [default_sub]
            else:
                 return {"status": "error", "message": "No subscription ID provided or found in configuration."}
        except Exception as e:
             logger.error(f"Error getting default subscription ID: {e}")
             return {"status": "error", "message": f"Error getting default subscription ID: {e}"}

    logger.info(f"Running workbook check '{check_name}' on subscriptions: {target_subscriptions}")
    # logger.debug(f"Executing KQL: {kql_query}") # Be careful logging full queries if they contain sensitive info

    try:
        # --- Permissions Check (Example) ---
        # You might add permission checks here based on auth_context if needed
        # if auth_context and not auth_context.get("permissions", {}).get("AZURE_RESOURCEGRAPH_READ", False):
        #     return {"status": "error", "message": "Permission denied: AZURE_RESOURCEGRAPH_READ required"}

        credential = get_azure_credential()
        resource_graph_client = ResourceGraphClient(credential)

        # Construct the query request
        query_request = QueryRequest(
            subscriptions=target_subscriptions,
            query=kql_query,
            options=QueryOptions(result_format=ResultFormat.object_array) # Use objectArray for easier JSON parsing
        )

        # Execute the query
        query_response = resource_graph_client.resources(query_request)

        logger.info(f"Check '{check_name}' query completed. Found {query_response.total_records} records.")

        return {
            "status": "success",
            "check_name": check_name,
            "record_count": query_response.total_records,
            "data": query_response.data # This will be a list of dictionaries
        }

    except HttpResponseError as e:
        logger.error(f"Azure Resource Graph query failed for check '{check_name}': {e}", exc_info=True)
        # Attempt to extract more specific error details if possible
        error_details = str(e)
        if e.error and hasattr(e.error, 'message'):
             error_details = e.error.message
        return {"status": "error", "message": f"Azure query failed: {error_details}"}
    except Exception as e:
        logger.error(f"Error running workbook check '{check_name}': {e}", exc_info=True)
        return {"status": "error", "message": f"An unexpected error occurred: {str(e)}"} 