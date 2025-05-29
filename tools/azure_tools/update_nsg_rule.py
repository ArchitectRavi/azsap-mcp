#!/usr/bin/env python3
"""
Azure NSG Rule Update Module

This module provides a function for updating existing Network Security Group (NSG) rules.
"""
import logging
from typing import Dict, Any, List, Optional, Union

from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.network.models import (
    SecurityRule,
    SecurityRuleAccess,
    SecurityRuleDirection,
    SecurityRuleProtocol
)
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential, 
    get_subscription_id, 
    get_resource_group
)

# Configure logging
logger = logging.getLogger(__name__)

async def update_nsg_rule(
    nsg_name: str,
    rule_name: str,
    priority: Optional[int] = None,
    direction: Optional[str] = None,
    access: Optional[str] = None,
    protocol: Optional[str] = None,
    source_address_prefix: Optional[str] = None,
    source_address_prefixes: Optional[List[str]] = None,
    source_port_range: Optional[str] = None,
    source_port_ranges: Optional[List[str]] = None,
    destination_address_prefix: Optional[str] = None,
    destination_address_prefixes: Optional[List[str]] = None,
    destination_port_range: Optional[str] = None,
    destination_port_ranges: Optional[List[str]] = None,
    description: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Update an existing rule in a Network Security Group
    
    Args:
        nsg_name (str): NSG name
        rule_name (str): Rule name
        priority (int, optional): Rule priority (100-4096)
        direction (str, optional): Rule direction ("Inbound" or "Outbound")
        access (str, optional): Rule access ("Allow" or "Deny")
        protocol (str, optional): Rule protocol ("Tcp", "Udp", "Icmp", "*")
        source_address_prefix (str, optional): Source address prefix
        source_address_prefixes (List[str], optional): Source address prefixes
        source_port_range (str, optional): Source port range
        source_port_ranges (List[str], optional): Source port ranges
        destination_address_prefix (str, optional): Destination address prefix
        destination_address_prefixes (List[str], optional): Destination address prefixes
        destination_port_range (str, optional): Destination port range
        destination_port_ranges (List[str], optional): Destination port ranges
        description (str, optional): Rule description
        resource_group (str, optional): Resource group name
        subscription_id (str, optional): Subscription ID
        sid (str, optional): SAP System ID
        auth_context (Dict[str, Any], optional): Authentication context
        
    Returns:
        Dict[str, Any]: Operation result
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_MODIFY", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_MODIFY permission required"
                    }
        
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Network Management Client
        network_client = NetworkManagementClient(credential, subscription_id)
        
        # Get the existing rule to update only specified parameters
        try:
            existing_rule = network_client.security_rules.get(
                resource_group,
                nsg_name,
                rule_name
            )
        except ResourceNotFoundError:
            return {
                "status": "error",
                "message": f"Rule {rule_name} not found in NSG {nsg_name}"
            }
        
        # Create updated rule parameters
        security_rule_params = SecurityRule(
            priority=priority if priority is not None else existing_rule.priority,
            protocol=protocol if protocol is not None else existing_rule.protocol,
            access=access if access is not None else existing_rule.access,
            direction=direction if direction is not None else existing_rule.direction,
            description=description if description is not None else existing_rule.description,
            source_address_prefix=source_address_prefix if source_address_prefix is not None else existing_rule.source_address_prefix,
            source_address_prefixes=source_address_prefixes if source_address_prefixes is not None else existing_rule.source_address_prefixes,
            source_port_range=source_port_range if source_port_range is not None else existing_rule.source_port_range,
            source_port_ranges=source_port_ranges if source_port_ranges is not None else existing_rule.source_port_ranges,
            destination_address_prefix=destination_address_prefix if destination_address_prefix is not None else existing_rule.destination_address_prefix,
            destination_address_prefixes=destination_address_prefixes if destination_address_prefixes is not None else existing_rule.destination_address_prefixes,
            destination_port_range=destination_port_range if destination_port_range is not None else existing_rule.destination_port_range,
            destination_port_ranges=destination_port_ranges if destination_port_ranges is not None else existing_rule.destination_port_ranges
        )
        
        # Update the rule
        logger.info(f"Updating rule {rule_name} in NSG {nsg_name} in resource group {resource_group}")
        network_client.security_rules.begin_create_or_update(
            resource_group,
            nsg_name,
            rule_name,
            security_rule_params
        ).result()
        
        return {
            "status": "success",
            "message": f"Rule {rule_name} updated in NSG {nsg_name}",
            "rule": {
                "name": rule_name,
                "priority": security_rule_params.priority,
                "direction": security_rule_params.direction,
                "access": security_rule_params.access,
                "protocol": security_rule_params.protocol,
                "source_address_prefix": security_rule_params.source_address_prefix,
                "source_address_prefixes": security_rule_params.source_address_prefixes,
                "source_port_range": security_rule_params.source_port_range,
                "source_port_ranges": security_rule_params.source_port_ranges,
                "destination_address_prefix": security_rule_params.destination_address_prefix,
                "destination_address_prefixes": security_rule_params.destination_address_prefixes,
                "destination_port_range": security_rule_params.destination_port_range,
                "destination_port_ranges": security_rule_params.destination_port_ranges,
                "description": security_rule_params.description
            }
        }
    except HttpResponseError as e:
        logger.error(f"Error updating NSG rule: {e}")
        return {
            "status": "error",
            "message": f"Error updating NSG rule: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error updating NSG rule: {e}")
        return {
            "status": "error",
            "message": f"Error updating NSG rule: {str(e)}"
        }
