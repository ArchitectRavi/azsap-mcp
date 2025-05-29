#!/usr/bin/env python3
"""
Azure NSG Operations Module

This module provides functions for managing Azure Network Security Groups (NSGs)
related to SAP systems, including listing, getting, and modifying NSG rules.
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
from tools.azure_tools.update_nsg_rule import update_nsg_rule

# Configure logging
logger = logging.getLogger(__name__)

async def get_nsg_rules(
    nsg_name: str,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get rules for a Network Security Group
    
    Args:
        nsg_name (str): NSG name
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        sid (str, optional): SAP System ID. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: NSG rules
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_VIEW", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_VIEW permission required"
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
        
        # Get NSG
        logger.info(f"Getting NSG {nsg_name} in resource group {resource_group}")
        nsg = network_client.network_security_groups.get(resource_group, nsg_name)
        
        # Extract rules
        security_rules = []
        if nsg.security_rules:
            for rule in nsg.security_rules:
                security_rules.append({
                    "name": rule.name,
                    "priority": rule.priority,
                    "direction": rule.direction,
                    "access": rule.access,
                    "protocol": rule.protocol,
                    "source_address_prefix": rule.source_address_prefix,
                    "source_address_prefixes": rule.source_address_prefixes,
                    "source_port_range": rule.source_port_range,
                    "source_port_ranges": rule.source_port_ranges,
                    "destination_address_prefix": rule.destination_address_prefix,
                    "destination_address_prefixes": rule.destination_address_prefixes,
                    "destination_port_range": rule.destination_port_range,
                    "destination_port_ranges": rule.destination_port_ranges,
                    "description": rule.description
                })
        
        default_rules = []
        if nsg.default_security_rules:
            for rule in nsg.default_security_rules:
                default_rules.append({
                    "name": rule.name,
                    "priority": rule.priority,
                    "direction": rule.direction,
                    "access": rule.access,
                    "protocol": rule.protocol,
                    "source_address_prefix": rule.source_address_prefix,
                    "source_address_prefixes": rule.source_address_prefixes,
                    "source_port_range": rule.source_port_range,
                    "source_port_ranges": rule.source_port_ranges,
                    "destination_address_prefix": rule.destination_address_prefix,
                    "destination_address_prefixes": rule.destination_address_prefixes,
                    "destination_port_range": rule.destination_port_range,
                    "destination_port_ranges": rule.destination_port_ranges,
                    "description": rule.description
                })
        
        return {
            "status": "success",
            "nsg": {
                "name": nsg.name,
                "id": nsg.id,
                "location": nsg.location,
                "resource_group": resource_group,
                "security_rules": security_rules,
                "default_security_rules": default_rules
            }
        }
    except ResourceNotFoundError as e:
        logger.error(f"NSG not found: {e}")
        return {
            "status": "error",
            "message": f"NSG not found: {nsg_name}"
        }
    except Exception as e:
        logger.error(f"Error getting NSG rules: {e}")
        return {
            "status": "error",
            "message": f"Error getting NSG rules: {str(e)}"
        }

async def add_nsg_rule(
    nsg_name: str,
    rule_name: str,
    priority: int,
    direction: str,
    access: str,
    protocol: str,
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
    Add a rule to a Network Security Group
    
    Args:
        nsg_name (str): NSG name
        rule_name (str): Rule name
        priority (int): Rule priority (100-4096)
        direction (str): Rule direction ("Inbound" or "Outbound")
        access (str): Rule access ("Allow" or "Deny")
        protocol (str): Rule protocol ("Tcp", "Udp", "Icmp", "*")
        source_address_prefix (str, optional): Source address prefix. Defaults to None.
        source_address_prefixes (List[str], optional): Source address prefixes. Defaults to None.
        source_port_range (str, optional): Source port range. Defaults to None.
        source_port_ranges (List[str], optional): Source port ranges. Defaults to None.
        destination_address_prefix (str, optional): Destination address prefix. Defaults to None.
        destination_address_prefixes (List[str], optional): Destination address prefixes. Defaults to None.
        destination_port_range (str, optional): Destination port range. Defaults to None.
        destination_port_ranges (List[str], optional): Destination port ranges. Defaults to None.
        description (str, optional): Rule description. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        sid (str, optional): SAP System ID. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
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
        
        # Validate direction
        if direction not in ["Inbound", "Outbound"]:
            return {
                "status": "error",
                "message": f"Invalid direction: {direction}. Must be 'Inbound' or 'Outbound'"
            }
            
        # Validate access
        if access not in ["Allow", "Deny"]:
            return {
                "status": "error",
                "message": f"Invalid access: {access}. Must be 'Allow' or 'Deny'"
            }
            
        # Validate protocol
        if protocol not in ["Tcp", "Udp", "Icmp", "*"]:
            return {
                "status": "error",
                "message": f"Invalid protocol: {protocol}. Must be 'Tcp', 'Udp', 'Icmp', or '*'"
            }
            
        # Validate priority
        if not 100 <= priority <= 4096:
            return {
                "status": "error",
                "message": f"Invalid priority: {priority}. Must be between 100 and 4096"
            }
        
        # Create security rule
        security_rule = SecurityRule(
            name=rule_name,
            priority=priority,
            direction=direction,
            access=access,
            protocol=protocol,
            source_address_prefix=source_address_prefix,
            source_address_prefixes=source_address_prefixes,
            source_port_range=source_port_range,
            source_port_ranges=source_port_ranges,
            destination_address_prefix=destination_address_prefix,
            destination_address_prefixes=destination_address_prefixes,
            destination_port_range=destination_port_range,
            destination_port_ranges=destination_port_ranges,
            description=description
        )
        
        # Add rule to NSG
        logger.info(f"Adding rule {rule_name} to NSG {nsg_name} in resource group {resource_group}")
        result = network_client.security_rules.begin_create_or_update(
            resource_group,
            nsg_name,
            rule_name,
            security_rule
        ).result()
        
        return {
            "status": "success",
            "message": f"Rule {rule_name} added to NSG {nsg_name}",
            "rule": {
                "name": result.name,
                "priority": result.priority,
                "direction": result.direction,
                "access": result.access,
                "protocol": result.protocol,
                "source_address_prefix": result.source_address_prefix,
                "source_address_prefixes": result.source_address_prefixes,
                "source_port_range": result.source_port_range,
                "source_port_ranges": result.source_port_ranges,
                "destination_address_prefix": result.destination_address_prefix,
                "destination_address_prefixes": result.destination_address_prefixes,
                "destination_port_range": result.destination_port_range,
                "destination_port_ranges": result.destination_port_ranges,
                "description": result.description
            }
        }
    except ResourceNotFoundError as e:
        logger.error(f"NSG not found: {e}")
        return {
            "status": "error",
            "message": f"NSG not found: {nsg_name}"
        }
    except Exception as e:
        logger.error(f"Error adding NSG rule: {e}")
        return {
            "status": "error",
            "message": f"Error adding NSG rule: {str(e)}"
        }

async def remove_nsg_rule(
    nsg_name: str,
    rule_name: str,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Remove a rule from a Network Security Group
    
    Args:
        nsg_name (str): NSG name
        rule_name (str): Rule name
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        sid (str, optional): SAP System ID. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
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
        
        # Remove rule from NSG
        logger.info(f"Removing rule {rule_name} from NSG {nsg_name} in resource group {resource_group}")
        network_client.security_rules.begin_delete(
            resource_group,
            nsg_name,
            rule_name
        ).result()
        
        return {
            "status": "success",
            "message": f"Rule {rule_name} removed from NSG {nsg_name}"
        }
    except ResourceNotFoundError as e:
        logger.error(f"NSG or rule not found: {e}")
        return {
            "status": "error",
            "message": f"NSG or rule not found: {nsg_name}/{rule_name}"
        }
    except Exception as e:
        logger.error(f"Error removing NSG rule: {e}")
        return {
            "status": "error",
            "message": f"Error removing NSG rule: {str(e)}"
        }

async def list_nsgs(
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    List Network Security Groups
    
    Args:
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        sid (str, optional): SAP System ID. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: List of NSGs
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_VIEW", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_VIEW permission required"
                    }
        
        # Get resource group from config if not provided and SID is provided
        if sid and not resource_group:
            try:
                resource_group = get_resource_group(sid, resource_group)
            except Exception as e:
                logger.warning(f"Could not get resource group for SID {sid}: {e}")
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Network Management Client
        network_client = NetworkManagementClient(credential, subscription_id)
        
        # List NSGs
        nsgs = []
        
        if resource_group:
            logger.info(f"Listing NSGs in resource group {resource_group}")
            nsg_list = network_client.network_security_groups.list(resource_group)
        else:
            logger.info(f"Listing NSGs in subscription {subscription_id}")
            nsg_list = network_client.network_security_groups.list_all()
        
        for nsg in nsg_list:
            nsg_resource_group = nsg.id.split("/")[4] if nsg.id else "Unknown"
            
            # Count rules
            security_rule_count = len(nsg.security_rules) if nsg.security_rules else 0
            default_rule_count = len(nsg.default_security_rules) if nsg.default_security_rules else 0
            
            nsgs.append({
                "name": nsg.name,
                "id": nsg.id,
                "location": nsg.location,
                "resource_group": nsg_resource_group,
                "security_rule_count": security_rule_count,
                "default_rule_count": default_rule_count
            })
        
        return {
            "status": "success",
            "nsgs": nsgs
        }
    except Exception as e:
        logger.error(f"Error listing NSGs: {e}")
        return {
            "status": "error",
            "message": f"Error listing NSGs: {str(e)}"
        }
