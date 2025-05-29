#!/usr/bin/env python3
"""
SAP Inventory Summary Module

This module provides functions to get an overview of SAP systems deployed in Azure.
"""
import logging
from typing import Dict, Any, List, Optional, Union
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential,
    get_subscription_id,
    get_resource_group
)

# Configure logging
logger = logging.getLogger(__name__)

# SAP tags to look for
SAP_TAGS = [
    "SAP", "SID", "SAPSID", "SAP-SID", "SAPSystemId", 
    "SAPSystem", "SAP System", "ApplicationRole"
]

async def get_sap_inventory_summary(
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get summary of SAP systems and components in Azure
    
    Args:
        resource_group (str, optional): Resource group name to filter resources
        subscription_id (str, optional): Subscription ID
        sid (str, optional): SAP System ID to filter resources
        auth_context (Dict[str, Any], optional): Authentication context
        
    Returns:
        Dict[str, Any]: SAP inventory summary
    """
    try:
        # Check permissions if auth_context is provided
        if auth_context and "permissions" in auth_context:
            if not auth_context.get("permissions", {}).get("AZURE_READ", False):
                if "ADMIN" not in auth_context.get("roles", []):
                    return {
                        "status": "error",
                        "message": "Permission denied: AZURE_READ permission required"
                    }
        
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Resource Management Client
        resource_client = ResourceManagementClient(credential, subscription_id)
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Query resource groups if not specified
        resource_groups = []
        if resource_group:
            resource_groups = [resource_group]
        else:
            # List all resource groups
            for rg in resource_client.resource_groups.list():
                # If SID filter is applied, check resource group tags or name
                if sid:
                    # Check if resource group name contains SID
                    if sid.lower() in rg.name.lower():
                        resource_groups.append(rg.name)
                        continue
                    
                    # Check if resource group tags contain SID
                    if rg.tags:
                        for tag_key, tag_value in rg.tags.items():
                            if (tag_key in SAP_TAGS and sid.lower() in str(tag_value).lower()) or \
                               (sid.lower() in tag_key.lower()):
                                resource_groups.append(rg.name)
                                break
                else:
                    resource_groups.append(rg.name)
        
        # Initialize summary
        summary = {
            "sap_systems": [],
            "vm_count": 0,
            "disk_count": 0,
            "availability_set_count": 0,
            "identified_components": {
                "database": [],
                "application": [],
                "central_services": [],
                "web_dispatcher": []
            },
            # Add workbook-aligned metrics
            "vm_series_distribution": {},
            "disk_types_distribution": {},
            "compliance_status": {
                "compliant": 0,
                "non_compliant": 0,
                "unknown": 0
            }
        }
        
        # Process each resource group
        for rg_name in resource_groups:
            # Track current SAP system
            current_sap_system = {
                "resource_group": rg_name,
                "components": [],
                "availability_sets": 0,
                "compliance_status": "Unknown"
            }
            
            # List all VMs in the resource group
            try:
                vms = list(compute_client.virtual_machines.list(rg_name))
                summary["vm_count"] += len(vms)
                
                # Process each VM to identify SAP components
                for vm in vms:
                    vm_tags = vm.tags or {}
                    vm_name = vm.name
                    vm_size = vm.hardware_profile.vm_size
                    vm_series = vm_size.split('_')[0]
                    
                    # Track VM series distribution
                    if vm_series in summary["vm_series_distribution"]:
                        summary["vm_series_distribution"][vm_series] += 1
                    else:
                        summary["vm_series_distribution"][vm_series] = 1
                    
                    # Try to identify SAP components from VM tags and name
                    is_sap_vm = False
                    component_type = "unknown"
                    sap_sid = None
                    
                    # Check VM tags for SAP indicators
                    for tag_key, tag_value in vm_tags.items():
                        # Look for SAP tags
                        if tag_key in SAP_TAGS:
                            is_sap_vm = True
                            sap_sid = tag_value
                            
                        # Try to determine component type
                        tag_key_lower = tag_key.lower()
                        tag_value_lower = str(tag_value).lower()
                        
                        if "db" in tag_key_lower or "database" in tag_key_lower or \
                           "hana" in tag_key_lower or "db" in tag_value_lower or \
                           "database" in tag_value_lower or "hana" in tag_value_lower:
                            component_type = "database"
                            summary["identified_components"]["database"].append(vm_name)
                            
                        elif "ascs" in tag_key_lower or "scs" in tag_key_lower or \
                             "ers" in tag_key_lower or "central" in tag_key_lower or \
                             "ascs" in tag_value_lower or "scs" in tag_value_lower or \
                             "ers" in tag_value_lower:
                            component_type = "central_services"
                            summary["identified_components"]["central_services"].append(vm_name)
                            
                        elif "app" in tag_key_lower or "application" in tag_key_lower or \
                             "pas" in tag_key_lower or "aas" in tag_key_lower or \
                             "app" in tag_value_lower or "pas" in tag_value_lower or \
                             "aas" in tag_value_lower:
                            component_type = "application"
                            summary["identified_components"]["application"].append(vm_name)
                            
                        elif "web" in tag_key_lower or "webdisp" in tag_key_lower or \
                             "web" in tag_value_lower or "webdisp" in tag_value_lower:
                            component_type = "web_dispatcher"
                            summary["identified_components"]["web_dispatcher"].append(vm_name)
                    
                    # Check VM name for component indicators if not identified from tags
                    if component_type == "unknown":
                        vm_name_lower = vm_name.lower()
                        
                        if "db" in vm_name_lower or "hana" in vm_name_lower or "sql" in vm_name_lower:
                            component_type = "database"
                            summary["identified_components"]["database"].append(vm_name)
                            is_sap_vm = True
                            
                        elif "ascs" in vm_name_lower or "scs" in vm_name_lower or "ers" in vm_name_lower:
                            component_type = "central_services"
                            summary["identified_components"]["central_services"].append(vm_name)
                            is_sap_vm = True
                            
                        elif "app" in vm_name_lower or "pas" in vm_name_lower or "aas" in vm_name_lower:
                            component_type = "application"
                            summary["identified_components"]["application"].append(vm_name)
                            is_sap_vm = True
                            
                        elif "web" in vm_name_lower or "wd" in vm_name_lower:
                            component_type = "web_dispatcher"
                            summary["identified_components"]["web_dispatcher"].append(vm_name)
                            is_sap_vm = True
                    
                    # If this is an SAP VM, add to the component list
                    if is_sap_vm:
                        # Try to extract SID from name if not found in tags
                        if not sap_sid and len(vm_name) >= 3:
                            # Common naming patterns for SAP VMs include SID
                            # Example: hanadb-s4h-vm1 -> S4H might be the SID
                            parts = vm_name.split('-')
                            for part in parts:
                                if len(part) == 3 and part.isalnum():
                                    sap_sid = part.upper()
                                    break
                        
                        current_sap_system["sid"] = sap_sid
                        
                        # Check if VM is in an availability set
                        in_availability_set = vm.availability_set is not None
                        
                        # Check for premium storage
                        has_premium_storage = False
                        for disk in vm.storage_profile.data_disks:
                            if disk.managed_disk and disk.managed_disk.storage_account_type and 'Premium' in disk.managed_disk.storage_account_type:
                                has_premium_storage = True
                                break
                        
                        # Basic compliance check
                        is_compliant = in_availability_set and has_premium_storage
                        compliance_status = "Compliant" if is_compliant else "Non-compliant"
                        
                        if is_compliant:
                            summary["compliance_status"]["compliant"] += 1
                        else:
                            summary["compliance_status"]["non_compliant"] += 1
                        
                        current_sap_system["components"].append({
                            "vm_name": vm_name,
                            "component_type": component_type,
                            "vm_size": vm.hardware_profile.vm_size,
                            "os_type": vm.storage_profile.os_disk.os_type,
                            "in_availability_set": in_availability_set,
                            "has_premium_storage": has_premium_storage,
                            "compliance_status": compliance_status
                        })
                
                # Count disks and track disk types
                disks = list(compute_client.disks.list_by_resource_group(rg_name))
                summary["disk_count"] += len(disks)
                
                # Track disk type distribution
                for disk in disks:
                    disk_type = disk.sku.name if disk.sku and disk.sku.name else "Unknown"
                    if disk_type in summary["disk_types_distribution"]:
                        summary["disk_types_distribution"][disk_type] += 1
                    else:
                        summary["disk_types_distribution"][disk_type] = 1
                
                # Count availability sets
                availability_sets = compute_client.availability_sets.list_by_resource_group(rg_name)
                availability_set_count = len(list(availability_sets))
                summary["availability_set_count"] += availability_set_count
                current_sap_system["availability_sets"] = availability_set_count
                
                # Set overall compliance status for the SAP system
                if current_sap_system["components"]:
                    compliant_components = sum(1 for comp in current_sap_system["components"] if comp["compliance_status"] == "Compliant")
                    if compliant_components == len(current_sap_system["components"]):
                        current_sap_system["compliance_status"] = "Compliant"
                    elif compliant_components > 0:
                        current_sap_system["compliance_status"] = "Partially Compliant"
                    else:
                        current_sap_system["compliance_status"] = "Non-compliant"
                
                # Add the SAP system to summary if components were found
                if current_sap_system["components"]:
                    summary["sap_systems"].append(current_sap_system)
                
            except Exception as e:
                logger.warning(f"Error processing resource group {rg_name}: {str(e)}")
                continue
        
        # Calculate summary stats
        summary["total_sap_systems"] = len(summary["sap_systems"])
        summary["total_components"] = sum(len(sys["components"]) for sys in summary["sap_systems"])
        
        # Calculate component type counts
        summary["component_counts"] = {
            "database": len(summary["identified_components"]["database"]),
            "application": len(summary["identified_components"]["application"]),
            "central_services": len(summary["identified_components"]["central_services"]),
            "web_dispatcher": len(summary["identified_components"]["web_dispatcher"])
        }
        
        return {
            "status": "success",
            "summary": summary
        }
            
    except HttpResponseError as e:
        logger.error(f"Error getting SAP inventory summary: {e}")
        return {
            "status": "error",
            "message": f"Error getting SAP inventory summary: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error getting SAP inventory summary: {e}")
        return {
            "status": "error",
            "message": f"Error getting SAP inventory summary: {str(e)}"
        }
