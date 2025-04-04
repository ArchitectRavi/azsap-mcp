#!/usr/bin/env python3
"""
SAP VM Compliance Checks

This module checks SAP VMs against best practices for performance and reliability.
"""
import logging
from typing import Dict, Any, List, Optional, Union
from azure.mgmt.compute import ComputeManagementClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential,
    get_subscription_id,
    get_resource_group
)

# Configure logging
logger = logging.getLogger(__name__)

# SAP VM best practices
SAP_VM_BEST_PRACTICES = {
    "supported_vm_series": [
        "M", "Mv2", "MDs", "E", "Es", "Esv3", "Easv4", "Edsv4", "Eds", 
        "GS", "Ds", "Dsv2", "Dsv3", "Dsv4", "Ddsv4", "Ddsv5", "Dadsv5"
    ],
    "min_memory_gb": {
        "HANA": 128,
        "AnyDB": 64,
        "App": 32
    },
    "min_cores": {
        "HANA": 16,
        "AnyDB": 8,
        "App": 4
    },
    "premium_disk_required": True,
    "accelerated_networking_required": True,
    # Additional best practices from the workbook
    "availability_set_required": True,
    "backup_required": True
}

async def check_vm_compliance(
    vm_name: str,
    sap_component_type: str = "HANA",  # Options: HANA, AnyDB, App
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Check if a VM complies with SAP best practices
    
    Args:
        vm_name (str): Azure VM name
        sap_component_type (str): SAP component type (HANA, AnyDB, App)
        resource_group (str, optional): Resource group name
        subscription_id (str, optional): Subscription ID
        sid (str, optional): SAP System ID
        auth_context (Dict[str, Any], optional): Authentication context
        
    Returns:
        Dict[str, Any]: Compliance check results
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
        
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM details
        try:
            vm = compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')
            nics = compute_client.virtual_machines.list_network_interfaces(resource_group, vm_name)
            
            # Get VM size details
            vm_size = vm.hardware_profile.vm_size
            vm_series = vm_size.split('_')[0]
            
            # Get VM size data
            vm_sizes = list(compute_client.virtual_machine_sizes.list(vm.location))
            current_vm_size = next((s for s in vm_sizes if s.name == vm_size), None)
            
            compliance_checks = []
            all_compliant = True
            
            # Check VM series
            series_check = {
                "check_name": "VM Series",
                "expected": ", ".join(SAP_VM_BEST_PRACTICES["supported_vm_series"]),
                "actual": vm_series,
                "compliant": any(vm_series.startswith(s) for s in SAP_VM_BEST_PRACTICES["supported_vm_series"]),
                "recommendation": "Use a VM series certified for SAP workloads"
            }
            compliance_checks.append(series_check)
            all_compliant = all_compliant and series_check["compliant"]
            
            # Memory check
            memory_check = {
                "check_name": "Memory Size",
                "expected": f">= {SAP_VM_BEST_PRACTICES['min_memory_gb'][sap_component_type]} GB",
                "actual": f"{current_vm_size.memory_in_mb / 1024:.1f} GB" if current_vm_size else "Unknown",
                "compliant": current_vm_size and (current_vm_size.memory_in_mb / 1024) >= SAP_VM_BEST_PRACTICES["min_memory_gb"][sap_component_type],
                "recommendation": f"Ensure VM has at least {SAP_VM_BEST_PRACTICES['min_memory_gb'][sap_component_type]} GB of memory for {sap_component_type} workloads"
            }
            compliance_checks.append(memory_check)
            all_compliant = all_compliant and memory_check["compliant"]
            
            # CPU cores check
            cores_check = {
                "check_name": "CPU Cores",
                "expected": f">= {SAP_VM_BEST_PRACTICES['min_cores'][sap_component_type]}",
                "actual": f"{current_vm_size.number_of_cores}" if current_vm_size else "Unknown",
                "compliant": current_vm_size and current_vm_size.number_of_cores >= SAP_VM_BEST_PRACTICES["min_cores"][sap_component_type],
                "recommendation": f"Ensure VM has at least {SAP_VM_BEST_PRACTICES['min_cores'][sap_component_type]} cores for {sap_component_type} workloads"
            }
            compliance_checks.append(cores_check)
            all_compliant = all_compliant and cores_check["compliant"]
            
            # Check premium storage
            has_premium_storage = False
            for disk in vm.storage_profile.data_disks:
                if disk.managed_disk and disk.managed_disk.storage_account_type and 'Premium' in disk.managed_disk.storage_account_type:
                    has_premium_storage = True
                    break
            
            storage_check = {
                "check_name": "Premium Storage",
                "expected": "Yes",
                "actual": "Yes" if has_premium_storage else "No",
                "compliant": has_premium_storage,
                "recommendation": "Use Premium Storage for SAP workloads"
            }
            compliance_checks.append(storage_check)
            all_compliant = all_compliant and storage_check["compliant"]
            
            # Check accelerated networking
            has_accelerated_networking = False
            nic_list = list(nics)
            for nic in nic_list:
                if nic.enable_accelerated_networking:
                    has_accelerated_networking = True
                    break
            
            network_check = {
                "check_name": "Accelerated Networking",
                "expected": "Enabled",
                "actual": "Enabled" if has_accelerated_networking else "Disabled",
                "compliant": has_accelerated_networking,
                "recommendation": "Enable Accelerated Networking for improved network performance"
            }
            compliance_checks.append(network_check)
            all_compliant = all_compliant and network_check["compliant"]
            
            # Check if VM is in an availability set
            in_availability_set = vm.availability_set is not None
            
            availability_check = {
                "check_name": "Availability Set",
                "expected": "Yes",
                "actual": "Yes" if in_availability_set else "No",
                "compliant": in_availability_set,
                "recommendation": "Deploy SAP VMs in an availability set for high availability"
            }
            compliance_checks.append(availability_check)
            all_compliant = all_compliant and availability_check["compliant"]
            
            return {
                "status": "success",
                "vm_name": vm_name,
                "resource_group": resource_group,
                "sap_component_type": sap_component_type,
                "overall_compliance": "Compliant" if all_compliant else "Non-compliant",
                "compliance_checks": compliance_checks,
                "recommendations": [check["recommendation"] for check in compliance_checks if not check["compliant"]]
            }
            
        except ResourceNotFoundError:
            return {
                "status": "error",
                "message": f"VM {vm_name} not found in resource group {resource_group}"
            }
            
    except HttpResponseError as e:
        logger.error(f"Error checking VM compliance: {e}")
        return {
            "status": "error",
            "message": f"Error checking VM compliance: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error checking VM compliance: {e}")
        return {
            "status": "error",
            "message": f"Error checking VM compliance: {str(e)}"
        }
