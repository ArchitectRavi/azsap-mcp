#!/usr/bin/env python3
"""
Azure Resource Information Module

This module provides functions for retrieving information about Azure resources
related to SAP systems, including resource groups, VM details, and metrics.
"""
import logging
import datetime
from typing import Dict, Any, List, Optional, Union

from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential, 
    get_subscription_id, 
    get_resource_group,
    get_vm_name
)

# Configure logging
logger = logging.getLogger(__name__)

async def get_resource_groups(
    subscription_id: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get Azure resource groups
    
    Args:
        subscription_id (str, optional): Subscription ID. Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: List of resource groups
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
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Resource Management Client
        resource_client = ResourceManagementClient(credential, subscription_id)
        
        # List resource groups
        logger.info(f"Listing resource groups in subscription {subscription_id}")
        resource_groups = []
        
        for rg in resource_client.resource_groups.list():
            resource_groups.append({
                "name": rg.name,
                "location": rg.location,
                "provisioning_state": rg.properties.provisioning_state if hasattr(rg, 'properties') and hasattr(rg.properties, 'provisioning_state') else "Unknown",
                "tags": rg.tags if rg.tags else {}
            })
        
        return {
            "status": "success",
            "resource_groups": resource_groups
        }
    except Exception as e:
        logger.error(f"Error getting resource groups: {e}")
        return {
            "status": "error",
            "message": f"Error getting resource groups: {str(e)}"
        }

async def get_vm_details(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get detailed information about an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: VM details
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
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                from tools.command_executor import get_system_info
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM
        logger.info(f"Getting details for VM {vm_name} in resource group {resource_group}")
        vm = compute_client.virtual_machines.get(
            resource_group, 
            vm_name, 
            expand="instanceView"
        )
        
        # Extract status information
        statuses = vm.instance_view.statuses if vm.instance_view else []
        power_state = next((s.display_status for s in statuses if s.code.startswith("PowerState/")), "Unknown")
        provision_state = next((s.display_status for s in statuses if s.code.startswith("ProvisioningState/")), "Unknown")
        
        # Get VM details
        vm_size = vm.hardware_profile.vm_size if vm.hardware_profile else "Unknown"
        os_type = vm.storage_profile.os_disk.os_type if vm.storage_profile and vm.storage_profile.os_disk else "Unknown"
        location = vm.location
        
        # Get network interfaces
        network_interfaces = []
        if vm.network_profile and vm.network_profile.network_interfaces:
            for nic in vm.network_profile.network_interfaces:
                nic_id = nic.id
                nic_name = nic_id.split("/")[-1] if nic_id else "Unknown"
                network_interfaces.append(nic_name)
        
        # Get disks
        disks = []
        if vm.storage_profile:
            # OS disk
            if vm.storage_profile.os_disk:
                os_disk = vm.storage_profile.os_disk
                disks.append({
                    "name": os_disk.name,
                    "type": "OS",
                    "caching": os_disk.caching,
                    "create_option": os_disk.create_option,
                    "disk_size_gb": os_disk.disk_size_gb,
                    "managed_disk": {
                        "id": os_disk.managed_disk.id if os_disk.managed_disk else None,
                        "storage_account_type": os_disk.managed_disk.storage_account_type if os_disk.managed_disk else None
                    } if os_disk.managed_disk else None
                })
            
            # Data disks
            if vm.storage_profile.data_disks:
                for data_disk in vm.storage_profile.data_disks:
                    disks.append({
                        "name": data_disk.name,
                        "type": "Data",
                        "caching": data_disk.caching,
                        "create_option": data_disk.create_option,
                        "disk_size_gb": data_disk.disk_size_gb,
                        "lun": data_disk.lun,
                        "managed_disk": {
                            "id": data_disk.managed_disk.id if data_disk.managed_disk else None,
                            "storage_account_type": data_disk.managed_disk.storage_account_type if data_disk.managed_disk else None
                        } if data_disk.managed_disk else None
                    })
        
        # Get tags
        tags = vm.tags if vm.tags else {}
        
        return {
            "status": "success",
            "vm_details": {
                "name": vm_name,
                "resource_group": resource_group,
                "location": location,
                "size": vm_size,
                "os_type": os_type,
                "power_state": power_state,
                "provisioning_state": provision_state,
                "network_interfaces": network_interfaces,
                "disks": disks,
                "tags": tags
            }
        }
    except ResourceNotFoundError as e:
        logger.error(f"VM not found: {e}")
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except Exception as e:
        logger.error(f"Error getting VM details: {e}")
        return {
            "status": "error",
            "message": f"Error getting VM details: {str(e)}"
        }

async def get_vm_metrics(
    sid: Optional[str] = None,
    vm_name: Optional[str] = None,
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    component: Optional[str] = None,
    metric_names: Optional[List[str]] = None,
    time_grain: str = "PT1H",
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Get metrics for an Azure VM
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        subscription_id (str, optional): Subscription ID. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        metric_names (List[str], optional): List of metric names to retrieve. Defaults to None.
        time_grain (str, optional): Time grain for metrics. Defaults to "PT1H" (1 hour).
        start_time (datetime.datetime, optional): Start time for metrics. Defaults to 24 hours ago.
        end_time (datetime.datetime, optional): End time for metrics. Defaults to now.
        auth_context (Dict[str, Any], optional): Authentication context. Defaults to None.
        
    Returns:
        Dict[str, Any]: VM metrics
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
        
        # Get system info if SID is provided
        if sid and not vm_name:
            try:
                from tools.command_executor import get_system_info
                system_info = get_system_info(sid, component)
                if "azure" in system_info and "vm_name" in system_info["azure"]:
                    vm_name = system_info["azure"]["vm_name"]
            except Exception as e:
                logger.warning(f"Could not get system info for SID {sid}: {e}")
        
        # Get VM name from config if not provided
        if not vm_name:
            vm_name = get_vm_name(sid, vm_name, component)
            
        # Get resource group from config if not provided
        if not resource_group:
            resource_group = get_resource_group(sid, resource_group)
            
        # Get subscription ID from config if not provided
        subscription_id = get_subscription_id(subscription_id)
        
        # Set default metric names if not provided
        if not metric_names:
            metric_names = [
                "Percentage CPU",
                "Available Memory Bytes",
                "Disk Read Bytes",
                "Disk Write Bytes",
                "Network In Total",
                "Network Out Total"
            ]
            
        # Set default time range if not provided
        if not start_time:
            start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        if not end_time:
            end_time = datetime.datetime.utcnow()
        
        # Get Azure credential
        credential = get_azure_credential()
        
        # Create Compute Management Client to get VM resource ID
        compute_client = ComputeManagementClient(credential, subscription_id)
        
        # Get VM to get resource ID
        vm = compute_client.virtual_machines.get(resource_group, vm_name)
        resource_id = vm.id
        
        # Create Monitor Management Client
        monitor_client = MonitorManagementClient(credential, subscription_id)
        
        # Get metrics
        logger.info(f"Getting metrics for VM {vm_name} in resource group {resource_group}")
        metrics_data = {}
        
        for metric_name in metric_names:
            try:
                metrics = monitor_client.metrics.list(
                    resource_id,
                    timespan=f"{start_time.isoformat()}/{end_time.isoformat()}",
                    interval=time_grain,
                    metricnames=metric_name,
                    aggregation="Average"
                )
                
                # Extract metric values
                metric_values = []
                
                for metric in metrics.value:
                    if metric.timeseries and metric.timeseries[0].data:
                        for data_point in metric.timeseries[0].data:
                            if data_point.average is not None:
                                metric_values.append({
                                    "timestamp": data_point.timestamp.isoformat(),
                                    "value": data_point.average
                                })
                
                metrics_data[metric_name] = metric_values
            except Exception as e:
                logger.warning(f"Error getting metric {metric_name}: {e}")
                metrics_data[metric_name] = []
        
        return {
            "status": "success",
            "vm_metrics": {
                "name": vm_name,
                "resource_group": resource_group,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "time_grain": time_grain,
                "metrics": metrics_data
            }
        }
    except ResourceNotFoundError as e:
        logger.error(f"VM not found: {e}")
        return {
            "status": "error",
            "message": f"VM not found: {vm_name}"
        }
    except Exception as e:
        logger.error(f"Error getting VM metrics: {e}")
        return {
            "status": "error",
            "message": f"Error getting VM metrics: {str(e)}"
        }
