#!/usr/bin/env python3
"""
SAP Quality Check Module

This module provides functions to validate SAP systems on Azure against Microsoft's best practices.
It is based on the QualityCheck tool developed by Microsoft for SAP on Azure.
"""
import logging
import json
import os
import re
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from tools.azure_tools.auth import (
    get_azure_credential,
    get_subscription_id,
    get_resource_group
)
from tools.azure_tools.ssh_client import SSHClient, SSHException

# Configure logging
logger = logging.getLogger(__name__)

# Path to the Quality Check configuration file
QUALITY_CHECK_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "QualityCheck.json")

# Load Quality Check configuration
try:
    with open(QUALITY_CHECK_CONFIG_PATH, 'r') as config_file:
        QUALITY_CHECK_CONFIG = json.load(config_file)
except (FileNotFoundError, json.JSONDecodeError) as e:
    logger.error(f"Error loading Quality Check configuration: {e}")
    QUALITY_CHECK_CONFIG = {}

# Mapping of PowerShell commands to Python equivalents
PS_TO_PY_COMMANDS = {
    "Get-AzVM": "compute_client.virtual_machines.get",
    "Get-AzNetworkInterface": "network_client.network_interfaces.get",
    "Get-AzDisk": "compute_client.disks.get",
}

async def run_quality_check(
    vm_name: str,
    vm_role: str = "DB",  # Options: DB, ASCS, APP
    sap_component_type: str = "HANA",  # Options: HANA, Oracle, MSSQL, Db2, ASE
    resource_group: Optional[str] = None,
    subscription_id: Optional[str] = None,
    sid: Optional[str] = None,
    vm_os: str = "SUSE",  # Options: Windows, SUSE, RedHat, OracleLinux
    high_availability: bool = False,
    ha_agent: str = "SBD",  # Options: SBD, FencingAgent
    ssh_username: Optional[str] = None,
    ssh_password: Optional[str] = None,
    ssh_key_path: Optional[str] = None,
    ssh_host: Optional[str] = None,
    ssh_port: int = 22,
    data_dir: str = "/hana/data",
    log_dir: str = "/hana/log",
    shared_dir: str = "/hana/shared",
    auth_context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Run SAP Quality Check to validate SAP systems against Microsoft's best practices
    
    Args:
        vm_name (str): Azure VM name
        vm_role (str): SAP component role (DB, ASCS, APP)
        sap_component_type (str): SAP component type (HANA, Oracle, MSSQL, Db2, ASE)
        resource_group (str, optional): Resource group name
        subscription_id (str, optional): Subscription ID
        sid (str, optional): SAP System ID
        vm_os (str): VM operating system (Windows, SUSE, RedHat, OracleLinux)
        high_availability (bool): Whether the system is configured for high availability
        ha_agent (str): High availability agent type (SBD, FencingAgent)
        ssh_username (str, optional): SSH username for Linux VMs
        ssh_password (str, optional): SSH password for Linux VMs
        ssh_key_path (str, optional): Path to SSH key file for Linux VMs
        ssh_host (str, optional): SSH hostname or IP address for Linux VMs
        ssh_port (int): SSH port for Linux VMs
        data_dir (str): Database data directory
        log_dir (str): Database log directory
        shared_dir (str): Database shared directory
        auth_context (Dict[str, Any], optional): Authentication context
        
    Returns:
        Dict[str, Any]: Quality check results
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
        
        # Create Azure clients
        compute_client = ComputeManagementClient(credential, subscription_id)
        network_client = NetworkManagementClient(credential, subscription_id)
        storage_client = StorageManagementClient(credential, subscription_id)
        
        # Initialize results dictionary
        results = {
            "vm_info": {},
            "checks": [],
            "overall_status": "Unknown",
            "pass_count": 0,
            "fail_count": 0,
            "warning_count": 0
        }
        
        # Get VM details
        try:
            vm = compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')
            results["vm_info"] = {
                "name": vm.name,
                "resource_group": resource_group,
                "location": vm.location,
                "vm_size": vm.hardware_profile.vm_size,
                "os_type": vm.storage_profile.os_disk.os_type,
                "os_disk": vm.storage_profile.os_disk.name,
                "data_disks": [disk.name for disk in vm.storage_profile.data_disks],
                "availability_set": vm.availability_set.id if vm.availability_set else None,
                "proximity_placement_group": vm.proximity_placement_group.id if vm.proximity_placement_group else None,
                "sap_component_type": sap_component_type,
                "role": vm_role,
                "high_availability": high_availability,
                "ha_agent": ha_agent if high_availability else None,
                "subscription_id": subscription_id
            }
            
            # Get network interfaces
            network_interfaces = []
            for nic_ref in vm.network_profile.network_interfaces:
                nic_id = nic_ref.id
                nic_name = nic_id.split('/')[-1]
                nic = network_client.network_interfaces.get(resource_group, nic_name)
                network_interfaces.append({
                    "name": nic.name,
                    "primary": nic.primary,
                    "accelerated_networking": nic.enable_accelerated_networking,
                    "ip_configurations": [
                        {
                            "name": ip_config.name,
                            "private_ip": ip_config.private_ip_address,
                            "public_ip": ip_config.public_ip_address.id.split('/')[-1] if ip_config.public_ip_address else None
                        } 
                        for ip_config in nic.ip_configurations
                    ]
                })
            results["vm_info"]["network_interfaces"] = network_interfaces
            
            # Get disks details
            disks = []
            # OS disk
            os_disk = compute_client.disks.get(resource_group, vm.storage_profile.os_disk.name)
            disks.append({
                "name": os_disk.name,
                "disk_size_gb": os_disk.disk_size_gb,
                "disk_iops_read_write": os_disk.disk_iops_read_write,
                "disk_mbps_read_write": os_disk.disk_m_bps_read_write,
                "storage_account_type": os_disk.sku.name,
                "os_disk": True,
                "caching": vm.storage_profile.os_disk.caching
            })
            
            # Data disks
            for data_disk_ref in vm.storage_profile.data_disks:
                data_disk = compute_client.disks.get(resource_group, data_disk_ref.name)
                disks.append({
                    "name": data_disk.name,
                    "disk_size_gb": data_disk.disk_size_gb,
                    "disk_iops_read_write": data_disk.disk_iops_read_write,
                    "disk_mbps_read_write": data_disk.disk_m_bps_read_write,
                    "storage_account_type": data_disk.sku.name,
                    "os_disk": False,
                    "lun": data_disk_ref.lun,
                    "caching": data_disk_ref.caching
                })
            results["vm_info"]["disks"] = disks
            
            # Check if SSH connection information is provided for Linux VMs
            if vm_os in ["SUSE", "RedHat", "OracleLinux"] and ssh_host and ssh_username and (ssh_password or ssh_key_path):
                # Establish SSH connection
                ssh_client = SSHClient()
                
                try:
                    if ssh_key_path:
                        ssh_client.connect_with_key(
                            hostname=ssh_host, 
                            username=ssh_username,
                            key_path=ssh_key_path,
                            password=ssh_password,  # Can be None if key doesn't require passphrase
                            port=ssh_port
                        )
                    else:
                        ssh_client.connect_with_password(
                            hostname=ssh_host, 
                            username=ssh_username,
                            password=ssh_password,
                            port=ssh_port
                        )
                    
                    # Get OS version
                    if vm_os == "SUSE":
                        os_version_cmd = "cat /etc/os-release | grep VERSION= | cut -d '\"' -f 2"
                    elif vm_os == "RedHat":
                        os_version_cmd = "cat /etc/redhat-release"
                    elif vm_os == "OracleLinux":
                        os_version_cmd = "cat /etc/oracle-release"
                    
                    os_version_result = ssh_client.execute_command(os_version_cmd)
                    results["vm_info"]["os_version"] = os_version_result.output.strip() if os_version_result.success else "Unknown"
                    
                    # Get filesystem info
                    fs_info_cmd = "df -h"
                    fs_info_result = ssh_client.execute_command(fs_info_cmd)
                    
                    if fs_info_result.success:
                        # Parse filesystem information
                        filesystems = []
                        lines = fs_info_result.output.strip().split('\n')
                        # Skip header line
                        for line in lines[1:]:
                            parts = line.split()
                            if len(parts) >= 6:
                                filesystems.append({
                                    "filesystem": parts[0],
                                    "size": parts[1],
                                    "used": parts[2],
                                    "available": parts[3],
                                    "use_percent": parts[4],
                                    "mounted_on": parts[5]
                                })
                        results["vm_info"]["filesystems"] = filesystems
                    
                    # Get LVM info if relevant
                    lvm_info_cmd = "vgs --noheadings 2>/dev/null || echo 'No volume groups found'"
                    lvm_info_result = ssh_client.execute_command(lvm_info_cmd)
                    
                    if lvm_info_result.success and "No volume groups found" not in lvm_info_result.output:
                        # LVM is in use, get details
                        vg_info_cmd = "vgs --units g"
                        vg_info_result = ssh_client.execute_command(vg_info_cmd)
                        
                        if vg_info_result.success:
                            results["vm_info"]["lvm"] = {
                                "volume_groups": vg_info_result.output.strip()
                            }
                            
                            # Get physical volumes
                            pv_info_cmd = "pvs --units g"
                            pv_info_result = ssh_client.execute_command(pv_info_cmd)
                            
                            if pv_info_result.success:
                                results["vm_info"]["lvm"]["physical_volumes"] = pv_info_result.output.strip()
                            
                            # Get logical volumes
                            lv_info_cmd = "lvs --units g"
                            lv_info_result = ssh_client.execute_command(lv_info_cmd)
                            
                            if lv_info_result.success:
                                results["vm_info"]["lvm"]["logical_volumes"] = lv_info_result.output.strip()
                    
                    # Close SSH connection
                    ssh_client.close()
                
                except SSHException as e:
                    logger.error(f"SSH connection error: {e}")
                    results["vm_info"]["ssh_error"] = str(e)
            
            # Run checks based on QualityCheck.json configuration
            checks = []
            
            # 1. Check VM size for SAP workload support
            vm_size = vm.hardware_profile.vm_size
            vm_supported = False
            
            if vm_size in QUALITY_CHECK_CONFIG.get("SupportedVMs", {}):
                vm_config = QUALITY_CHECK_CONFIG["SupportedVMs"][vm_size]
                
                if vm_role in vm_config:
                    supported_dbs = vm_config[vm_role].get("SupportedDB", [])
                    vm_supported = sap_component_type in supported_dbs
            
            checks.append({
                "check_id": "VM-0001",
                "check_name": "Supported VM Size",
                "description": f"VM size {vm_size} support for {sap_component_type} as {vm_role}",
                "result": "Pass" if vm_supported else "Fail",
                "expected": f"VM size supports {sap_component_type} as {vm_role}",
                "actual": f"VM size {'supports' if vm_supported else 'does not support'} {sap_component_type} as {vm_role}",
                "recommendation": "Use a supported VM size for SAP workloads" if not vm_supported else ""
            })
            
            if vm_supported:
                results["pass_count"] += 1
            else:
                results["fail_count"] += 1
            
            # 2. Check for Premium Storage for data disks (if DB role)
            if vm_role == "DB":
                has_premium_storage = False
                for disk in disks:
                    if not disk["os_disk"] and "Premium" in disk["storage_account_type"]:
                        has_premium_storage = True
                        break
                
                checks.append({
                    "check_id": "DB-0001",
                    "check_name": "Premium Storage for Database",
                    "description": "Premium storage for database disks",
                    "result": "Pass" if has_premium_storage else "Fail",
                    "expected": "Premium Storage for database disks",
                    "actual": f"{'Premium' if has_premium_storage else 'Standard'} storage used for database disks",
                    "recommendation": "Use Premium Storage for database disks" if not has_premium_storage else ""
                })
                
                if has_premium_storage:
                    results["pass_count"] += 1
                else:
                    results["fail_count"] += 1
            
            # 3. Check for Accelerated Networking
            has_accelerated_networking = False
            for nic in network_interfaces:
                if nic["accelerated_networking"]:
                    has_accelerated_networking = True
                    break
            
            checks.append({
                "check_id": "NET-0001",
                "check_name": "Accelerated Networking",
                "description": "Accelerated Networking enabled",
                "result": "Pass" if has_accelerated_networking else "Fail",
                "expected": "Accelerated Networking enabled",
                "actual": f"Accelerated Networking {'enabled' if has_accelerated_networking else 'disabled'}",
                "recommendation": "Enable Accelerated Networking for improved network performance" if not has_accelerated_networking else ""
            })
            
            if has_accelerated_networking:
                results["pass_count"] += 1
            else:
                results["fail_count"] += 1
            
            # 4. Check for Availability Set (if high_availability is True)
            if high_availability:
                in_availability_set = vm.availability_set is not None
                
                checks.append({
                    "check_id": "HA-0001",
                    "check_name": "Availability Set",
                    "description": "VM in Availability Set for high availability",
                    "result": "Pass" if in_availability_set else "Fail",
                    "expected": "VM in Availability Set",
                    "actual": f"VM {'in' if in_availability_set else 'not in'} Availability Set",
                    "recommendation": "Deploy VM in an Availability Set for high availability" if not in_availability_set else ""
                })
                
                if in_availability_set:
                    results["pass_count"] += 1
                else:
                    results["fail_count"] += 1
            
            # 5. Check for Proximity Placement Group (PPG)
            has_ppg = vm.proximity_placement_group is not None
            
            checks.append({
                "check_id": "PERF-0001",
                "check_name": "Proximity Placement Group",
                "description": "VM in Proximity Placement Group for low latency",
                "result": "Warning" if not has_ppg else "Pass",
                "expected": "VM in Proximity Placement Group",
                "actual": f"VM {'in' if has_ppg else 'not in'} Proximity Placement Group",
                "recommendation": "Consider using Proximity Placement Group for low latency between SAP components" if not has_ppg else ""
            })
            
            if has_ppg:
                results["pass_count"] += 1
            else:
                results["warning_count"] += 1
            
            # Save checks to results
            results["checks"] = checks
            
            # Calculate overall status
            if results["fail_count"] > 0:
                results["overall_status"] = "Failed"
            elif results["warning_count"] > 0:
                results["overall_status"] = "Warning"
            else:
                results["overall_status"] = "Passed"
            
            return {
                "status": "success",
                "data": results
            }
            
        except ResourceNotFoundError:
            return {
                "status": "error",
                "message": f"VM {vm_name} not found in resource group {resource_group}"
            }
            
    except HttpResponseError as e:
        logger.error(f"Error running quality check: {e}")
        return {
            "status": "error",
            "message": f"Error running quality check: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error running quality check: {e}")
        return {
            "status": "error",
            "message": f"Error running quality check: {str(e)}"
        }


async def get_quality_check_definitions() -> Dict[str, Any]:
    """
    Get the QualityCheck configuration definitions
    
    Returns:
        Dict[str, Any]: Quality check definitions
    """
    try:
        return {
            "status": "success",
            "data": {
                "supported_vms": list(QUALITY_CHECK_CONFIG.get("SupportedVMs", {}).keys()),
                "supported_os_db_combinations": QUALITY_CHECK_CONFIG.get("SupportedOSDBCombinations", {}),
                "checks": QUALITY_CHECK_CONFIG.get("Checks", []),
                "version": QUALITY_CHECK_CONFIG.get("Version", "Unknown")
            }
        }
    except Exception as e:
        logger.error(f"Error getting quality check definitions: {e}")
        return {
            "status": "error",
            "message": f"Error getting quality check definitions: {str(e)}"
        }
