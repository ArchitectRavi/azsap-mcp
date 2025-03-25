#!/usr/bin/env python3
"""
Command execution module for SAP/HANA administration tools
"""
import logging
import subprocess
import paramiko
import os
import json
from typing import Dict, Any, Tuple, Optional, List
from pathlib import Path
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default SSH configuration
DEFAULT_SSH_CONFIG = {
    "username": "root",
    "port": 22
}

def load_system_config() -> Dict[str, Any]:
    """
    Load system configuration from executor_config.json
    
    Returns:
        dict: Configuration dictionary
    """
    config_path = Path(__file__).parent.parent / "config" / "executor_config.json"
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {"systems": {}, "ssh": DEFAULT_SSH_CONFIG}

def get_system_config(sid: str) -> Dict[str, Any]:
    """
    Get system configuration for a specific SID
    
    Args:
        sid (str): SAP System ID
        
    Returns:
        dict: System configuration
        
    Raises:
        ValueError: If system is not found in configuration
    """
    config = load_system_config()
    systems = config.get("systems", {})
    
    # Find the system with matching SID
    for system_id, system_config in systems.items():
        if system_config.get("sid", "").upper() == sid.upper():
            return system_config
    
    # System not found
    raise ValueError(f"System with SID {sid} not found in configuration")

def get_system_info(sid: str, component: str = "db") -> Dict[str, Any]:
    """
    Get system information for a specific SID and component
    
    Args:
        sid (str): SAP System ID
        component (str): System component (app, db, etc.)
        
    Returns:
        dict: System component information
    """
    config = load_system_config()
    
    # Make SID case-insensitive by creating a mapping of uppercase SIDs to actual SIDs
    sid_map = {s.upper(): s for s in config.get("systems", {})}
    
    # Check if SID exists (case-insensitive)
    if sid.upper() not in sid_map:
        raise ValueError(f"System with SID '{sid}' not found in configuration")
    
    # Get the actual SID as it appears in the config
    actual_sid = sid_map[sid.upper()]
    system_config = config["systems"][actual_sid]
    
    # Check if component exists
    if component not in system_config.get("components", {}):
        raise ValueError(f"Component '{component}' not found for system '{actual_sid}'")
    
    component_config = system_config["components"][component]
    
    # Get global SSH config
    global_ssh_config = config.get("ssh", DEFAULT_SSH_CONFIG)
    
    # Check for system-specific SSH config
    system_ssh_config = system_config.get("ssh", {})
    
    # Merge global and system-specific SSH configs, with system-specific taking precedence
    ssh_config = {**global_ssh_config, **system_ssh_config}
    
    # Check for component-specific SSH config
    component_ssh_config = component_config.get("ssh", {})
    
    # Merge with component-specific SSH config, with component-specific taking precedence
    ssh_config = {**ssh_config, **component_ssh_config}
    
    # Get system-level SAP users
    system_sap_users = system_config.get("sap_users", {})
    
    # Check for component-specific SAP users
    component_sap_users = component_config.get("sap_users", {})
    
    # Merge system and component SAP users, with component-specific taking precedence
    sap_users = {**system_sap_users, **component_sap_users}
    
    return {
        "sid": actual_sid,
        "hostname": component_config["hostname"],
        "instance_number": component_config["instance_number"],
        "ssh": ssh_config,
        "sap_users": sap_users
    }

def list_systems() -> List[Dict[str, Any]]:
    """
    List all configured systems
    
    Returns:
        list: List of system configurations
    """
    config = load_system_config()
    systems = []
    
    for sid, system_config in config.get("systems", {}).items():
        systems.append({
            "sid": sid,
            "description": system_config.get("description", ""),
            "type": system_config.get("type", "SAP_HANA"),
            "components": list(system_config.get("components", {}).keys())
        })
    
    return systems

async def execute_command_for_system(sid: str, component: str, command: str, 
                                    use_sudo: bool = False, timeout: int = None) -> Dict[str, Any]:
    """
    Execute command on a system identified by SID and component
    
    Args:
        sid (str): SAP System ID
        component (str): System component (app, db, etc.)
        command (str): Command to execute
        use_sudo (bool): Whether to use sudo for command execution
        timeout (int): Command timeout in seconds
            
    Returns:
        dict: Command execution results with status, return_code, stdout, stderr
    """
    try:
        # Get system info
        system_info = get_system_info(sid, component)
        
        # Use system-specific timeout or default
        config = load_system_config()
        effective_timeout = timeout or config.get("default_timeout", 60)
        
        # Execute command on the host
        return await execute_command(
            host=system_info["hostname"],
            command=command,
            use_sudo=use_sudo,
            timeout=effective_timeout,
            ssh_config=system_info["ssh"]
        )
    except ValueError as e:
        logger.error(f"System configuration error: {e}")
        return {
            "status": "error",
            "error": str(e),
            "return_code": -1,
            "stdout": "",
            "stderr": f"Configuration error: {e}"
        }
    except Exception as e:
        logger.error(f"Error executing command for {sid}/{component}: {e}")
        return {
            "status": "error",
            "error": str(e),
            "return_code": -1,
            "stdout": "",
            "stderr": f"Execution error: {e}"
        }

async def execute_command_as_sap_user(sid: str, component: str, command: str, 
                                     sap_user_type: str = "sidadm", timeout: int = None) -> Dict[str, Any]:
    """
    Execute command on a system as a specific SAP user (sidadm, dbadm, etc.)
    
    Args:
        sid (str): SAP System ID
        component (str): System component (app, db, etc.)
        command (str): Command to execute
        sap_user_type (str): Type of SAP user (sidadm, dbadm, etc.)
        timeout (int): Command timeout in seconds
            
    Returns:
        dict: Command execution results with status, return_code, stdout, stderr
    """
    try:
        # Get system configuration
        system_info = get_system_info(sid, component)
        if not system_info:
            raise ValueError(f"System {sid}/{component} not found in configuration")
        
        # Get effective timeout
        config = load_system_config()
        default_timeout = config.get("default_timeout", 60)
        effective_timeout = timeout if timeout is not None else default_timeout
        
        # Get SAP user credentials from the merged system_info that includes component-specific users
        sap_users = system_info.get("sap_users", {})
        if not sap_users or sap_user_type not in sap_users:
            # If specific user type not found, try to use sidadm as fallback
            if sap_user_type != "sidadm" and "sidadm" in sap_users:
                logger.warning(f"User type {sap_user_type} not found for {sid}/{component}, falling back to sidadm")
                sap_user_type = "sidadm"
            else:
                raise ValueError(f"SAP user type {sap_user_type} not configured for system {sid}/{component}")
        
        sap_user = sap_users[sap_user_type]
        username = sap_user.get("username")
        password = sap_user.get("password")
        
        if not username:
            raise ValueError(f"Username not configured for {sap_user_type} on system {sid}/{component}")
        
        # Prepare sudo command to execute as the SAP user
        sudo_command = f"sudo -u {username} {command}"
        logger.info(f"Executing command as {username} on {system_info['hostname']} ({component}): {command}")
        
        # Execute command
        return await execute_command(
            host=system_info["hostname"],
            command=sudo_command,
            use_sudo=False,  # We're already using sudo in the command
            timeout=effective_timeout,
            ssh_config=system_info["ssh"]
        )
    except ValueError as e:
        logger.error(f"System configuration error: {e}")
        return {
            "status": "error",
            "error": str(e),
            "return_code": -1,
            "stdout": "",
            "stderr": f"Configuration error: {e}"
        }
    except Exception as e:
        logger.error(f"Error executing command as SAP user for {sid}/{component}: {e}")
        return {
            "status": "error",
            "error": str(e),
            "return_code": -1,
            "stdout": "",
            "stderr": f"Execution error: {e}"
        }

async def execute_command(host: str, command: str, use_sudo: bool = False, 
                         timeout: int = 60, ssh_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute command on target host
    
    Args:
        host (str): Target hostname or IP
        command (str): Command to execute
        use_sudo (bool): Whether to use sudo for command execution
        timeout (int): Command timeout in seconds
        ssh_config (dict): SSH configuration for remote execution
            
    Returns:
        dict: Command execution results with status, return_code, stdout, stderr
    """
    # Log the command execution
    logger.info(f"Executing command on {host}: {command}")
    
    try:
        # Check if local execution
        if host in ['localhost', '127.0.0.1'] or not host:
            return_code, stdout, stderr = await _execute_local(command, use_sudo, timeout)
        else:
            # Use provided SSH config or default
            effective_ssh_config = ssh_config or DEFAULT_SSH_CONFIG
            return_code, stdout, stderr = await _execute_remote(host, command, use_sudo, timeout, effective_ssh_config)
        
        # Prepare result
        status = "success" if return_code == 0 else "error"
        
        return {
            "status": status,
            "return_code": return_code,
            "stdout": stdout,
            "stderr": stderr
        }
    except Exception as e:
        logger.error(f"Command execution error: {e}")
        return {
            "status": "error",
            "error": str(e),
            "return_code": -1,
            "stdout": "",
            "stderr": str(e)
        }

async def _execute_local(command: str, use_sudo: bool = False, timeout: int = 60) -> Tuple[int, str, str]:
    """
    Execute command locally
    
    Args:
        command (str): Command to execute
        use_sudo (bool): Whether to use sudo
        timeout (int): Command timeout in seconds
        
    Returns:
        tuple: (return_code, stdout, stderr)
    """
    try:
        # Prepare command with sudo if required
        if use_sudo:
            full_command = f"sudo {command}"
        else:
            full_command = command
            
        # Execute the command
        process = await asyncio.create_subprocess_shell(
            full_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for command completion with timeout
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        return process.returncode, stdout.decode('utf-8'), stderr.decode('utf-8')
        
    except asyncio.TimeoutError:
        logger.error(f"Command timeout after {timeout} seconds: {command}")
        return -1, "", f"Command timeout after {timeout} seconds"
        
    except Exception as e:
        logger.error(f"Command execution error: {str(e)}")
        return -1, "", str(e)

async def _execute_remote(host: str, command: str, use_sudo: bool = False, 
                         timeout: int = 60, ssh_config: Dict[str, Any] = None) -> Tuple[int, str, str]:
    """
    Execute command on remote host via SSH
    
    Args:
        host (str): Target hostname or IP
        command (str): Command to execute
        use_sudo (bool): Whether to use sudo
        timeout (int): Command timeout in seconds
        ssh_config (dict): SSH configuration
        
    Returns:
        tuple: (return_code, stdout, stderr)
    """
    client = None
    try:
        # Get SSH connection details
        username = ssh_config.get("username", "root")
        key_file = ssh_config.get("key_file")
        password = ssh_config.get("password")
        port = ssh_config.get("port", 22)
        use_key_auth = ssh_config.get("use_key_auth", True if key_file else False)
        key_requires_passphrase = ssh_config.get("key_requires_passphrase", False)
        
        # Create SSH client
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to remote host based on authentication method
        if use_key_auth and key_file and os.path.exists(key_file):
            logger.debug(f"Connecting to {host} using key-based authentication")
            if key_requires_passphrase and password:
                # Use key with passphrase
                pkey = paramiko.RSAKey.from_private_key_file(key_file, password=password)
                client.connect(
                    hostname=host,
                    username=username,
                    pkey=pkey,
                    port=port,
                    timeout=timeout
                )
            else:
                # Use key without passphrase
                client.connect(
                    hostname=host,
                    username=username,
                    key_filename=key_file,
                    port=port,
                    timeout=timeout
                )
        else:
            # Use password authentication
            logger.debug(f"Connecting to {host} using password authentication")
            client.connect(
                hostname=host,
                username=username,
                password=password,
                port=port,
                timeout=timeout
            )
            
        # Prepare command with sudo if required
        if use_sudo:
            full_command = f"sudo {command}"
        else:
            full_command = command
            
        # Execute command
        stdin, stdout, stderr = client.exec_command(full_command, timeout=timeout)
        
        # Get return code
        return_code = stdout.channel.recv_exit_status()
        
        # Get output
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')
        
        return return_code, stdout_str, stderr_str
        
    except Exception as e:
        logger.error(f"Remote execution error on {host}: {str(e)}")
        return -1, "", str(e)
    finally:
        # Ensure client is closed
        if client:
            client.close()

# Example usage
async def main():
    # Example: List all systems
    systems = list_systems()
    print(f"Configured systems: {json.dumps(systems, indent=2)}")
    
    # Example: Execute command on a system
    if systems:
        sid = systems[0]["sid"]
        component = systems[0]["components"][0]
        result = await execute_command_for_system(
            sid=sid,
            component=component,
            command="df -h",
            use_sudo=True
        )
        print(f"Command result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
