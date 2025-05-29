#!/usr/bin/env python3
"""
SSH Client Module for Azure Tools

This module provides functionality to connect to Linux VMs via SSH
and execute commands remotely.
"""
import os
import logging
import paramiko
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union

# Configure logging
logger = logging.getLogger(__name__)

class SSHException(Exception):
    """Exception raised for SSH connection and command execution errors."""
    pass


@dataclass
class SSHResult:
    """Class to store SSH command execution results."""
    success: bool
    exit_code: int
    output: str
    error: str


class SSHClient:
    """
    SSH client for connecting to Linux VMs and executing commands.
    Uses paramiko library for SSH operations.
    """
    
    def __init__(self):
        """Initialize SSH client."""
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connected = False
    
    def connect_with_password(self, hostname: str, username: str, password: str, port: int = 22) -> None:
        """
        Connect to a remote host using username and password.
        
        Args:
            hostname: The hostname or IP address to connect to
            username: The username to authenticate as
            password: The password for authentication
            port: The port to connect to (default: 22)
            
        Raises:
            SSHException: If connection fails
        """
        try:
            self.client.connect(
                hostname=hostname,
                username=username,
                password=password,
                port=port,
                timeout=10,
                allow_agent=False,
                look_for_keys=False
            )
            self.connected = True
        except Exception as e:
            self.connected = False
            raise SSHException(f"Failed to connect to {hostname}:{port} as {username}: {str(e)}")
    
    def connect_with_key(self, hostname: str, username: str, key_path: str, 
                         password: Optional[str] = None, port: int = 22) -> None:
        """
        Connect to a remote host using username and private key.
        
        Args:
            hostname: The hostname or IP address to connect to
            username: The username to authenticate as
            key_path: Path to the private key file
            password: Passphrase for the private key (if required)
            port: The port to connect to (default: 22)
            
        Raises:
            SSHException: If connection fails
        """
        try:
            key = paramiko.RSAKey.from_private_key_file(key_path, password=password) \
                if password else paramiko.RSAKey.from_private_key_file(key_path)
                
            self.client.connect(
                hostname=hostname,
                username=username,
                pkey=key,
                port=port,
                timeout=10,
                allow_agent=False,
                look_for_keys=False
            )
            self.connected = True
        except Exception as e:
            self.connected = False
            raise SSHException(f"Failed to connect to {hostname}:{port} as {username} using key: {str(e)}")
    
    def execute_command(self, command: str, timeout: int = 60) -> SSHResult:
        """
        Execute a command on the remote host.
        
        Args:
            command: The command to execute
            timeout: Command execution timeout in seconds (default: 60)
            
        Returns:
            SSHResult: Result of the command execution
            
        Raises:
            SSHException: If not connected or command execution fails
        """
        if not self.connected:
            raise SSHException("Not connected to any host")
        
        try:
            # Execute command
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            
            # Get command output
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8', errors='replace')
            error = stderr.read().decode('utf-8', errors='replace')
            
            return SSHResult(
                success=(exit_code == 0),
                exit_code=exit_code,
                output=output,
                error=error
            )
        except Exception as e:
            raise SSHException(f"Failed to execute command '{command}': {str(e)}")
    
    def execute_sudo_command(self, command: str, password: str, timeout: int = 60) -> SSHResult:
        """
        Execute a sudo command on the remote host.
        
        Args:
            command: The command to execute (without sudo prefix)
            password: The sudo password
            timeout: Command execution timeout in seconds (default: 60)
            
        Returns:
            SSHResult: Result of the command execution
            
        Raises:
            SSHException: If not connected or command execution fails
        """
        if not self.connected:
            raise SSHException("Not connected to any host")
        
        sudo_command = f"sudo -S {command}"
        
        try:
            # Execute command
            stdin, stdout, stderr = self.client.exec_command(sudo_command, timeout=timeout)
            
            # Send password
            stdin.write(f"{password}\n")
            stdin.flush()
            
            # Get command output
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8', errors='replace')
            error = stderr.read().decode('utf-8', errors='replace')
            
            return SSHResult(
                success=(exit_code == 0),
                exit_code=exit_code,
                output=output,
                error=error
            )
        except Exception as e:
            raise SSHException(f"Failed to execute sudo command '{command}': {str(e)}")
    
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """
        Upload a file to the remote host.
        
        Args:
            local_path: Path to the local file
            remote_path: Path where to save the file on the remote host
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            SSHException: If not connected or file upload fails
        """
        if not self.connected:
            raise SSHException("Not connected to any host")
        
        try:
            sftp = self.client.open_sftp()
            sftp.put(local_path, remote_path)
            sftp.close()
            return True
        except Exception as e:
            raise SSHException(f"Failed to upload file from {local_path} to {remote_path}: {str(e)}")
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """
        Download a file from the remote host.
        
        Args:
            remote_path: Path to the file on the remote host
            local_path: Path where to save the file locally
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            SSHException: If not connected or file download fails
        """
        if not self.connected:
            raise SSHException("Not connected to any host")
        
        try:
            sftp = self.client.open_sftp()
            sftp.get(remote_path, local_path)
            sftp.close()
            return True
        except Exception as e:
            raise SSHException(f"Failed to download file from {remote_path} to {local_path}: {str(e)}")
    
    def close(self) -> None:
        """Close the SSH connection."""
        if self.connected:
            self.client.close()
            self.connected = False
