#!/usr/bin/env python3
"""
Azure Authentication Module

This module provides authentication utilities for Azure operations,
supporting multiple authentication methods including service principal,
managed identity, and interactive login.
"""
import os
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.core.exceptions import ClientAuthenticationError

# Configure logging
logger = logging.getLogger(__name__)

def get_azure_config() -> Dict[str, Any]:
    """
    Load Azure configuration from config file
    
    Returns:
        Dict[str, Any]: Azure configuration
    """
    try:
        config_path = Path(__file__).parent.parent.parent / "config" / "azure_config.json"
        if not config_path.exists():
            logger.warning(f"Azure config file not found at {config_path}")
            return {}
            
        with open(config_path, "r") as f:
            config = json.load(f)
            
        return config
    except Exception as e:
        logger.error(f"Error loading Azure config: {e}")
        return {}

def get_azure_credential(tenant_id: Optional[str] = None) -> Any:
    """
    Get Azure credential for authentication
    
    Args:
        tenant_id (str, optional): Azure tenant ID. Defaults to None.
        
    Returns:
        Any: Azure credential object
    """
    try:
        # First try to get credentials from config
        config = get_azure_config()
        
        # If tenant_id is not provided, try to get it from config
        if not tenant_id and "tenant_id" in config:
            tenant_id = config.get("tenant_id")
            
        # If service principal credentials are available, use them
        if all(k in config for k in ["client_id", "client_secret"]) and tenant_id:
            client_id = config.get("client_id")
            client_secret = config.get("client_secret")
            
            logger.info(f"Using service principal authentication for Azure")
            return ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        
        # Otherwise, use DefaultAzureCredential which tries multiple authentication methods
        logger.info(f"Using default authentication for Azure")
        return DefaultAzureCredential(tenant_id=tenant_id)
    except Exception as e:
        logger.error(f"Error getting Azure credential: {e}")
        raise

def get_subscription_id(subscription_id: Optional[str] = None) -> str:
    """
    Get Azure subscription ID
    
    Args:
        subscription_id (str, optional): Subscription ID. Defaults to None.
        
    Returns:
        str: Subscription ID
    """
    if subscription_id:
        return subscription_id
        
    # Try to get subscription ID from config
    config = get_azure_config()
    if "subscription_id" in config:
        return config.get("subscription_id")
        
    # Try to get subscription ID from environment variable
    if "AZURE_SUBSCRIPTION_ID" in os.environ:
        return os.environ.get("AZURE_SUBSCRIPTION_ID")
        
    raise ValueError("Subscription ID not provided and not found in config or environment")

def get_resource_group(sid: Optional[str] = None, resource_group: Optional[str] = None) -> str:
    """
    Get Azure resource group name
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        resource_group (str, optional): Resource group name. Defaults to None.
        
    Returns:
        str: Resource group name
    """
    if resource_group:
        return resource_group
        
    # If SID is provided, try to get resource group from config
    if sid:
        config = get_azure_config()
        systems = config.get("systems", {})
        
        if sid in systems and "resource_group" in systems[sid]:
            return systems[sid]["resource_group"]
            
    # Try to get default resource group from config
    config = get_azure_config()
    if "default_resource_group" in config:
        return config.get("default_resource_group")
        
    raise ValueError("Resource group not provided and not found in config")

def get_vm_name(sid: Optional[str] = None, vm_name: Optional[str] = None, component: Optional[str] = None) -> str:
    """
    Get Azure VM name
    
    Args:
        sid (str, optional): SAP System ID. Defaults to None.
        vm_name (str, optional): VM name. Defaults to None.
        component (str, optional): Component name (e.g., "db", "app"). Defaults to None.
        
    Returns:
        str: VM name
    """
    if vm_name:
        return vm_name
        
    # If SID is provided, try to get VM name from config
    if sid:
        config = get_azure_config()
        systems = config.get("systems", {})
        
        if sid in systems:
            system_config = systems[sid]
            
            # If component is provided, try to get VM name from component config
            if component and "components" in system_config and component in system_config["components"]:
                component_config = system_config["components"][component]
                if "vm_name" in component_config:
                    return component_config["vm_name"]
            
            # If no component or component not found, try to get default VM name
            if "vm_name" in system_config:
                return system_config["vm_name"]
                
    raise ValueError("VM name not provided and not found in config")

def test_azure_auth() -> Dict[str, Any]:
    """
    Test Azure authentication
    
    Returns:
        Dict[str, Any]: Test result
    """
    try:
        credential = get_azure_credential()
        subscription_id = get_subscription_id()
        
        # Try to get a token to verify authentication
        token = credential.get_token("https://management.azure.com/.default")
        
        return {
            "status": "success",
            "message": "Azure authentication successful",
            "subscription_id": subscription_id
        }
    except ClientAuthenticationError as e:
        logger.error(f"Azure authentication error: {e}")
        return {
            "status": "error",
            "message": f"Azure authentication error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error testing Azure authentication: {e}")
        return {
            "status": "error",
            "message": f"Error testing Azure authentication: {str(e)}"
        }
