#!/usr/bin/env python3
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

"""
Azure Authentication Module

This module provides authentication utilities for Azure operations,
supporting multiple authentication methods including service principal,
managed identity, and interactive login.
"""
import os
import json
import logging
import importlib
import inspect
from typing import Dict, Any, Optional, Callable
from pathlib import Path

from azure.identity import (
    DefaultAzureCredential, 
    ClientSecretCredential, 
    ManagedIdentityCredential, 
    AzureCliCredential
)
from azure.core.exceptions import ClientAuthenticationError

# Configure logging
logger = logging.getLogger(__name__)

# Check if ClientAssertionCredential is available
try:
    from azure.identity import ClientAssertionCredential
    HAS_CLIENT_ASSERTION_CREDENTIAL = True
    
    # Check the signature to determine if it needs a callback function
    sig = inspect.signature(ClientAssertionCredential.__init__)
    param_names = [p.name for p in list(sig.parameters.values())]
    
    # Newer versions (>=1.12) have 'func' as parameter name instead of 'client_assertion'
    NEEDS_CALLBACK = 'func' in param_names
    PARAM_NAME = 'func' if NEEDS_CALLBACK else 'client_assertion'
    
    logger.info(f"Successfully detected ClientAssertionCredential API: " +
               f"Parameter name is '{PARAM_NAME}'")
except (ImportError, AttributeError) as e:
    HAS_CLIENT_ASSERTION_CREDENTIAL = False
    NEEDS_CALLBACK = False
    PARAM_NAME = None
    logger.warning(f"ClientAssertionCredential not available or could not determine API: {e}")

def get_azure_config() -> Dict[str, Any]:
    """
    Load Azure configuration from config file and supplement with Key Vault secrets
    
    Returns:
        Dict[str, Any]: Azure configuration
    """
    try:
        # Load base configuration from file
        config_path = Path(__file__).parent.parent.parent / "config" / "azure_config.json"
        if not config_path.exists():
            logger.warning(f"Azure config file not found at {config_path}")
            return {}
            
        with open(config_path, "r") as f:
            config = json.load(f)
        
        # If Key Vault details are provided, fetch secrets
        if "key_vault" in config and "url" in config["key_vault"]:
            key_vault_url = config["key_vault"]["url"]
            
            # Get Azure credentials from environment variables
            tenant_id = os.getenv("AZURE_TENANT_ID")
            client_id = os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("AZURE_CLIENT_SECRET")
            
            if not all([tenant_id, client_id, client_secret]):
                logger.error("Missing Azure credentials in environment variables")
                raise ValueError("No valid authentication method found")
                
            # Use ClientSecretCredential specifically for Key Vault access
            logger.info("Using environment variables for Key Vault authentication")
            kv_credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            
            # Create SecretClient with the proper credential
            secret_client = SecretClient(vault_url=key_vault_url, credential=kv_credential)
            
            # List of secret names to fetch (non-Azure secrets)
            secret_names = config["key_vault"].get("secrets", [
                "database_password",  # Example of non-Azure secret
                "api_key",          # Another example
                "other_secret"      # More examples
            ])
            
            # Fetch secrets from Key Vault
            for secret_name in secret_names:
                try:
                    secret = secret_client.get_secret(secret_name)
                    
                    # Map secret names to config keys
                    config[secret_name] = secret.value
                    logger.info(f"Retrieved secret {secret_name} from Key Vault")
                except Exception as e:
                    logger.error(f"Error retrieving secret {secret_name}: {e}")
        
        # If essential Azure credentials are missing from config, try to get them from environment variables
        if "subscription_id" not in config or "tenant_id" not in config or "client_id" not in config or "client_secret" not in config:
            env_creds = get_env_credentials()
            for key, value in env_creds.items():
                if key not in config or not config[key]:
                    config[key] = value
                    logger.info(f"Retrieved {key} from environment variables")
            
        return config
    except Exception as e:
        logger.error(f"Error loading Azure config: {e}")
        return {}

def get_env_credentials() -> Dict[str, Any]:
    """
    Get Azure credentials from environment variables as a fallback
    
    Returns:
        Dict[str, Any]: Azure credentials from environment
    """
    creds = {}
    
    # Check for basic Azure credentials in environment
    if os.environ.get("AZURE_SUBSCRIPTION_ID"):
        creds["subscription_id"] = os.environ.get("AZURE_SUBSCRIPTION_ID")
    if os.environ.get("AZURE_TENANT_ID"):
        creds["tenant_id"] = os.environ.get("AZURE_TENANT_ID")
    if os.environ.get("AZURE_CLIENT_ID"):
        creds["client_id"] = os.environ.get("AZURE_CLIENT_ID")
    if os.environ.get("AZURE_CLIENT_SECRET"):
        creds["client_secret"] = os.environ.get("AZURE_CLIENT_SECRET")
        
    return creds

def get_azure_credential(tenant_id: Optional[str] = None, use_cli: bool = False) -> Any:
    """
    Get Azure credential for authentication
    
    Args:
        tenant_id (str, optional): Azure tenant ID. Defaults to None.
        use_cli (bool, optional): Force use of Azure CLI for authentication. Defaults to False.
        
    Returns:
        Any: Azure credential object
    """
    try:
        # If use_cli is True, prioritize Azure CLI auth immediately
        if use_cli:
            logger.info("Using Azure CLI authentication for Azure (explicitly requested)")
            try:
                return AzureCliCredential()
            except Exception as e:
                logger.warning(f"Failed to use Azure CLI (explicitly requested): {e}")
                # Fall through to other authentication methods
        
        # First try to get credentials from config
        config = get_azure_config()
        
        # If tenant_id is not provided, try to get it from config
        if not tenant_id and "tenant_id" in config:
            tenant_id = config.get("tenant_id")
        
        if not tenant_id:
            tenant_id = os.environ.get("AZURE_TENANT_ID")
         
        # Check if Azure CLI is available - do this early as a reliable fallback
        # This happens before any other authentication method except explicit CLI request
        azure_cli_available = False
        if not use_cli:  # Only try if we didn't already try above
            logger.info(f"Checking if Azure CLI authentication is available")
            try:
                cli_cred = AzureCliCredential()
                # Test if CLI is actually authenticated
                token = cli_cred.get_token("https://management.azure.com/.default")
                if token:
                    logger.info(f"Azure CLI authentication is available and will be used as fallback")
                    azure_cli_available = True
                    # We don't return here, we'll try other methods first
            except Exception as e:
                logger.warning(f"Azure CLI authentication is not available: {e}")
                # Fall through to other methods
        
        # Check for workload identity federation (federated credentials)
        client_id = os.environ.get("AZURE_CLIENT_ID") or config.get("client_id") 
        
        if HAS_CLIENT_ASSERTION_CREDENTIAL and tenant_id and client_id:
            # 1. Check for token file path
            if os.environ.get("AZURE_FEDERATED_TOKEN_FILE"):
                token_file = os.environ.get("AZURE_FEDERATED_TOKEN_FILE")
                logger.info(f"Using workload identity federation with token file for Azure: {token_file}")
                
                try:
                    # Read token from file
                    with open(token_file, "r") as f:
                        token = f.read().strip()
                    
                    if NEEDS_CALLBACK:
                        # For newer versions that require a callback function
                        def get_token(*args, **kwargs) -> str:
                            return token
                            
                        # Create kwargs dynamically based on parameter name
                        kwargs = {
                            'tenant_id': tenant_id,
                            'client_id': client_id,
                            'func': get_token
                        }
                    else:
                        # For older versions that accept the token directly
                        kwargs = {
                            'tenant_id': tenant_id,
                            'client_id': client_id,
                            'client_assertion': token
                        }
                    
                    return ClientAssertionCredential(**kwargs)
                except Exception as e:
                    logger.warning(f"Failed to use federated token file for authentication: {e}")

            # 2. Check for direct client assertion token
            if os.environ.get("AZURE_CLIENT_ASSERTION"):
                logger.info("Using workload identity federation with direct token for Azure")
                token = os.environ.get("AZURE_CLIENT_ASSERTION")
                
                try:
                    if NEEDS_CALLBACK:
                        # For newer versions that require a callback function
                        def get_token(*args, **kwargs) -> str:
                            return token
                            
                        # Create kwargs dynamically based on parameter name
                        kwargs = {
                            'tenant_id': tenant_id,
                            'client_id': client_id,
                            'func': get_token
                        }
                    else:
                        # For older versions that accept the token directly
                        kwargs = {
                            'tenant_id': tenant_id,
                            'client_id': client_id,
                            'client_assertion': token
                        }
                    
                    return ClientAssertionCredential(**kwargs)
                except Exception as e:
                    logger.warning(f"Failed to use client assertion for authentication: {e}")

            # 3. Check for client assertion callback function
            callback_file = os.environ.get("AZURE_CLIENT_ASSERTION_CALLBACK_SCRIPT")
            if callback_file and os.path.exists(callback_file):
                logger.info("Using workload identity federation with callback script for Azure")
                
                try:
                    # Import the callback module
                    import importlib.util
                    spec = importlib.util.spec_from_file_location("assertion_callback", callback_file)
                    callback_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(callback_module)
                    
                    # Get the get_token function from the module
                    if hasattr(callback_module, "get_token"):
                        if NEEDS_CALLBACK:
                            kwargs = {
                                'tenant_id': tenant_id,
                                'client_id': client_id,
                                'func': callback_module.get_token
                            }
                        else:
                            # For older versions, we need to call the function to get the token
                            token = callback_module.get_token()
                            kwargs = {
                                'tenant_id': tenant_id,
                                'client_id': client_id,
                                'client_assertion': token
                            }
                        
                        return ClientAssertionCredential(**kwargs)
                except Exception as e:
                    logger.warning(f"Failed to use client assertion callback for authentication: {e}")
        
        # If Azure CLI was available, use it now as our first fallback
        if azure_cli_available:
            logger.info(f"Falling back to Azure CLI authentication")
            return AzureCliCredential()
        
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
        
        # If AZURE_CLIENT_SECRET is set in environment variables
        if os.environ.get("AZURE_CLIENT_SECRET") and client_id and tenant_id:
            client_secret = os.environ.get("AZURE_CLIENT_SECRET")
            logger.info(f"Using service principal authentication from environment variables for Azure")
            return ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        
        # If only client_id is available but no client_secret, try managed identity
        if client_id and tenant_id:
            logger.info(f"Using managed identity authentication for Azure with client_id: {client_id}")
            try:
                return ManagedIdentityCredential(client_id=client_id)
            except Exception as e:
                logger.warning(f"Failed to use managed identity with client_id: {e}")
                # Fall through to DefaultAzureCredential
        
        # Finally, use DefaultAzureCredential which tries multiple authentication methods
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