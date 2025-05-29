#!/usr/bin/env python3
"""
SAP Inventory Checks

This module provides functions for checking SAP deployments against best practices.
Based on the SAP on Azure inventory checks workbook.
"""
from tools.sap_inventory.vm_compliance import check_vm_compliance
from tools.sap_inventory.inventory_summary import get_sap_inventory_summary
from tools.sap_inventory.quality_check import run_quality_check, get_quality_check_definitions
