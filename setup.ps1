# AZSAP-MCP Quick Setup Script
# This script copies template configuration files to their target locations

Write-Host "AZSAP-MCP Configuration Setup" -ForegroundColor Green
Write-Host "==============================" -ForegroundColor Green
Write-Host ""

# Check if we're in the right directory
if (!(Test-Path "config\executor_config.template.json")) {
    Write-Host "Error: Please run this script from the azsap-mcp root directory" -ForegroundColor Red
    exit 1
}

# Copy template files
Write-Host "Copying configuration template files..." -ForegroundColor Yellow

# Executor configuration
if (!(Test-Path "config\executor_config.json")) {
    Copy-Item "config\executor_config.template.json" "config\executor_config.json"
    Write-Host "✅ Created config\executor_config.json" -ForegroundColor Green
} else {
    Write-Host "⚠️  config\executor_config.json already exists - skipping" -ForegroundColor Yellow
}

# Azure configuration
if (!(Test-Path "config\azure_config.json")) {
    Copy-Item "config\azure_config.template.json" "config\azure_config.json"
    Write-Host "✅ Created config\azure_config.json" -ForegroundColor Green
} else {
    Write-Host "⚠️  config\azure_config.json already exists - skipping" -ForegroundColor Yellow
}

# Authentication configuration
if (!(Test-Path "config\auth_config.json")) {
    Copy-Item "config\auth_config.template.json" "config\auth_config.json"
    Write-Host "✅ Created config\auth_config.json" -ForegroundColor Green
} else {
    Write-Host "⚠️  config\auth_config.json already exists - skipping" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Configuration files have been created!" -ForegroundColor Green
Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Cyan
Write-Host "1. Edit .env file with your HANA connection details"
Write-Host "2. Edit config\executor_config.json with your SAP system details"
Write-Host "3. Edit config\azure_config.json with your Azure subscription details"
Write-Host "4. Edit config\auth_config.json with your authentication preferences"
Write-Host ""
Write-Host "FILES TO CONFIGURE:" -ForegroundColor Cyan
Write-Host "📝 .env - HANA connection and Azure credentials"
Write-Host "📝 config\executor_config.json - SAP systems and SSH configuration"
Write-Host "📝 config\azure_config.json - Azure subscription and VM mappings"
Write-Host "📝 config\auth_config.json - User authentication and roles"
Write-Host ""
Write-Host "For detailed configuration instructions, see SETUP_GUIDE.md" -ForegroundColor Cyan
Write-Host ""
Write-Host "To test your configuration after editing:" -ForegroundColor Cyan
Write-Host "  python server.py --debug"
