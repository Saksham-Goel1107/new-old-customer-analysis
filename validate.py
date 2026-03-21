#!/usr/bin/env python3
"""
Validation script to check configuration before deployment
"""

import os
import sys
import json
from pathlib import Path

def check_file_exists(filepath, name):
    """Check if a file exists"""
    if Path(filepath).exists():
        print(f"✅ {name}: {filepath}")
        return True
    else:
        print(f"❌ {name}: {filepath} (NOT FOUND)")
        return False

def check_env_file():
    """Check .env file"""
    print("\n📋 Checking .env file...")
    
    if not Path('.env').exists():
        print("❌ .env file not found")
        print("   Create it with: cp .env.example .env")
        return False
    
    env_vars = {}
    with open('.env', 'r') as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    env_vars[key.strip()] = value.strip()
    
    required = ['INPUT_SHEET_ID', 'OUTPUT_SHEET_ID']
    missing = []
    
    for var in required:
        value = env_vars.get(var, '').strip()
        if not value or value.startswith('your-'):
            missing.append(var)
            print(f"❌ {var}: Not configured")
        else:
            print(f"✅ {var}: Configured")
    
    return len(missing) == 0

def check_credentials():
    """Check Google credentials file"""
    print("\n🔐 Checking credentials file...")
    
    creds_file = '.env'
    creds_path = None
    
    # Try to get path from .env
    if Path('.env').exists():
        with open('.env', 'r') as f:
            for line in f:
                if 'GOOGLE_CREDENTIALS_FILE' in line and '=' in line:
                    creds_path = line.split('=', 1)[1].strip()
                    break
    
    if not creds_path:
        creds_path = 'credentials.json'
    
    # Check if file exists
    if not Path(creds_path).exists():
        print(f"❌ Credentials file not found: {creds_path}")
        print("   Download from Google Cloud Console and save as credentials.json")
        return False
    
    print(f"✅ Credentials file found: {creds_path}")
    
    # Validate JSON
    try:
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        
        # Check for required fields
        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        missing = [f for f in required_fields if f not in creds]
        
        if missing:
            print(f"❌ Missing fields in credentials: {missing}")
            return False
        
        print(f"✅ Credentials JSON is valid")
        print(f"   Project: {creds.get('project_id', 'unknown')}")
        print(f"   Email: {creds.get('client_email', 'unknown')}")
        return True
        
    except json.JSONDecodeError:
        print("❌ Credentials file is not valid JSON")
        return False
    except Exception as e:
        print(f"❌ Error reading credentials: {e}")
        return False

def check_python_packages():
    """Check if required Python packages can be imported"""
    print("\n📦 Checking Python packages...")
    
    packages = [
        'pandas',
        'numpy',
        'gspread',
        'google.auth',
        'apscheduler',
    ]
    
    all_ok = True
    for package in packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} (not installed)")
            all_ok = False
    
    if not all_ok:
        print("\n   Run: pip install -r requirements.txt")
    
    return all_ok

def check_docker():
    """Check if Docker is available"""
    print("\n🐳 Checking Docker...")
    
    import subprocess
    
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker: {result.stdout.strip()}")
        else:
            print("❌ Docker not found")
            return False
    except FileNotFoundError:
        print("❌ Docker is not installed")
        return False
    
    try:
        result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker Compose: {result.stdout.strip()}")
        else:
            print("❌ Docker Compose not found")
            return False
    except FileNotFoundError:
        print("❌ Docker Compose is not installed")
        return False
    
    return True

def check_required_files():
    """Check if all required files exist"""
    print("\n📄 Checking required files...")
    
    files = [
        ('cohort_analysis.py', 'Main analysis module'),
        ('app.py', 'Application entry point'),
        ('Dockerfile', 'Docker configuration'),
        ('docker-compose.yml', 'Docker Compose configuration'),
        ('requirements.txt', 'Python dependencies'),
        ('.env.example', 'Environment template'),
    ]
    
    all_ok = True
    for filepath, name in files:
        if not check_file_exists(filepath, name):
            all_ok = False
    
    return all_ok

def main():
    """Run all checks"""
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  Cohort Analysis - Pre-Deployment Validation                      ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    results = []
    
    # Run checks
    results.append(("Required Files", check_required_files()))
    results.append((".env Configuration", check_env_file()))
    results.append(("Google Credentials", check_credentials()))
    results.append(("Python Packages", check_python_packages()))
    results.append(("Docker Installation", check_docker()))
    
    # Summary
    print("\n" + "="*70)
    print("📊 VALIDATION SUMMARY")
    print("="*70)
    
    all_passed = True
    for check, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{check:.<50} {status}")
        if not result:
            all_passed = False
    
    print("="*70)
    
    if all_passed:
        print("\n✅ All checks passed! Ready to deploy.")
        print("\nNext steps:")
        print("  1. Windows: double-click deploy.bat")
        print("  2. Linux/Mac: chmod +x deploy.sh && ./deploy.sh")
        print("\nOr manually start with:")
        print("  docker-compose up -d")
        return 0
    else:
        print("\n❌ Some checks failed. Fix the issues above and try again.")
        print("\nCommon fixes:")
        print("  • Missing .env: cp .env.example .env")
        print("  • Need credentials: Download from Google Cloud Console")
        print("  • No Python packages: pip install -r requirements.txt")
        print("  • Docker not installed: Visit https://www.docker.com/products/docker-desktop")
        return 1

if __name__ == '__main__':
    sys.exit(main())
