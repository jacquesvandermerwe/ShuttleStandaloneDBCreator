#!/bin/bash

# JBang Excel Processor Environment Setup Script
# This script sets up the environment to run the ExcelProcessor JBang script

set -e

echo "🚀 Setting up JBang Excel Processor environment..."
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check if running on supported OS
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
    elif [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
        OS="windows"
    else
        OS="unknown"
    fi
    print_info "Detected OS: $OS"
}

# Check Java installation and version
check_java() {
    print_info "Checking Java installation..."
    
    if command -v java &> /dev/null; then
        JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')
        
        if [ "$JAVA_VERSION" -ge 21 ]; then
            print_status "Java $JAVA_VERSION found - Virtual threads supported"
            return 0
        else
            print_warning "Java $JAVA_VERSION found - requires Java 21+ for Virtual threads"
            print_info "Script will still work but with limited concurrency"
        fi
    else
        print_error "Java not found"
        print_info "Please install Java 21+ from:"
        print_info "  • Oracle: https://www.oracle.com/java/technologies/downloads/"
        print_info "  • OpenJDK: https://openjdk.org/"
        print_info "  • Homebrew (macOS): brew install openjdk@21"
        print_info "  • Package manager (Linux): apt install openjdk-21-jdk"
        return 1
    fi
}

# Check if JBang is installed
check_jbang() {
    print_info "Checking JBang installation..."
    
    if command -v jbang &> /dev/null; then
        JBANG_VERSION=$(jbang version 2>/dev/null | head -n 1 || echo "unknown")
        print_status "JBang found: $JBANG_VERSION"
        return 0
    else
        print_warning "JBang not found"
        return 1
    fi
}

# Install JBang based on OS
install_jbang() {
    print_info "Installing JBang..."
    
    case $OS in
        "macos")
            if command -v brew &> /dev/null; then
                print_info "Installing JBang via Homebrew..."
                brew install jbang
            else
                print_info "Installing JBang via curl..."
                curl -Ls https://sh.jbang.dev | bash -s - app setup
                export PATH="$HOME/.jbang/bin:$PATH"
                echo 'export PATH="$HOME/.jbang/bin:$PATH"' >> ~/.zshrc
                echo 'export PATH="$HOME/.jbang/bin:$PATH"' >> ~/.bashrc
            fi
            ;;
        "linux")
            if command -v curl &> /dev/null; then
                print_info "Installing JBang via curl..."
                curl -Ls https://sh.jbang.dev | bash -s - app setup
                export PATH="$HOME/.jbang/bin:$PATH"
                echo 'export PATH="$HOME/.jbang/bin:$PATH"' >> ~/.bashrc
            else
                print_error "curl not found. Please install curl first."
                return 1
            fi
            ;;
        "windows")
            print_info "For Windows, please install JBang manually:"
            print_info "  • Chocolatey: choco install jbang"
            print_info "  • Scoop: scoop install jbang"
            print_info "  • Manual: Download from https://github.com/jbangdev/jbang/releases"
            return 1
            ;;
        *)
            print_error "Unsupported OS. Please install JBang manually."
            print_info "Visit: https://www.jbang.dev/download/"
            return 1
            ;;
    esac
}

# Verify required Java files exist
check_java_files() {
    print_info "Checking for required Java files..."
    
    local missing_files=()
    
    if [ ! -f "ExcelProcessor.java" ]; then
        missing_files+=("ExcelProcessor.java")
    else
        print_status "ExcelProcessor.java found"
    fi
    
    if [ ! -f "ExcelDataExtractor.java" ]; then
        missing_files+=("ExcelDataExtractor.java")
    else
        print_status "ExcelDataExtractor.java found"
    fi
    
    if [ ! -f "InspectColumnsStreaming.java" ]; then
        missing_files+=("InspectColumnsStreaming.java")
    else
        print_status "InspectColumnsStreaming.java found"
    fi
    
    if [ ${#missing_files[@]} -gt 0 ]; then
        print_error "Missing Java files: ${missing_files[*]}"
        print_info "Please ensure you're in the correct directory"
        return 1
    fi
    
    return 0
}

# Test JBang with all scripts
test_jbang_scripts() {
    print_info "Testing JBang script compilation..."
    
    local failed_scripts=()
    
    if jbang --dry-run ExcelProcessor.java &> /dev/null; then
        print_status "ExcelProcessor.java validation successful"
    else
        failed_scripts+=("ExcelProcessor.java")
    fi
    
    if jbang --dry-run ExcelDataExtractor.java &> /dev/null; then
        print_status "ExcelDataExtractor.java validation successful"
    else
        failed_scripts+=("ExcelDataExtractor.java")
    fi
    
    if jbang --dry-run InspectColumnsStreaming.java &> /dev/null; then
        print_status "InspectColumnsStreaming.java validation successful"
    else
        failed_scripts+=("InspectColumnsStreaming.java")
    fi
    
    if [ ${#failed_scripts[@]} -gt 0 ]; then
        print_error "JBang validation failed for: ${failed_scripts[*]}"
        print_info "Check the scripts for syntax errors"
        return 1
    fi
    
    return 0
}

# Verify runner scripts exist
check_runner_scripts() {
    print_info "Checking for runner scripts..."
    
    local script_count=0
    
    if [ -f "run.sh" ]; then
        print_status "run.sh found"
        if [ ! -x "run.sh" ]; then
            chmod +x run.sh
            print_status "Made run.sh executable"
        fi
        script_count=$((script_count + 1))
    else
        print_warning "run.sh not found"
    fi
    
    if [ -f "run-extractor.sh" ]; then
        print_status "run-extractor.sh found"
        if [ ! -x "run-extractor.sh" ]; then
            chmod +x run-extractor.sh
            print_status "Made run-extractor.sh executable"
        fi
        script_count=$((script_count + 1))
    else
        print_warning "run-extractor.sh not found"
    fi
    
    if [ $script_count -eq 0 ]; then
        print_warning "No runner scripts found - you'll need to use direct JBang commands"
    else
        print_status "Found $script_count runner script(s)"
    fi
}

# Main setup function
main() {
    echo "======================================"
    echo "  JBang Excel Processor Setup"
    echo "======================================"
    echo
    
    detect_os
    echo
    
    # Check Java
    if ! check_java; then
        exit 1
    fi
    echo
    
    # Check and install JBang if needed
    if ! check_jbang; then
        echo
        read -p "Install JBang automatically? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if ! install_jbang; then
                exit 1
            fi
            echo
            # Reload PATH
            export PATH="$HOME/.jbang/bin:$PATH"
            
            # Verify installation
            if ! check_jbang; then
                print_error "JBang installation failed"
                exit 1
            fi
        else
            print_info "Please install JBang manually and re-run this script"
            exit 1
        fi
    fi
    echo
    
    # Check Java files
    if ! check_java_files; then
        exit 1
    fi
    echo
    
    # Test the scripts
    if ! test_jbang_scripts; then
        print_warning "Some script validations failed, but setup will continue"
    fi
    echo
    
    # Check runner scripts
    check_runner_scripts
    echo
    
    print_status "Environment setup complete!"
    echo
    print_info "Usage examples:"
    print_info "  • Pattern extraction: ./run.sh [directory]"
    print_info "  • Comprehensive extraction: ./run-extractor.sh [directory]"
    print_info "  • File inspection: jbang InspectColumnsStreaming.java [excel_file]"
    print_info "  • Direct JBang usage: jbang ExcelProcessor.java [directory]"
    echo
    print_info "Available tools:"
    print_info "  • ExcelProcessor.java - Extract specific patterns (folder/claims)"
    print_info "  • ExcelDataExtractor.java - Extract all data by categories"
    print_info "  • InspectColumnsStreaming.java - Examine file structure"
    echo
    print_info "For more information, see README.md"
}

# Run main function
main "$@"