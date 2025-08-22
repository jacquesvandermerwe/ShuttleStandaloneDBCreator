#!/bin/bash

# Overview Extractor Runner Script
# Usage: ./run-overview.sh [directory_path]

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if TransferOverviewExtractor.java exists
check_overview_extractor() {
    if [ ! -f "TransferOverviewExtractor.java" ]; then
        print_error "TransferOverviewExtractor.java not found in current directory"
        print_info "Please ensure you're in the correct directory"
        exit 1
    fi
}

# Check if JBang is available
check_jbang() {
    if ! command -v jbang &> /dev/null; then
        print_error "JBang not found"
        print_info "Please install JBang first by running: ./setup.sh"
        print_info "Or install manually from: https://www.jbang.dev/download/"
        exit 1
    fi
}

# Show usage information
show_usage() {
    echo "Overview Extractor - Extract Overview sheets to separate Excel files"
    echo
    echo "Usage: $0 [directory_path]"
    echo
    echo "Arguments:"
    echo "  directory_path    Directory containing Excel files (default: current directory)"
    echo
    echo "Examples:"
    echo "  $0                     # Process Excel files in current directory"
    echo "  $0 ~/Documents/reports # Process Excel files in ~/Documents/reports"
    echo "  $0 /path/to/excel      # Process Excel files in /path/to/excel"
    echo
    echo "Output Files:"
    echo "  • Transfer-Overview-[filename].xlsx - Excel file containing only Overview sheet"
    echo "  • Transfer-Overview-[filename].xls  - Excel file containing only Overview sheet"
    echo
    echo "Features:"
    echo "  • Extracts Overview sheets from Excel files"
    echo "  • Creates new Excel files with only the Overview sheet"
    echo "  • Preserves original formatting and data types"
    echo "  • Memory-efficient processing for large files"
    echo "  • Skips files that don't contain Overview sheets"
    echo
}

# Main function
main() {
    # Parse command line arguments
    if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
        show_usage
        exit 0
    fi
    
    DIRECTORY=${1:-.}
    
    # Validate directory
    if [ ! -d "$DIRECTORY" ]; then
        print_error "Directory does not exist: $DIRECTORY"
        exit 1
    fi
    
    # Convert to absolute path
    DIRECTORY=$(cd "$DIRECTORY" && pwd)
    
    echo "======================================"
    echo "       Overview Extractor Runner"
    echo "======================================"
    echo
    
    # Pre-flight checks
    print_info "Running pre-flight checks..."
    check_overview_extractor
    check_jbang
    print_success "Pre-flight checks passed"
    echo
    
    # Show configuration
    print_info "Configuration:"
    echo "  📁 Target directory: $DIRECTORY"
    echo "  📊 Script: TransferOverviewExtractor.java"
    echo "  🧵 Processing: Sequential (one file at a time)"
    echo "  📋 Output format: Excel files (.xlsx/.xls)"
    echo
    
    # Count Excel files in directory only (not subdirectories, excluding temporary files and already processed files)
    EXCEL_COUNT=$(find "$DIRECTORY" -maxdepth 1 \( -name "*.xlsx" -o -name "*.xls" \) ! -name "~*" ! -name "Transfer-Overview-*" ! -name "Overview-*" 2>/dev/null | wc -l | tr -d ' ')
    
    if [ "$EXCEL_COUNT" -eq 0 ]; then
        print_warning "No Excel files found in: $DIRECTORY"
        print_info "Looking for files with extensions: .xlsx, .xls"
        print_info "Excluding temporary files (~*) and already processed files (Transfer-Overview-*)"
        exit 0
    fi
    
    print_info "Found $EXCEL_COUNT Excel file(s) to process"
    echo
    
    # Show extraction details
    print_info "Extraction Details:"
    echo "  📋 Target sheet: Overview (sheet named exactly 'Overview')"
    echo "  💾 Output format: New Excel files in organized reports/[filename]/Overview/ structure"
    echo "  📊 Data preservation: Original formatting and data types maintained"
    echo
    
    # Confirm execution
    read -p "Continue with Overview extraction? (Y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        print_info "Extraction cancelled"
        exit 0
    fi
    echo
    
    # Run the extractor
    print_info "Starting Overview Extractor..."
    echo "----------------------------------------"
    
    if jbang TransferOverviewExtractor.java "$DIRECTORY"; then
        echo "----------------------------------------"
        print_success "Overview extraction completed successfully!"
        echo
        
        # Show results
        print_info "Output files created:"
        
        # Check for organized Overview structure
        REPORT_DIR="$DIRECTORY/report"
        if [ -d "$REPORT_DIR" ]; then
            echo "  📊 Organized Overview Structure:"
            echo "     Report location: report/"
            echo ""
            
            # Find all Overview directories
            OVERVIEW_COUNT=0
            find "$REPORT_DIR" -mindepth 2 -maxdepth 2 -name "Overview" -type d | while read -r overview_dir; do
                REPORT_NAME=$(basename "$(dirname "$overview_dir")")
                echo "  📁 Report: $REPORT_NAME"
                
                # Count Overview files in this directory
                OVERVIEW_FILES=$(find "$overview_dir" -name "Overview-*.xlsx" -o -name "Overview-*.xls" -o -name "Overview-*.csv" 2>/dev/null | wc -l | tr -d ' ')
                echo "     📊 Overview files: $OVERVIEW_FILES"
                
                # List Overview files
                find "$overview_dir" \( -name "Overview-*.xlsx" -o -name "Overview-*.xls" -o -name "Overview-*.csv" \) 2>/dev/null | while read -r overview_file; do
                    FILE_NAME=$(basename "$overview_file")
                    FILE_EXT="${FILE_NAME##*.}"
                    if [ "$FILE_EXT" = "csv" ]; then
                        echo "     - $FILE_NAME (CSV format - large file)"
                    else
                        echo "     - $FILE_NAME (Excel format with charts/formatting)"
                    fi
                done
                echo ""
                OVERVIEW_COUNT=$((OVERVIEW_COUNT + OVERVIEW_FILES))
            done
            
            if [ "$OVERVIEW_COUNT" -eq 0 ]; then
                print_warning "No Overview files were created"
                print_info "This could mean:"
                print_info "  • No Excel files contained Overview sheets"
                print_info "  • The sheets may be named differently (must be exactly 'Overview')"
            fi
        else
            # Fallback to old flat structure search
            OVERVIEW_COUNT=$(find "$DIRECTORY" -name "Transfer-Overview-*.xlsx" -o -name "Transfer-Overview-*.xls" 2>/dev/null | wc -l | tr -d ' ')
            
            if [ "$OVERVIEW_COUNT" -gt 0 ]; then
                echo "  📊 Overview Excel files (flat structure): $OVERVIEW_COUNT"
                find "$DIRECTORY" \( -name "Transfer-Overview-*.xlsx" -o -name "Transfer-Overview-*.xls" \) 2>/dev/null | while read -r file; do
                    echo "     - $(basename "$file")"
                done
            else
                print_warning "No Overview files were created"
                print_info "This could mean:"
                print_info "  • No Excel files contained Overview sheets"
                print_info "  • The sheets may be named differently (must be exactly 'Overview')"
            fi
        fi
        
    else
        echo "----------------------------------------"
        print_error "Overview extraction failed!"
        print_info "Check the output above for error details"
        exit 1
    fi
}

# Handle Ctrl+C gracefully
trap 'echo; print_warning "Overview extraction interrupted by user"; exit 130' INT

# Run main function with all arguments
main "$@"