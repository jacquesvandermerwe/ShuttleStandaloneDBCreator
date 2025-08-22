#!/bin/bash

# Excel Data Extractor Runner Script
# Usage: ./run-extractor.sh [directory_path]

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

# Check if ExcelDataExtractor.java exists
check_excel_extractor() {
    if [ ! -f "ExcelDataExtractor.java" ]; then
        print_error "ExcelDataExtractor.java not found in current directory"
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
    echo "Excel Data Extractor - Extract comprehensive data from Excel Transfer Report sheets"
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
    echo "  • Folder-Object-[filename].csv - Rows where File Source Size = 0"
    echo "  • File-Object-[filename].csv   - Rows where File Source Size > 0"
    echo "  • Claims-[filename].csv        - Folder objects ending with /Claims/XXXX (no children)"
    echo "  • Customer-Folders-[filename].csv - Folder objects ending with Folders/XXX/ (no children)"
    echo "  • [STATUS]-Status-[filename].csv - Rows grouped by unique File Status values"
    echo
    echo "Features:"
    echo "  • Processes all rows from Transfer Report sheets"
    echo "  • Creates separate CSV files for folders, files, claims, customer folders, and status groups"
    echo "  • Pattern-based extraction for Claims and Customer Folders from folder objects"
    echo "  • Memory-efficient streaming processing for large files"
    echo "  • Automatic cleanup of empty CSV files"
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
    echo "     Excel Data Extractor Runner"
    echo "======================================"
    echo
    
    # Pre-flight checks
    print_info "Running pre-flight checks..."
    check_excel_extractor
    check_jbang
    print_success "Pre-flight checks passed"
    echo
    
    # Show configuration
    print_info "Configuration:"
    echo "  📁 Target directory: $DIRECTORY"
    echo "  📊 Script: ExcelDataExtractor.java"
    echo "  🧵 Processing: Sequential (one file at a time)"
    echo "  📋 Output types: Folder Objects, File Objects, Status Groups"
    echo
    
    # Count Excel files in directory only (not subdirectories, excluding temporary files starting with ~)
    EXCEL_COUNT=$(find "$DIRECTORY" -maxdepth 1 -name "*.xlsx" -o -name "*.xls" 2>/dev/null | grep -v '/~' | wc -l | tr -d ' ')
    
    if [ "$EXCEL_COUNT" -eq 0 ]; then
        print_warning "No Excel files found in: $DIRECTORY"
        print_info "Looking for files with extensions: .xlsx, .xls"
        exit 0
    fi
    
    print_info "Found $EXCEL_COUNT Excel file(s) to process"
    echo
    
    # Show extraction details
    print_info "Extraction Details:"
    echo "  📂 Folder Objects: Rows where File Source Size = 0"
    echo "  📋 File Objects: Rows where File Source Size > 0"
    echo "  💼 Claims: Folder objects ending with /Claims/XXXX (no children)"
    echo "  👥 Customer Folders: Folder objects ending with Folders/XXX/ (no children)"
    echo "  🏷️  Status Groups: Rows grouped by unique File Status values"
    echo
    
    # Confirm execution
    read -p "Continue with data extraction? (Y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        print_info "Extraction cancelled"
        exit 0
    fi
    echo
    
    # Run the extractor
    print_info "Starting Excel Data Extractor..."
    echo "----------------------------------------"
    
    if jbang ExcelDataExtractor.java "$DIRECTORY"; then
        echo "----------------------------------------"
        print_success "Data extraction completed successfully!"
        echo
        
        # Run SQLite import if FolderHierarchyImporter.java exists
        if [ -f "FolderHierarchyImporter.java" ]; then
            print_info "Running SQLite import..."
            echo "----------------------------------------"
            
            if jbang FolderHierarchyImporter.java "$DIRECTORY"; then
                echo "----------------------------------------"
                print_success "SQLite database import completed!"
                print_info "Database location: folder_hierarchy.db"
                echo
            else
                echo "----------------------------------------"
                print_warning "SQLite import failed - continuing without database import"
                echo
            fi
        else
            print_warning "FolderHierarchyImporter.java not found - skipping SQLite import"
            echo
        fi
        
        # Show results
        print_info "Output files created:"
        
        # Check for organized report structure
        REPORT_DIR="$DIRECTORY/report"
        if [ -d "$REPORT_DIR" ]; then
            echo "  📊 Organized Report Structure:"
            echo "     Report location: report/"
            echo ""
            
            # Find all report directories 
            find "$REPORT_DIR" -mindepth 1 -maxdepth 1 -type d | while read -r report_dir; do
                REPORT_NAME=$(basename "$report_dir")
                echo "  📁 Report: $REPORT_NAME"
                
                # Check for main files (Folder-Object, File-Object)
                FOLDER_FILES=$(find "$report_dir" -maxdepth 1 -name "Folder-Object-*.csv" 2>/dev/null | wc -l | tr -d ' ')
                FILE_FILES=$(find "$report_dir" -maxdepth 1 -name "File-Object-*.csv" 2>/dev/null | wc -l | tr -d ' ')
                
                [ "$FOLDER_FILES" -gt 0 ] && echo "     📂 Folder Objects: $FOLDER_FILES files"
                [ "$FILE_FILES" -gt 0 ] && echo "     📋 File Objects: $FILE_FILES files"
                
                # Check for Claims folder
                if [ -d "$report_dir/Claims" ]; then
                    CLAIMS_COUNT=$(find "$report_dir/Claims" -name "Claims-*.csv" 2>/dev/null | wc -l | tr -d ' ')
                    echo "     💼 Claims: $CLAIMS_COUNT files"
                fi
                
                # Check for Customer folder
                if [ -d "$report_dir/Customer" ]; then
                    CUSTOMER_COUNT=$(find "$report_dir/Customer" -name "Customer-Folders-*.csv" 2>/dev/null | wc -l | tr -d ' ')
                    echo "     👥 Customer Folders: $CUSTOMER_COUNT files"
                fi
                
                # Check for Status folder
                if [ -d "$report_dir/Status" ]; then
                    STATUS_COUNT=$(find "$report_dir/Status" -name "*-Status-*.csv" 2>/dev/null | wc -l | tr -d ' ')
                    echo "     🏷️  Status Groups: $STATUS_COUNT files"
                    
                    # List status types
                    find "$report_dir/Status" -name "*-Status-*.csv" 2>/dev/null | while read -r status_file; do
                        STATUS_TYPE=$(basename "$status_file" | sed 's/-Status-.*\.csv$//')
                        echo "        - $STATUS_TYPE"
                    done
                fi
                echo ""
            done
        else
            # Fallback to old flat structure search
            FOLDER_COUNT=$(find "$DIRECTORY" -name "Folder-Object-*.csv" 2>/dev/null | wc -l | tr -d ' ')
            FILE_COUNT=$(find "$DIRECTORY" -name "File-Object-*.csv" 2>/dev/null | wc -l | tr -d ' ')
            CLAIMS_COUNT=$(find "$DIRECTORY" -name "Claims-*.csv" 2>/dev/null | wc -l | tr -d ' ')
            CUSTOMER_FOLDERS_COUNT=$(find "$DIRECTORY" -name "Customer-Folders-*.csv" 2>/dev/null | wc -l | tr -d ' ')
            STATUS_COUNT=$(find "$DIRECTORY" -name "*-Status-*.csv" 2>/dev/null | wc -l | tr -d ' ')
            
            if [ "$FOLDER_COUNT" -gt 0 ] || [ "$FILE_COUNT" -gt 0 ] || [ "$CLAIMS_COUNT" -gt 0 ] || [ "$CUSTOMER_FOLDERS_COUNT" -gt 0 ] || [ "$STATUS_COUNT" -gt 0 ]; then
                echo "  📊 Found CSV files (flat structure):"
                [ "$FOLDER_COUNT" -gt 0 ] && echo "     📂 Folder Objects: $FOLDER_COUNT files"
                [ "$FILE_COUNT" -gt 0 ] && echo "     📋 File Objects: $FILE_COUNT files"
                [ "$CLAIMS_COUNT" -gt 0 ] && echo "     💼 Claims: $CLAIMS_COUNT files"
                [ "$CUSTOMER_FOLDERS_COUNT" -gt 0 ] && echo "     👥 Customer Folders: $CUSTOMER_FOLDERS_COUNT files"
                [ "$STATUS_COUNT" -gt 0 ] && echo "     🏷️  Status Groups: $STATUS_COUNT files"
            else
                print_warning "No CSV files were created (no matching data found)"
            fi
        fi
        
    else
        echo "----------------------------------------"
        print_error "Data extraction failed!"
        print_info "Check the output above for error details"
        exit 1
    fi
}

# Handle Ctrl+C gracefully
trap 'echo; print_warning "Data extraction interrupted by user"; exit 130' INT

# Run main function with all arguments
main "$@"