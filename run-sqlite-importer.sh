#!/bin/bash

# SQLite Direct Importer Runner Script
# Usage: ./run-sqlite-importer.sh [directory_path] [database_name]

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

# Check if SQLiteDirectImporter.java exists
check_sqlite_importer() {
    if [ ! -f "SQLiteDirectImporter.java" ]; then
        print_error "SQLiteDirectImporter.java not found in current directory"
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
    echo "SQLite Direct Importer - Import Excel Transfer Report data directly into SQLite database"
    echo
    echo "Usage: $0 [directory_path] [database_name]"
    echo
    echo "Arguments:"
    echo "  directory_path    Directory containing Excel files (default: current directory)"
    echo "  database_name     Name of SQLite database file (default: transfer_reports.db)"
    echo
    echo "Examples:"
    echo "  $0                                    # Process source/ folder in current directory, create transfer_reports.db"
    echo "  $0 ~/Documents/reports                # Process source/ folder in ~/Documents/reports, create transfer_reports.db"
    echo "  $0 /path/to/project migration_data.db # Process source/ folder in /path/to/project, create migration_data.db"
    echo
    echo "Features:"
    echo "  • Direct Excel to SQLite import (no CSV intermediates)"
    echo "  • UPSERT statements for handling multiple Excel files"
    echo "  • Hierarchical folder structure calculation (parent_folder, parent_id, level)"
    echo "  • Dynamic status-based views (match-exists, filtered, success, etc.)"
    echo "  • Aggregation views for status counts, files vs folders"
    echo "  • Hierarchical query view for parent-child relationships"
    echo "  • Memory-efficient streaming processing for large files"
    echo "  • Automatic file management: source/ → processed/ after completion"
    echo
    echo "Database Views Created:"
    echo "  • files_view - All files (source_file_size > 0)"
    echo "  • folders_view - All folders (source_file_size = 0)"
    echo "  • status_summary - Status counts and breakdown"
    echo "  • hierarchy_children - Recursive parent-child view"
    echo "  • status_[name] - Individual views for each status type"
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
    DATABASE_NAME=${2:-transfer_reports.db}
    
    # Validate directory
    if [ ! -d "$DIRECTORY" ]; then
        print_error "Directory does not exist: $DIRECTORY"
        exit 1
    fi
    
    # Convert to absolute path
    DIRECTORY=$(cd "$DIRECTORY" && pwd)
    
    echo "======================================"
    echo "    SQLite Direct Importer Runner"
    echo "======================================"
    echo
    
    # Pre-flight checks
    print_info "Running pre-flight checks..."
    check_sqlite_importer
    check_jbang
    print_success "Pre-flight checks passed"
    echo
    
    # Show configuration
    print_info "Configuration:"
    echo "  📁 Base directory: $DIRECTORY"
    echo "  📥 Source folder: $DIRECTORY/source (Excel files to process)"
    echo "  📤 Processed folder: $DIRECTORY/processed (files moved after processing)"
    echo "  💾 Database file: $DIRECTORY/report/$DATABASE_NAME"
    echo "  📊 Script: SQLiteDirectImporter.java"
    echo "  🔄 Processing: Direct Excel to SQLite (no CSV files)"
    echo "  📋 UPSERT mode: Handles multiple files and updates"
    echo
    
    # Count Excel files in source directory only (not subdirectories, excluding temporary files)
    SOURCE_DIR="$DIRECTORY/source"
    if [ ! -d "$SOURCE_DIR" ]; then
        mkdir -p "$SOURCE_DIR"
        print_info "Created source directory: $SOURCE_DIR"
    fi
    
    EXCEL_COUNT=$(find "$SOURCE_DIR" -maxdepth 1 \( -name "*.xlsx" -o -name "*.xls" \) ! -name "~*" 2>/dev/null | wc -l | tr -d ' ')
    
    if [ "$EXCEL_COUNT" -eq 0 ]; then
        print_warning "No Excel files found in source directory: $SOURCE_DIR"
        print_info "Please place Excel files (.xlsx, .xls) in the source/ folder"
        print_info "Files will be moved to processed/ folder after processing"
        exit 0
    fi
    
    print_info "Found $EXCEL_COUNT Excel file(s) to process"
    echo
    
    # Show processing details
    print_info "Processing Details:"
    echo "  📋 Target sheets: Transfer Report* (all Transfer Report sheets)"
    echo "  💾 Output: SQLite database with hierarchical structure"
    echo "  🔄 Method: UPSERT statements (handles duplicates and updates)"
    echo "  📊 Views: Status-based, files/folders, hierarchical queries"
    echo "  🏗️  Structure: parent_folder, parent_id, level columns calculated"
    echo
    
    # Check if database already exists
    if [ -f "$DIRECTORY/report/$DATABASE_NAME" ]; then
        print_warning "Database file already exists: report/$DATABASE_NAME"
        print_info "Existing data will be updated using UPSERT statements"
        echo
    fi
    
    # Confirm execution
    read -p "Continue with SQLite import? (Y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        print_info "Import cancelled"
        exit 0
    fi
    echo
    
    # Run the importer
    print_info "Starting SQLite Direct Importer..."
    echo "----------------------------------------"
    
    if jbang SQLiteDirectImporter.java "$DIRECTORY" "$DATABASE_NAME"; then
        echo "----------------------------------------"
        print_success "SQLite import completed successfully!"
        echo
        
        # Show results
        DATABASE_PATH="$DIRECTORY/report/$DATABASE_NAME"
        if [ -f "$DATABASE_PATH" ]; then
            print_info "Database created: $DATABASE_PATH"
            
            # Show database size
            DB_SIZE=$(ls -lh "$DATABASE_PATH" | awk '{print $5}')
            echo "  📊 Database size: $DB_SIZE"
            
            # Show available views (if sqlite3 is available)
            if command -v sqlite3 &> /dev/null; then
                echo
                print_info "Database views available:"
                sqlite3 "$DATABASE_PATH" "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;" | while read -r view_name; do
                    echo "  📋 $view_name"
                done
                
                echo
                print_info "Sample queries to try:"
                echo "  sqlite3 report/$DATABASE_NAME \"SELECT * FROM status_summary;\""
                echo "  sqlite3 report/$DATABASE_NAME \"SELECT COUNT(*) as total_records FROM transfer_data;\""
                echo "  sqlite3 report/$DATABASE_NAME \"SELECT * FROM files_view LIMIT 10;\""
                echo "  sqlite3 report/$DATABASE_NAME \"SELECT * FROM folders_view LIMIT 10;\""
                echo "  sqlite3 report/$DATABASE_NAME \"SELECT level, COUNT(*) FROM transfer_data GROUP BY level;\""
            else
                echo
                print_info "Install sqlite3 command-line tool to query the database:"
                echo "  macOS: brew install sqlite"
                echo "  Ubuntu/Debian: apt-get install sqlite3"
                echo "  Windows: Download from https://sqlite.org/download.html"
            fi
        else
            print_error "Expected database file not found: $DATABASE_PATH"
        fi
        
    else
        echo "----------------------------------------"
        print_error "SQLite import failed!"
        print_info "Check the output above for error details"
        exit 1
    fi
}

# Handle Ctrl+C gracefully
trap 'echo; print_warning "SQLite import interrupted by user"; exit 130' INT

# Run main function with all arguments
main "$@"