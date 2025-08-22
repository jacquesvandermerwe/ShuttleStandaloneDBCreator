# Development Guide

This file provides guidance when working with code in this repository.

## Project Overview

ShuttleStandaloneDBCreator is a comprehensive Java-based suite for processing Excel Transfer Report files and converting the data into organized CSV files and SQLite databases. The project specializes in extracting and categorizing data from large (700MB+) Excel files containing document migration information.

## Build and Run Commands

This project uses JBang for simplified Java execution without traditional build tools:

### Setup
- Initial setup: `./setup.sh` (installs JBang, validates Java files)
- Prerequisites: Java 21+ (for virtual threads), JBang

### Core Processing Tools
- **SQLite Direct Import**: `./run-sqlite-importer.sh [directory]` (recommended - direct Excel to SQLite)
- **CSV Data Extraction**: `./run-extractor.sh [directory]` (uses ExcelDataExtractor.java)
- **Overview Extraction**: `./run-overview.sh [directory]` (uses TransferOverviewExtractor.java)
- **File Inspection**: `jbang InspectColumnsStreaming.java [excel_file]`

### Direct JBang Usage
- `jbang SQLiteDirectImporter.java [directory]` - Direct Excel to SQLite import (recommended)
- `jbang ExcelDataExtractor.java [directory]` - Extract all data by categories to CSV
- `jbang InspectColumnsStreaming.java [file]` - Examine file structure
- `jbang TransferOverviewExtractor.java [directory]` - Extract Overview sheets

## Architecture

## Folder Structure

The application uses an organized folder structure:

```
project-directory/
├── source/                          # Place Excel files here for processing
├── processed/                       # Processed Excel files moved here automatically
├── report/                          # Contains SQLite databases and organized outputs
│   ├── transfer_reports.db          # Main SQLite database
│   └── [filename]/                  # Per-file organized outputs (if using CSV extractors)
│       ├── Overview/                # Overview sheets
│       ├── Claims/                  # Claims data
│       └── Status/                  # Status-based data
└── [other project files]
```

### Core Components

1. **SQLiteDirectImporter.java** - **RECOMMENDED** Direct Excel to SQLite import
   - Processes Excel files from `source/` folder
   - Moves completed files to `processed/` folder  
   - Creates SQLite database in `report/` folder with job names
   - No intermediate CSV files created

2. **ExcelProcessor.java** - Targeted pattern extraction
   - Extracts rows where Source File Size = 0
   - Filters by specific UNC path patterns for UK legal document structure
   - Creates: Folder-[filename].csv, Claims-[filename].csv

3. **ExcelDataExtractor.java** - Comprehensive data extraction to CSV
   - Categorizes all Transfer Report data by file size and patterns
   - Creates organized folder structure: `report/[filename]/[category]/`
   - Output files:
     - `Folder-Object-[filename].csv` (File Size = 0)
     - `File-Object-[filename].csv` (File Size > 0)
     - `Claims-[filename].csv` (Claims pattern matches)
     - `Customer-Folders-[filename].csv` (Customer pattern matches)
     - `[STATUS]-Status-[filename].csv` (Grouped by File Status values)

4. **InspectColumnsStreaming.java** - File structure inspector
   - Examines first 1000 rows using streaming API
   - Identifies column structure and highlights status columns
   - Useful for understanding new file formats

5. **TransferOverviewExtractor.java** - Overview sheet extractor
   - Extracts "Overview" sheets from Excel files
   - Creates organized structure: `report/[filename]/Overview/Overview-[filename].xlsx`
   - Preserves formatting, charts, and images using ZIP-level manipulation

### Pattern Recognition

The project recognizes these folder hierarchy patterns:
- **Claims**: `/Clients/[Customer]/[Policy]/Claim Documents/[ClaimID]`
- **Customer Folders**: `/Clients/[CustomerName]`
- **Policy References**: `/Clients/[Customer]/[PolicyNumber]` (numeric only)
- **UNC Paths**: `//UKDOCDWNPSFS102/PI_Folders/D/DATA/HCCD/Folders/[NUMBER]`

### Database Schema

SQLite database (`transfer_reports.db`) includes:
- Main table: `transfer_data` with all Excel columns plus hierarchy and job tracking
- Computed columns: `parent_folder`, `parent_id`, `level`, `job_name`
- Analysis views: `files_view`, `folders_view`, `status_summary`, `hierarchy_children`
- Dynamic status views: `status_[name]` for each unique status type
- Indexes for performance on all key fields

### Memory Management

- Uses Apache POI streaming API for XLSX files (memory efficient)
- Traditional POI approach for XLS files
- 8GB heap allocation via JBang JVM options
- Transaction batching (1000 records per batch)
- Automatic garbage collection between files

## File Processing Workflow

### SQLite Direct Import (Recommended)
1. **Setup**: Place Excel files in `source/` folder
2. **Processing**: Run `./run-sqlite-importer.sh` to process all Transfer Report sheets  
3. **Database Creation**: Creates `report/transfer_reports.db` with hierarchical data and job tracking
4. **File Management**: Automatically moves processed files to `processed/` folder
5. **Analysis**: Query using built-in views and status breakdowns

### CSV Extraction Workflow (Alternative)
1. **Discovery**: Find Excel files in target directory
2. **Extraction**: Process Transfer Report sheets using streaming methods  
3. **Categorization**: Sort data by file size, patterns, and status values
4. **Output**: Create organized CSV files in `report/[filename]/[category]/` structure

## Runner Scripts

- `setup.sh` - Environment setup and validation
- `run-extractor.sh` - Main data extraction pipeline (with optional SQLite import)
- `run-overview.sh` - Overview sheet extraction (if TransferOverviewExtractor.java exists)
- All scripts include progress reporting, error handling, and colored output

## Development Notes

- Project uses JBang shebang format for easy execution without build tools
- All Java files are self-contained with dependency declarations in comments  
- Memory-optimized for processing 700MB+ Excel files with streaming APIs
- Pattern matching is specific to UK legal document migration structure
- Excel date handling accounts for Excel's 1900 leap year bug
- Cross-platform emoji support detection for output formatting
- Automatic folder structure management with source/processed/report organization