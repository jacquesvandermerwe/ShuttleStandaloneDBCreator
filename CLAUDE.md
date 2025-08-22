# Development Guide

This file provides guidance when working with code in this repository.

## Project Overview

ShuttleStandaloneDBCreator is a Java-based suite for processing Excel Transfer Report files and importing the data directly into SQLite databases. The project specializes in extracting and categorizing data from large (700MB+) Excel files containing document migration information.

## Build and Run Commands

This project uses JBang for simplified Java execution without traditional build tools:

### Setup
- Initial setup: `./setup.sh` (installs JBang, validates Java files)
- Prerequisites: Java 21+ (for virtual threads), JBang

### Core Processing Tools
- **SQLite Direct Import**: `./run-sqlite-importer.sh` (recommended - direct Excel to SQLite database)
- **Overview Extraction**: `./run-overview.sh [directory]` (extract Overview sheets to Excel files)
- **File Inspection**: `jbang InspectColumnsStreaming.java [excel_file]` (examine file structure)

### Direct JBang Usage
- `jbang SQLiteDirectImporter.java` - Direct Excel to SQLite import (primary workflow)
- `jbang TransferOverviewExtractor.java [directory]` - Extract Overview sheets
- `jbang InspectColumnsStreaming.java [file]` - Examine file structure

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

2. **InspectColumnsStreaming.java** - File structure inspector
   - Examines first 1000 rows using streaming API
   - Identifies column structure and highlights status columns
   - Useful for understanding new file formats

3. **TransferOverviewExtractor.java** - Overview sheet extractor
   - Extracts "Overview" sheets from Excel files
   - Creates organized structure: `report/[filename]/Overview/Overview-[filename].xlsx`
   - Preserves formatting, charts, and images using ZIP-level manipulation

### Hierarchical Processing

The project performs generic hierarchy calculation:
- **Path Level Calculation**: Counts directory separators in file paths
- **Parent Folder Extraction**: Derives parent directory from file path structure
- **Parent ID Linking**: Maps parent folders to target_file_id references
- **Generic Pattern Support**: Works with any folder hierarchy structure

### Database Schema

SQLite database (`transfer_reports.db`) includes:

**Main Table: `transfer_data`**
- All Excel columns mapped correctly to match actual file structure
- Core fields: `file_name`, `source_file_size`, `target_file_size`, `target_file_id`
- Account fields: `source_account`, `target_account`
- Timestamp fields: `creation_time`, `source_last_modification_time`, `target_last_modification_time`, `last_access_time`, `start_time`, `transfer_time`
- Processing fields: `checksum_method`, `checksum`, `file_status`, `errors`, `status`, `translated_file_name`
- Computed columns: `parent_folder`, `parent_id`, `level`, `job_name` (filename without extension)
- Metadata: `import_timestamp`

**Views and Indexes:**
- Analysis views: `files_view`, `folders_view`, `status_summary`, `hierarchy_children`
- Performance indexes on all key fields (created after data import for optimal performance)
- UNIQUE constraint on `(file_name, target_file_id)` for UPSERT operations

### Performance Optimizations

- **Streaming Processing**: Apache POI streaming API for XLSX files (memory efficient)
- **Index Management**: Drops indexes before bulk import, recreates after completion for optimal performance
- **Memory Settings**: 8GB heap allocation via JBang JVM options
- **Batch Processing**: Transaction batching (1000 records per batch)
- **Resource Management**: Automatic garbage collection between files
- **File Type Handling**: Streaming for .xlsx files, traditional API for .xls files
- **Warning Suppression**: Native access and SLF4J logging optimizations

## File Processing Workflow

### SQLite Direct Import (Recommended)
1. **Setup**: Place Excel files in `source/` folder
2. **Processing**: Run `./run-sqlite-importer.sh` to process all Transfer Report sheets
3. **Detailed Feedback**: Shows sheet-by-sheet processing with timing and row counts
4. **Performance**: Drops indexes during import, recreates after completion for optimal speed
5. **Database Creation**: Creates `report/transfer_reports.db` with hierarchical data and job tracking
6. **File Management**: Automatically moves processed files to `processed/` folder
7. **Analysis**: Query using built-in views and status breakdowns

**Import Process Details:**
- Processes only sheets named "Transfer Report*"
- Provides real-time feedback on sheet discovery and processing
- Handles both new database creation and appending to existing databases
- Maps Excel columns correctly to database fields
- Stores source filename in `job_name` field for traceability

## Runner Scripts

- `setup.sh` - Environment setup and validation
- `run-sqlite-importer.sh` - Primary data processing pipeline (direct Excel to SQLite)
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