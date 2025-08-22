# Shuttle Transfer Report Processor

A comprehensive Java-based suite for processing Excel Transfer Report files and converting the data into organized SQLite databases. Specializes in extracting and categorizing data from large Excel files containing document migration information.

## Quick Start

1. **Setup Environment**
   ```bash
   ./setup.sh
   ```

2. **Place Excel Files**
   - Put your Excel files in the `source/` folder

3. **Process Files**
   ```bash
   ./run-sqlite-importer.sh
   ```

4. **View Results**
   - Database: `report/transfer_reports.db`
   - Processed files moved to: `processed/`

## Project Structure

```
project/
├── source/              # Place Excel files here
├── processed/          # Processed files moved here automatically  
├── report/            # SQLite databases and organized outputs
│   └── transfer_reports.db
├── setup.sh           # Environment setup
├── run-sqlite-importer.sh  # Main processing script (recommended)
└── run-overview.sh         # Overview sheet extraction
```

## Core Features

### SQLite Direct Import (Recommended)
- **Direct Excel → SQLite**: No intermediate CSV files
- **Job Tracking**: Each Excel filename stored as job_name in database
- **Hierarchical Data**: Automatic parent-child folder relationships
- **UPSERT Operations**: Safe processing of multiple files
- **Auto File Management**: source/ → processed/ after completion
- **Memory Efficient**: Handles 700MB+ Excel files using streaming APIs

### Database Structure
- **Main Table**: `transfer_data` with Excel columns mapped to actual file structure
- **Core Fields**: `file_name`, `source_file_size`, `target_file_size`, `target_file_id`, `file_status`, `status`
- **Key Columns**: `job_name` (source Excel filename), `parent_folder`, `parent_id`, `level`
- **Analysis Views**: 
  - `files_view` - All files (source_file_size > 0)
  - `folders_view` - All folders (source_file_size = 0)
  - `status_summary` - Status counts and breakdown
  - `hierarchy_children` - Recursive parent-child relationships
- **Performance**: Indexes created after data import for optimal speed

### Alternative Processing Options
- **Overview Extraction**: `./run-overview.sh` - Extract Overview sheets to Excel
- **File Inspection**: `jbang InspectColumnsStreaming.java [file]` - Examine structure

## Usage Examples

### Basic Processing
```bash
# Process all Excel files in source/ folder
./run-sqlite-importer.sh

# Process with custom database name
./run-sqlite-importer.sh . my_database.db
```

### Database Queries
```sql
-- View all job names and record counts
SELECT job_name, COUNT(*) FROM transfer_data GROUP BY job_name;

-- View status summary
SELECT * FROM status_summary;

-- Find hierarchical relationships
SELECT * FROM hierarchy_children WHERE target_file_id = 'parent-id';

-- View specific job data
SELECT * FROM transfer_data WHERE job_name = 'MyExcelFileName' LIMIT 10;
```

### Advanced Usage
```bash
# Direct JBang execution
jbang SQLiteDirectImporter.java /path/to/project custom.db

# Extract only Overview sheets
./run-overview.sh /path/to/excel/files
```

## Performance

- **Index Management**: Drops indexes during import, recreates after completion for optimal speed
- **Memory Optimized**: 8GB heap allocation with streaming APIs
- **Large File Support**: Successfully processes 700MB+ Excel files
- **Batch Processing**: 1000-record transaction batches
- **Real-time Feedback**: Sheet-by-sheet progress with timing and row counts
- **Cross-Platform**: Windows, macOS, Linux support

## Requirements

- **Java 21+** (for virtual threads and modern features)
- **JBang** (installed automatically by setup.sh)
- **Excel Files**: .xlsx and .xls formats supported

## Hierarchical Processing

The application calculates folder hierarchy relationships:
- **Path Level**: Calculated by counting directory separators in file paths
- **Parent Folder**: Extracted from file path directory structure  
- **Parent ID**: Linked to target_file_id of parent folder record
- **Generic Structure**: Works with any folder hierarchy pattern

## Troubleshooting

### Common Issues
- **No Excel files found**: Ensure files are in `source/` folder
- **Memory errors**: Large files may need increased heap size
- **Permission errors**: Ensure script files are executable

### File Management
- Processed files are automatically moved to `processed/` folder
- Database files are created in `report/` folder
- Temporary files are automatically cleaned up

## Development

This project uses JBang for simplified Java execution without traditional build tools. All dependencies are declared within the Java files themselves.

### Key Components
- `SQLiteDirectImporter.java` - Main direct import processor with column mapping corrections
- `TransferOverviewExtractor.java` - Overview sheet extractor
- `InspectColumnsStreaming.java` - File structure inspector

### Memory Management
- Apache POI streaming API for XLSX files
- Traditional POI for XLS files
- Automatic garbage collection between files
- Transaction batching for database operations
- Performance-optimized index management

---

*This tool is designed for processing Shuttle document migration Transfer Reports, optimized for UK legal document structures and large-scale data migration analysis.*