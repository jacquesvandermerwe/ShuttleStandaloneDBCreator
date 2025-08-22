///usr/bin/env jbang "$0" "$@" ; exit $?

// JVM Options for handling large Excel files
//JAVA_OPTIONS -Xmx8g -XX:+UseG1GC -Djdk.xml.maxGeneralEntitySizeLimit=0 -Djdk.xml.totalEntitySizeLimit=0 -Djdk.xml.maxParameterEntitySizeLimit=0 -Djdk.xml.entityExpansionLimit=0 -Djdk.xml.maxElementDepth=0 --enable-native-access=ALL-UNNAMED -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN

// Dependencies for Excel processing and SQLite
//DEPS org.apache.poi:poi:5.3.0
//DEPS org.apache.poi:poi-ooxml:5.3.0
//DEPS org.apache.poi:poi-scratchpad:5.3.0
//DEPS commons-io:commons-io:2.16.1
//DEPS org.xerial:sqlite-jdbc:3.45.3.0
//DEPS org.apache.logging.log4j:log4j-core:2.23.1
//DEPS org.apache.logging.log4j:log4j-slf4j-impl:2.23.1

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
import org.xml.sax.ContentHandler;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SQLite Direct Importer - Import Excel Transfer Report data directly into SQLite database
 * 
 * This application processes Excel files containing "Transfer Report" sheets and imports
 * all data directly into a SQLite database using UPSERT statements. No CSV files are created.
 * 
 * Features:
 * - Direct Excel to SQLite import with UPSERT for handling duplicates
 * - Hierarchical folder structure calculation (parent_folder, parent_id, level)
 * - Status-based views for different file statuses (match-exists, filtered, etc.)
 * - Aggregation views for status counts, files vs folders
 * - Hierarchical query view for parent-child relationships
 * - Memory-efficient streaming processing for large files (700MB+)
 * 
 * Usage: jbang SQLiteDirectImporter.java [directory_path] [database_name]
 */
public class SQLiteDirectImporter {
    
    private static final String DEFAULT_DATABASE_NAME = "transfer_reports.db";
    private static final String TABLE_NAME = "transfer_data";
    private static final int BATCH_SIZE = 1000;
    
    // Platform detection for emoji support
    private static final boolean SUPPORTS_EMOJI = !System.getProperty("os.name").toLowerCase().contains("windows");
    
    // Database column names (snake_case for SQLite) - Updated to match actual Excel structure
    private static final String[] DB_COLUMNS = {
        "file_name", "source_file_size", "target_file_size", "target_file_id", "source_account",
        "target_account", "creation_time", "source_last_modified_by", "source_last_modification_time",
        "target_last_modification_time", "last_access_time", "start_time", "transfer_time",
        "checksum_method", "checksum", "file_status", "errors", "status", "translated_file_name"
    };
    
    // Excel header names (exactly as they appear in the Excel files) - Updated to match inspection
    private static final String[] EXCEL_HEADERS = {
        "File Name", "Source File Size", "Target File Size", "Target File ID", "Source Account",
        "Target Account", "Creation Time", "Source Last Modified By", "Source Last Modification Time",
        "Target Last Modification Time", "Last Access Time", "Start Time", "Transfer Time",
        "Checksum Method", "Checksum", "File Status", "Errors", "Status", "Translated File Name"
    };
    
    // Date/time columns that need conversion from Excel serial numbers
    private static final Set<String> DATE_COLUMNS = Set.of(
        "creation_time", "source_last_modification_time", "target_last_modification_time",
        "last_access_time", "start_time", "transfer_time"
    );
    
    private static void printInfo(String message) {
        System.out.println((SUPPORTS_EMOJI ? "🔍 " : "[INFO] ") + message);
    }
    
    private static void printSuccess(String message) {
        System.out.println((SUPPORTS_EMOJI ? "✅ " : "[SUCCESS] ") + message);
    }
    
    private static void printProgress(String message) {
        System.out.println((SUPPORTS_EMOJI ? "🏗️  " : "[PROGRESS] ") + message);
    }
    
    private static void printError(String message) {
        System.err.println((SUPPORTS_EMOJI ? "❌ " : "[ERROR] ") + message);
    }
    
    public static void main(String[] args) throws Exception {
        long applicationStartTime = System.currentTimeMillis();
        
        // Increase Apache POI memory limit for large Excel files (1GB limit)
        IOUtils.setByteArrayMaxOverride(1024 * 1024 * 1024);
        
        String basePath = args.length > 0 ? args[0] : ".";
        String databaseName = args.length > 1 ? args[1] : DEFAULT_DATABASE_NAME;
        
        Path baseDirectory = Paths.get(basePath);
        if (!Files.exists(baseDirectory)) {
            printError("Directory does not exist: " + basePath);
            System.exit(1);
        }
        
        // Create source and processed directories
        Path sourceDirectory = baseDirectory.resolve("source");
        Path processedDirectory = baseDirectory.resolve("processed");
        
        try {
            Files.createDirectories(sourceDirectory);
            Files.createDirectories(processedDirectory);
        } catch (IOException e) {
            printError("Could not create source/processed directories: " + e.getMessage());
            System.exit(1);
        }
        
        // Create report directory and put database there
        Path reportDirectory = baseDirectory.resolve("report");
        try {
            Files.createDirectories(reportDirectory);
        } catch (IOException e) {
            printError("Could not create report directory: " + e.getMessage());
            System.exit(1);
        }
        
        Path databasePath = reportDirectory.resolve(databaseName);
        
        printInfo("Starting SQLite Direct Import");
        printInfo("Base Directory: " + baseDirectory.toAbsolutePath());
        printInfo("Source Directory: " + sourceDirectory.toAbsolutePath());
        printInfo("Processed Directory: " + processedDirectory.toAbsolutePath());
        printInfo("Database: " + databasePath.toAbsolutePath());
        
        List<Path> excelFiles = findExcelFiles(sourceDirectory);
        if (excelFiles.isEmpty()) {
            printInfo("No Excel files found in directory");
            return;
        }
        
        printInfo("Found " + excelFiles.size() + " Excel files to process");
        
        // Statistics tracking
        int filesProcessed = 0;
        int totalRowsProcessed = 0;
        int totalRowsInserted = 0;
        
        try (Connection conn = createDatabase(databasePath)) {
            // Drop indexes before data import for optimal performance
            printInfo("Dropping indexes for faster bulk import...");
            dropIndexes(conn);
            
            // Process files sequentially to avoid memory issues
            for (Path file : excelFiles) {
                try {
                    printInfo("Processing file: " + file.getFileName());
                    long startTime = System.currentTimeMillis();
                    
                    int rowsProcessed = processExcelFile(conn, file);
                    filesProcessed++;
                    totalRowsProcessed += rowsProcessed;
                    
                    long endTime = System.currentTimeMillis();
                    long processingTime = endTime - startTime;
                    printSuccess("Completed file: " + file.getFileName() + " in " + formatTime(processingTime) + 
                               " (" + String.format("%,d", rowsProcessed) + " rows)");
                    
                    // Move processed file to processed directory
                    try {
                        Path targetPath = processedDirectory.resolve(file.getFileName());
                        Files.move(file, targetPath, StandardCopyOption.REPLACE_EXISTING);
                        printInfo("Moved " + file.getFileName() + " to processed folder");
                    } catch (IOException e) {
                        printError("Could not move " + file.getFileName() + " to processed folder: " + e.getMessage());
                    }
                    
                    // Force garbage collection after each file
                    System.gc();
                } catch (Exception e) {
                    printError("Error processing " + file + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
            // Create indexes for optimal query performance after data loading
            printInfo("Creating database indexes...");
            createIndexes(conn);
            
            // Calculate parent IDs and create views
            printInfo("Calculating hierarchical relationships...");
            calculateParentIDs(conn);
            
            printInfo("Creating database views...");
            createViews(conn);
            
            // Display final statistics
            displayFinalStatistics(conn, filesProcessed, totalRowsProcessed, applicationStartTime);
            
        } catch (SQLException e) {
            printError("Database error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Find all Excel files in the specified directory (non-recursive)
     */
    private static List<Path> findExcelFiles(Path directory) throws IOException {
        List<Path> excelFiles = new ArrayList<>();
        
        try (var stream = Files.list(directory)) {
            stream.filter(Files::isRegularFile)
                  .forEach(file -> {
                      String fileName = file.getFileName().toString();
                      String lowerFileName = fileName.toLowerCase();
                      
                      // Skip temporary Excel files
                      if (fileName.startsWith("~$") || fileName.startsWith("~")) {
                          return;
                      }
                      
                      // Include .xlsx and .xls files
                      if (lowerFileName.endsWith(".xlsx") || lowerFileName.endsWith(".xls")) {
                          excelFiles.add(file);
                      }
                  });
        }
        
        return excelFiles;
    }
    
    /**
     * Create SQLite database and initialize schema
     */
    private static Connection createDatabase(Path databasePath) throws SQLException {
        boolean isNewDatabase = !Files.exists(databasePath);
        
        if (isNewDatabase) {
            printInfo("Creating new SQLite database: " + databasePath.getFileName());
        } else {
            printInfo("Using existing SQLite database: " + databasePath.getFileName());
        }
        
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
        
        // Enable performance optimizations
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA foreign_keys = ON");
            stmt.execute("PRAGMA synchronous = NORMAL");
            stmt.execute("PRAGMA cache_size = -2000000"); // 2GB cache
            stmt.execute("PRAGMA temp_store = MEMORY");
            stmt.execute("PRAGMA journal_mode = WAL");
        }
        
        // Create table if it doesn't exist
        createTable(conn);
        
        return conn;
    }
    
    /**
     * Create the main transfer_data table
     */
    private static void createTable(Connection conn) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME).append(" (");
        createTableSQL.append("id INTEGER PRIMARY KEY, ");
        
        // Add all original columns with appropriate data types
        for (String column : DB_COLUMNS) {
            if (DATE_COLUMNS.contains(column)) {
                createTableSQL.append(column).append(" DATETIME, ");
            } else if (column.equals("source_file_size") || column.equals("target_file_size") || column.equals("target_file_id")) {
                createTableSQL.append(column).append(" BIGINT, ");
            } else {
                createTableSQL.append(column).append(" TEXT, ");
            }
        }
        
        // Add hierarchical columns
        createTableSQL.append("parent_folder TEXT, ");
        createTableSQL.append("parent_id TEXT, ");
        createTableSQL.append("level INTEGER, ");
        createTableSQL.append("job_name TEXT, ");
        createTableSQL.append("import_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, ");
        
        // Add unique constraint for UPSERT operations  
        createTableSQL.append("UNIQUE(file_name, target_file_id)");
        createTableSQL.append(")");
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL.toString());
        }
        
        printProgress("Database table created/verified (indexes will be created after data import)");
    }
    
    /**
     * Process a single Excel file
     */
    private static int processExcelFile(Connection conn, Path filePath) throws Exception {
        if (filePath.toString().toLowerCase().endsWith(".xlsx")) {
            return processXLSXStreaming(conn, filePath);
        } else {
            return processXLSTraditional(conn, filePath);
        }
    }
    
    /**
     * Process XLSX files using streaming API
     */
    private static int processXLSXStreaming(Connection conn, Path filePath) throws Exception {
        int totalRowsProcessed = 0;
        int sheetCount = 0;
        int transferReportSheets = 0;
        
        try (OPCPackage pkg = OPCPackage.open(filePath.toFile())) {
            XSSFReader reader = new XSSFReader(pkg);
            SharedStrings sst = reader.getSharedStringsTable();
            
            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            
            while (sheets.hasNext()) {
                try (InputStream sheet = sheets.next()) {
                    String sheetName = sheets.getSheetName();
                    sheetCount++;
                    
                    System.out.println("    " + (SUPPORTS_EMOJI ? "📄 " : "[SHEET] ") + 
                                     "Sheet " + sheetCount + ": " + sheetName);
                    
                    if (sheetName.startsWith("Transfer Report")) {
                        transferReportSheets++;
                        System.out.println("      " + (SUPPORTS_EMOJI ? "🔄 " : "[PROCESSING] ") + 
                                         "Processing Transfer Report data...");
                        
                        long sheetStartTime = System.currentTimeMillis();
                        StreamingHandler handler = new StreamingHandler(conn, filePath.getFileName().toString());
                        processSheet(sheet, sst, handler);
                        int rowsProcessed = handler.getRowsProcessed();
                        int errorCount = handler.getErrorCount();
                        totalRowsProcessed += rowsProcessed;
                        
                        long sheetDuration = System.currentTimeMillis() - sheetStartTime;
                        String errorSummary = errorCount > 0 ? " (" + errorCount + " errors)" : "";
                        System.out.println("      " + (SUPPORTS_EMOJI ? "✅ " : "[COMPLETED] ") + 
                                         String.format("%,d", rowsProcessed) + " rows processed in " + 
                                         formatTime(sheetDuration) + errorSummary);
                    } else {
                        System.out.println("      " + (SUPPORTS_EMOJI ? "⏭️  " : "[SKIPPED] ") + 
                                         "Not a Transfer Report sheet - skipping");
                    }
                }
            }
        }
        
        System.out.println("    " + (SUPPORTS_EMOJI ? "📊 " : "[SUMMARY] ") + 
                          "File summary: " + transferReportSheets + " Transfer Report sheets processed out of " + 
                          sheetCount + " total sheets");
        
        return totalRowsProcessed;
    }
    
    /**
     * Process XLS files using traditional API
     */
    private static int processXLSTraditional(Connection conn, Path filePath) throws Exception {
        int totalRowsProcessed = 0;
        int transferReportSheets = 0;
        int totalSheets = 0;
        
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             Workbook workbook = new HSSFWorkbook(fis)) {
            
            totalSheets = workbook.getNumberOfSheets();
            
            for (int i = 0; i < totalSheets; i++) {
                Sheet sheet = workbook.getSheetAt(i);
                String sheetName = sheet.getSheetName();
                
                System.out.println("    " + (SUPPORTS_EMOJI ? "📄 " : "[SHEET] ") + 
                                 "Sheet " + (i + 1) + ": " + sheetName);
                
                if (sheetName.startsWith("Transfer Report")) {
                    transferReportSheets++;
                    System.out.println("      " + (SUPPORTS_EMOJI ? "🔄 " : "[PROCESSING] ") + 
                                     "Processing Transfer Report data...");
                    
                    long sheetStartTime = System.currentTimeMillis();
                    int rowsProcessed = processSheetTraditional(conn, sheet, filePath.getFileName().toString());
                    totalRowsProcessed += rowsProcessed;
                    
                    long sheetDuration = System.currentTimeMillis() - sheetStartTime;
                    System.out.println("      " + (SUPPORTS_EMOJI ? "✅ " : "[COMPLETED] ") + 
                                     String.format("%,d", rowsProcessed) + " rows processed in " + 
                                     formatTime(sheetDuration));
                } else {
                    System.out.println("      " + (SUPPORTS_EMOJI ? "⏭️  " : "[SKIPPED] ") + 
                                     "Not a Transfer Report sheet - skipping");
                }
            }
        }
        
        System.out.println("    " + (SUPPORTS_EMOJI ? "📊 " : "[SUMMARY] ") + 
                          "File summary: " + transferReportSheets + " Transfer Report sheets processed out of " + 
                          totalSheets + " total sheets");
        
        return totalRowsProcessed;
    }
    
    /**
     * Streaming event handler for XLSX processing
     */
    private static class StreamingHandler implements SheetContentsHandler {
        private final Connection conn;
        private final String sourceFile;
        private final String jobName;
        private final PreparedStatement upsertStmt;
        private List<String> currentRow;
        private String[] headerRow;
        private boolean isFirstRow = true;
        private int rowsProcessed = 0;
        private int batchCount = 0;
        private int errorCount = 0;
        
        public StreamingHandler(Connection conn, String sourceFile) throws SQLException {
            this.conn = conn;
            this.sourceFile = sourceFile;
            this.jobName = extractJobName(sourceFile);
            this.upsertStmt = conn.prepareStatement(buildUpsertSQL());
            conn.setAutoCommit(false);
        }
        
        @Override
        public void startRow(int rowNum) {
            currentRow = new ArrayList<>();
        }
        
        @Override
        public void cell(String cellReference, String formattedValue, XSSFComment comment) {
            int columnIndex = getColumnIndex(cellReference);
            
            while (currentRow.size() <= columnIndex) {
                currentRow.add("");
            }
            
            currentRow.set(columnIndex, formattedValue != null ? formattedValue : "");
        }
        
        @Override
        public void endRow(int rowNum) {
            if (isFirstRow) {
                headerRow = currentRow.toArray(new String[0]);
                isFirstRow = false;
                return;
            }
            
            try {
                insertRow(currentRow.toArray(new String[0]));
                rowsProcessed++;
                batchCount++;
                
                if (batchCount >= BATCH_SIZE) {
                    upsertStmt.executeBatch();
                    conn.commit();
                    batchCount = 0;
                    System.out.print(".");
                    System.out.flush();
                }
            } catch (SQLException e) {
                errorCount++;
                String fileName = currentRow.size() > 0 ? currentRow.get(0) : "unknown";
                printError("Row " + (rowNumber + 1) + " skipped - SQL error for file '" + fileName + "': " + e.getMessage());
                
                // Log problematic row data for debugging (first few columns only)
                StringBuilder rowPreview = new StringBuilder();
                for (int i = 0; i < Math.min(currentRow.size(), 3); i++) {
                    if (i > 0) rowPreview.append(", ");
                    String value = currentRow.get(i);
                    rowPreview.append(value != null && value.length() > 50 ? value.substring(0, 50) + "..." : value);
                }
                printError("Row data preview: " + rowPreview.toString());
                
                // Continue processing despite the error
                if (errorCount % 100 == 0) {
                    System.out.println();
                    printError("Warning: " + errorCount + " rows have been skipped due to errors");
                }
            }
        }
        
        private void insertRow(String[] rowData) throws SQLException {
            // Map Excel columns to database columns and insert
            for (int i = 0; i < DB_COLUMNS.length && i < EXCEL_HEADERS.length; i++) {
                String value = i < rowData.length ? rowData[i] : "";
                String dbColumn = DB_COLUMNS[i];
                
                if (DATE_COLUMNS.contains(dbColumn)) {
                    String convertedDate = convertExcelDateToSQLite(value);
                    if (convertedDate != null) {
                        upsertStmt.setString(i + 1, convertedDate);
                    } else {
                        upsertStmt.setNull(i + 1, Types.VARCHAR);
                    }
                } else if (dbColumn.equals("source_file_size") || dbColumn.equals("target_file_size") || dbColumn.equals("target_file_id")) {
                    try {
                        if (value != null && !value.trim().isEmpty()) {
                            long fileSize = Long.parseLong(value.trim());
                            upsertStmt.setLong(i + 1, fileSize);
                        } else {
                            upsertStmt.setNull(i + 1, Types.BIGINT);
                        }
                    } catch (NumberFormatException e) {
                        upsertStmt.setNull(i + 1, Types.BIGINT);
                    }
                } else {
                    upsertStmt.setString(i + 1, value);
                }
            }
            
            // Set hierarchical columns
            String fileName = rowData.length > 0 ? rowData[0] : "";
            int level = calculateLevel(fileName);
            String parentFolder = getParentFolder(fileName);
            
            upsertStmt.setString(DB_COLUMNS.length + 1, parentFolder);
            upsertStmt.setString(DB_COLUMNS.length + 2, null); // parent_id will be calculated later
            upsertStmt.setInt(DB_COLUMNS.length + 3, level);
            upsertStmt.setString(DB_COLUMNS.length + 4, jobName);
            
            upsertStmt.addBatch();
        }
        
        public void finish() throws SQLException {
            if (batchCount > 0) {
                upsertStmt.executeBatch();
                conn.commit();
            }
            upsertStmt.close();
            conn.setAutoCommit(true);
            if (rowsProcessed > 0) {
                System.out.println(); // New line after progress dots
            }
            
            // Report error summary if any errors occurred
            if (errorCount > 0) {
                printError("Import completed with " + errorCount + " rows skipped due to errors");
            }
        }
        
        public int getRowsProcessed() {
            return rowsProcessed;
        }
        
        public int getErrorCount() {
            return errorCount;
        }
        
        private int getColumnIndex(String cellReference) {
            if (cellReference == null || cellReference.isEmpty()) return 0;
            
            // Extract column letters from cell reference (e.g., "B10" -> "B")
            String colStr = cellReference.replaceAll("\\d", "");
            return CellReference.convertColStringToIndex(colStr);
        }
    }
    
    /**
     * Process sheet using traditional POI approach (for XLS files)
     */
    private static int processSheetTraditional(Connection conn, Sheet sheet, String sourceFile) throws SQLException {
        String jobName = extractJobName(sourceFile);
        PreparedStatement upsertStmt = conn.prepareStatement(buildUpsertSQL());
        conn.setAutoCommit(false);
        
        int rowsProcessed = 0;
        int batchCount = 0;
        int errorCount = 0;
        
        try {
            for (Row row : sheet) {
                if (row.getRowNum() == 0) continue; // Skip header
                
                String[] rowData = new String[EXCEL_HEADERS.length];
                for (int i = 0; i < EXCEL_HEADERS.length && i < row.getLastCellNum(); i++) {
                    Cell cell = row.getCell(i);
                    rowData[i] = getCellValueAsString(cell);
                }
                
                try {
                    // Insert row using same logic as streaming handler
                    insertRowTraditional(upsertStmt, rowData, jobName);
                    rowsProcessed++;
                    batchCount++;
                    
                    if (batchCount >= BATCH_SIZE) {
                        upsertStmt.executeBatch();
                        conn.commit();
                        batchCount = 0;
                        System.out.print(".");
                        System.out.flush();
                    }
                } catch (SQLException e) {
                    errorCount++;
                    String fileName = rowData.length > 0 ? rowData[0] : "unknown";
                    printError("Row " + (row.getRowNum() + 1) + " skipped - SQL error for file '" + fileName + "': " + e.getMessage());
                    
                    // Log problematic row data for debugging (first few columns only)
                    StringBuilder rowPreview = new StringBuilder();
                    for (int i = 0; i < Math.min(rowData.length, 3); i++) {
                        if (i > 0) rowPreview.append(", ");
                        String value = rowData[i];
                        rowPreview.append(value != null && value.length() > 50 ? value.substring(0, 50) + "..." : value);
                    }
                    printError("Row data preview: " + rowPreview.toString());
                    
                    // Continue processing despite the error
                    if (errorCount % 100 == 0) {
                        System.out.println();
                        printError("Warning: " + errorCount + " rows have been skipped due to errors");
                    }
                }
            }
            
            if (batchCount > 0) {
                upsertStmt.executeBatch();
                conn.commit();
            }
            
            if (rowsProcessed > 0) {
                System.out.println(); // New line after progress dots
            }
            
            // Report error summary if any errors occurred
            if (errorCount > 0) {
                printError("Import completed with " + errorCount + " rows skipped due to errors");
            }
            
        } finally {
            upsertStmt.close();
            conn.setAutoCommit(true);
        }
        
        return rowsProcessed;
    }
    
    /**
     * Insert row for traditional processing
     */
    private static void insertRowTraditional(PreparedStatement upsertStmt, String[] rowData, String jobName) throws SQLException {
        for (int i = 0; i < DB_COLUMNS.length; i++) {
            String value = i < rowData.length ? rowData[i] : "";
            String dbColumn = DB_COLUMNS[i];
            
            if (DATE_COLUMNS.contains(dbColumn)) {
                String convertedDate = convertExcelDateToSQLite(value);
                if (convertedDate != null) {
                    upsertStmt.setString(i + 1, convertedDate);
                } else {
                    upsertStmt.setNull(i + 1, Types.VARCHAR);
                }
            } else if (dbColumn.equals("source_file_size") || dbColumn.equals("target_file_size") || dbColumn.equals("target_file_id")) {
                try {
                    if (value != null && !value.trim().isEmpty()) {
                        long fileSize = Long.parseLong(value.trim());
                        upsertStmt.setLong(i + 1, fileSize);
                    } else {
                        upsertStmt.setNull(i + 1, Types.BIGINT);
                    }
                } catch (NumberFormatException e) {
                    upsertStmt.setNull(i + 1, Types.BIGINT);
                }
            } else {
                upsertStmt.setString(i + 1, value);
            }
        }
        
        // Set hierarchical columns
        String fileName = rowData.length > 0 ? rowData[0] : "";
        int level = calculateLevel(fileName);
        String parentFolder = getParentFolder(fileName);
        
        upsertStmt.setString(DB_COLUMNS.length + 1, parentFolder);
        upsertStmt.setString(DB_COLUMNS.length + 2, null);
        upsertStmt.setInt(DB_COLUMNS.length + 3, level);
        upsertStmt.setString(DB_COLUMNS.length + 4, jobName);
        
        upsertStmt.addBatch();
    }
    
    /**
     * Build UPSERT SQL statement
     */
    private static String buildUpsertSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT OR REPLACE INTO ").append(TABLE_NAME).append(" (");
        
        // Add column names
        for (String column : DB_COLUMNS) {
            sql.append(column).append(", ");
        }
        sql.append("parent_folder, parent_id, level, job_name) VALUES (");
        
        // Add placeholders
        for (int i = 0; i < DB_COLUMNS.length + 4; i++) {
            sql.append("?, ");
        }
        sql.setLength(sql.length() - 2); // Remove last comma
        sql.append(")");
        
        return sql.toString();
    }
    
    /**
     * Process sheet using SAX parsing
     */
    private static void processSheet(InputStream sheetInputStream, SharedStrings sst, StreamingHandler handler) throws Exception {
        XMLReader parser = XMLReaderFactory.createXMLReader();
        DataFormatter formatter = new DataFormatter();
        ContentHandler contentHandler = new XSSFSheetXMLHandler(null, sst, handler, formatter, false);
        parser.setContentHandler(contentHandler);
        parser.parse(new org.xml.sax.InputSource(sheetInputStream));
        handler.finish();
    }
    
    /**
     * Drop indexes before bulk import for optimal performance
     */
    private static void dropIndexes(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long startTime = System.currentTimeMillis();
            
            // Drop indexes (ignore errors if they don't exist)
            String[] indexes = {
                "idx_file_name", "idx_target_file_id", "idx_file_status", "idx_status",
                "idx_parent_folder", "idx_parent_id", "idx_level", "idx_source_file_size", "idx_job_name"
            };
            
            int droppedCount = 0;
            for (String index : indexes) {
                try {
                    stmt.execute("DROP INDEX IF EXISTS " + index);
                    droppedCount++;
                } catch (SQLException e) {
                    // Ignore errors - index might not exist
                }
            }
            
            long duration = System.currentTimeMillis() - startTime;
            printSuccess("Dropped " + droppedCount + " indexes in " + duration + "ms");
        }
    }
    
    /**
     * Create indexes for optimal query performance (called after data import)
     */
    private static void createIndexes(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long startTime = System.currentTimeMillis();
            
            // Create indexes for performance
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_file_name ON " + TABLE_NAME + " (file_name)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_target_file_id ON " + TABLE_NAME + " (target_file_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_file_status ON " + TABLE_NAME + " (file_status)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_status ON " + TABLE_NAME + " (status)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_parent_folder ON " + TABLE_NAME + " (parent_folder)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_parent_id ON " + TABLE_NAME + " (parent_id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_level ON " + TABLE_NAME + " (level)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_source_file_size ON " + TABLE_NAME + " (source_file_size)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_job_name ON " + TABLE_NAME + " (job_name)");
            
            long duration = System.currentTimeMillis() - startTime;
            printSuccess("Created 9 database indexes in " + duration + "ms");
        }
    }
    
    /**
     * Calculate parent IDs for all records
     */
    private static void calculateParentIDs(Connection conn) throws SQLException {
        String updateSQL = "UPDATE " + TABLE_NAME + " SET parent_id = (" +
                "SELECT p2.target_file_id FROM " + TABLE_NAME + " p2 " +
                "WHERE p2.file_name = " + TABLE_NAME + ".parent_folder LIMIT 1" +
                ") WHERE parent_folder IS NOT NULL";

        try (Statement stmt = conn.createStatement()) {
            long startTime = System.currentTimeMillis();
            int updatedRows = stmt.executeUpdate(updateSQL);
            long duration = System.currentTimeMillis() - startTime;
            
            printSuccess("Updated parent IDs for " + String.format("%,d", updatedRows) + " records in " + duration + "ms");
        }
    }
    
    /**
     * Create database views for analysis
     */
    private static void createViews(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            
            // View for all files (source_file_size > 0)
            stmt.execute("CREATE VIEW IF NOT EXISTS files_view AS " +
                "SELECT * FROM " + TABLE_NAME + " WHERE source_file_size > 0");
            
            // View for all folders (source_file_size = 0)
            stmt.execute("CREATE VIEW IF NOT EXISTS folders_view AS " +
                "SELECT * FROM " + TABLE_NAME + " WHERE source_file_size = 0 OR source_file_size IS NULL");
            
            // Status aggregation view
            stmt.execute("CREATE VIEW IF NOT EXISTS status_summary AS " +
                "SELECT " +
                "  COALESCE(file_status, 'Unknown') as status_name, " +
                "  COUNT(*) as record_count, " +
                "  COUNT(CASE WHEN source_file_size > 0 THEN 1 END) as file_count, " +
                "  COUNT(CASE WHEN source_file_size = 0 OR source_file_size IS NULL THEN 1 END) as folder_count " +
                "FROM " + TABLE_NAME + " " +
                "GROUP BY COALESCE(file_status, 'Unknown') " +
                "ORDER BY record_count DESC");
            
            // Dynamic status views - get all unique statuses and create views
            try (ResultSet rs = stmt.executeQuery("SELECT DISTINCT COALESCE(file_status, 'Unknown') as status FROM " + TABLE_NAME + " WHERE file_status IS NOT NULL")) {
                while (rs.next()) {
                    String status = rs.getString("status");
                    String sanitizedStatus = sanitizeViewName(status);
                    
                    if (!sanitizedStatus.isEmpty()) {
                        String viewSQL = "CREATE VIEW IF NOT EXISTS status_" + sanitizedStatus + " AS " +
                            "SELECT * FROM " + TABLE_NAME + " WHERE file_status = '" + status.replace("'", "''") + "'";
                        stmt.execute(viewSQL);
                    }
                }
            }
            
            // Hierarchical query view - shows all children of a given parent ID
            stmt.execute("CREATE VIEW IF NOT EXISTS hierarchy_children AS " +
                "WITH RECURSIVE hierarchy_tree(id, file_name, target_file_id, parent_id, level, depth, path) AS (" +
                "  SELECT id, file_name, target_file_id, parent_id, level, 0 as depth, file_name as path " +
                "  FROM " + TABLE_NAME + " " +
                "  WHERE parent_id IS NULL " +
                "  UNION ALL " +
                "  SELECT t.id, t.file_name, t.target_file_id, t.parent_id, t.level, h.depth + 1, h.path || ' > ' || t.file_name " +
                "  FROM " + TABLE_NAME + " t " +
                "  INNER JOIN hierarchy_tree h ON t.parent_id = h.target_file_id " +
                ") " +
                "SELECT * FROM hierarchy_tree ORDER BY path");
            
            printSuccess("Database views created successfully");
        }
    }
    
    /**
     * Sanitize status name for use as view name
     */
    private static String sanitizeViewName(String status) {
        if (status == null || status.trim().isEmpty()) {
            return "unknown";
        }
        
        return status.toLowerCase()
                    .replaceAll("[^a-z0-9_]", "_")
                    .replaceAll("_+", "_")
                    .replaceAll("^_|_$", "");
    }
    
    /**
     * Extract job name from Excel filename (remove extension)
     */
    private static String extractJobName(String filename) {
        if (filename == null || filename.trim().isEmpty()) {
            return "Unknown";
        }
        
        // Remove file extension
        String jobName = filename.replaceAll("\\.(xlsx|xls)$", "");
        return jobName.trim();
    }
    
    /**
     * Utility methods
     */
    private static String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        DataFormatter formatter = new DataFormatter();
        return formatter.formatCellValue(cell);
    }
    
    private static int calculateLevel(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return 0;
        }
        
        String cleanPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        if (cleanPath.isEmpty()) {
            return 0;
        }
        
        return cleanPath.split("/").length;
    }
    
    private static String getParentFolder(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return null;
        }
        
        if (calculateLevel(filePath) <= 1) {
            return null;
        }
        
        int lastSlashIndex = filePath.lastIndexOf('/');
        if (lastSlashIndex > 0) {
            return filePath.substring(0, lastSlashIndex);
        }
        
        return null;
    }
    
    private static String convertExcelDateToSQLite(String excelDateStr) {
        if (excelDateStr == null || excelDateStr.trim().isEmpty()) {
            return null;
        }
        
        try {
            double excelDate = Double.parseDouble(excelDateStr.trim());
            if (excelDate == 0) {
                return null;
            }
            
            LocalDateTime excelEpoch = LocalDateTime.of(1899, 12, 30, 0, 0);
            
            long wholeDays = (long) excelDate;
            double fractionDay = excelDate - wholeDays;
            
            if (wholeDays > 59) {
                wholeDays -= 1;
            }
            
            LocalDateTime dateTime = excelEpoch.plusDays(wholeDays);
            long seconds = Math.round(fractionDay * 24 * 60 * 60);
            dateTime = dateTime.plusSeconds(seconds);
            
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private static String formatTime(long milliseconds) {
        long seconds = milliseconds / 1000;
        long minutes = seconds / 60;
        seconds = seconds % 60;
        
        if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        } else {
            return String.format("%ds", seconds);
        }
    }
    
    private static void displayFinalStatistics(Connection conn, int filesProcessed, int totalRowsProcessed, long applicationStartTime) throws SQLException {
        long applicationEndTime = System.currentTimeMillis();
        long totalApplicationTime = applicationEndTime - applicationStartTime;
        
        System.out.println();
        System.out.println("========================================");
        System.out.println("======== FINAL STATISTICS ========");
        System.out.println("========================================");
        System.out.println("Files processed: " + filesProcessed);
        System.out.println("Total application time: " + formatTime(totalApplicationTime));
        System.out.println("Total rows processed: " + String.format("%,d", totalRowsProcessed));
        
        try (Statement stmt = conn.createStatement()) {
            // Total records in database
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM " + TABLE_NAME)) {
                if (rs.next()) {
                    System.out.println("Total records in database: " + String.format("%,d", rs.getInt("total")));
                }
            }
            
            // Records by type
            try (ResultSet rs = stmt.executeQuery("SELECT " +
                "COUNT(CASE WHEN source_file_size > 0 THEN 1 END) as files, " +
                "COUNT(CASE WHEN source_file_size = 0 OR source_file_size IS NULL THEN 1 END) as folders " +
                "FROM " + TABLE_NAME)) {
                if (rs.next()) {
                    System.out.println("Files: " + String.format("%,d", rs.getInt("files")));
                    System.out.println("Folders: " + String.format("%,d", rs.getInt("folders")));
                }
            }
            
            // Top statuses
            System.out.println("\nTop 5 File Statuses:");
            try (ResultSet rs = stmt.executeQuery("SELECT status_name, record_count FROM status_summary LIMIT 5")) {
                while (rs.next()) {
                    System.out.println("  " + rs.getString("status_name") + ": " + String.format("%,d", rs.getInt("record_count")));
                }
            }
        }
        
        System.out.println("========================================");
        printSuccess("SQLite Direct Import completed successfully!");
    }
}