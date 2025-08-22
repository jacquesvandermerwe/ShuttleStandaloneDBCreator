///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.xerial:sqlite-jdbc:3.45.3.0
//DEPS org.apache.commons:commons-csv:1.10.0
//DEPS commons-io:commons-io:2.16.1

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.csv.*;
import org.apache.commons.io.FilenameUtils;

/**
 * FolderHierarchyImporter - SQLite Import Tool for Folder Objects with Parent Hierarchy
 * 
 * This JBang script scans the reports directory for Folder-Object-*.csv files,
 * creates a SQLite database, and imports the CSV data with additional hierarchical
 * information including parent folder relationships and folder depth levels.
 * 
 * Features:
 * - Discovers all Folder-Object-*.csv files in the reports directory structure
 * - Creates SQLite database with all original CSV columns plus hierarchical columns
 * - Calculates folder hierarchy levels based on path tokenization
 * - Identifies parent folders by removing the last path token
 * - Links parent folders using Target File ID for parent-child relationships
 * - Creates indexes for efficient querying
 * - Handles large datasets with transaction batching
 * 
 * Usage: jbang FolderHierarchyImporter.java [reports_directory]
 *
 * @version 1.0
 */
public class FolderHierarchyImporter {
    private static final String DATABASE_NAME = "folder_hierarchy.db";
    private static final String TABLE_NAME = "folder_objects";
    private static final int BATCH_SIZE = 1000; // Transaction batch size
    
    // Platform detection for emoji support
    private static final boolean SUPPORTS_EMOJI = !System.getProperty("os.name").toLowerCase().contains("windows");
    
    // Utility method for cross-platform output
    private static void printInfo(String message) {
        if (SUPPORTS_EMOJI) {
            System.out.println("🔍 " + message);
        } else {
            System.out.println("[INFO] " + message);
        }
    }
    
    private static void printSuccess(String message) {
        if (SUPPORTS_EMOJI) {
            System.out.println("📊 " + message);
        } else {
            System.out.println("[SUCCESS] " + message);
        }
    }
    
    private static void printProgress(String message) {
        if (SUPPORTS_EMOJI) {
            System.out.println("🏗️  " + message);
        } else {
            System.out.println("[PROGRESS] " + message);
        }
    }
    
    private static void printListItem(String message) {
        if (SUPPORTS_EMOJI) {
            System.out.println("   📋 " + message);
        } else {
            System.out.println("   - " + message);
        }
    }
    
    // Database column names (used for table creation and internal processing)
    private static final String[] DB_COLUMNS = {
        "File_Name", "Source_File_Size", "Target_File_Size", "Source_File_ID", "Target_File_ID",
        "Source_Account", "Source_Namespace", "Target_Account", "Source_Created_By", "Creation_Time",
        "Source_Last_Modified_By", "Source_Last_Modification_Time", "Target_Last_Modification_Time",
        "Last_Access_Time", "Start_Time", "Transfer_Time", "Checksum_Method", "Checksum",
        "File_Status", "Errors", "Status", "Translated_File_Name"
    };
    
    // CSV header names (exactly as they appear in the CSV files)
    private static final String[] CSV_HEADERS = {
        "File Name", "Source File Size", "Target File Size", "Source File ID", "Target File ID",
        "Source Account", "Source Namespace", "Target Account", "Source Created By", "Creation Time",
        "Source Last Modified By", "Source Last Modification Time", "Target Last Modification Time",
        "Last Access Time", "Start Time", "Transfer Time", "Checksum Method", "Checksum",
        "File Status", "Errors", "Status", "Translated File Name"
    };
    
    // Date/time columns that need conversion from Excel serial numbers (using DB column names)
    private static final Set<String> DATE_COLUMNS = Set.of(
        "Creation_Time", "Source_Last_Modification_Time", "Target_Last_Modification_Time",
        "Last_Access_Time", "Start_Time", "Transfer_Time"
    );
    
    public static void main(String[] args) {
        Path reportDirectory;
        if (args.length > 0) {
            reportDirectory = Paths.get(args[0]).resolve("report");
        } else {
            reportDirectory = Paths.get(".").resolve("report");
        }
        
        if (!Files.exists(reportDirectory)) {
            System.err.println("[ERROR] Report directory not found: " + reportDirectory);
            System.exit(1);
        }
        
        printInfo("Scanning for Folder-Object CSV files in: " + reportDirectory);
        
        try {
            List<Path> csvFiles = findFolderObjectCsvFiles(reportDirectory);
            if (csvFiles.isEmpty()) {
                printInfo("No Folder-Object-*.csv files found");
                return;
            }
            
            printInfo("Found " + csvFiles.size() + " Folder-Object CSV file(s):");
            csvFiles.forEach(file -> printListItem(file.toString()));
            
            // Create SQLite database and import data
            Path dbPath = reportDirectory.getParent().resolve(DATABASE_NAME);
            importToSQLite(csvFiles, dbPath);
            
        } catch (Exception e) {
            System.err.println("[ERROR] Error processing files: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Find all Folder-Object-*.csv files in the report directory structure
     */
    private static List<Path> findFolderObjectCsvFiles(Path reportDir) throws IOException {
        List<Path> csvFiles = new ArrayList<>();
        
        try (var stream = Files.walk(reportDir)) {
            csvFiles = stream
                .filter(Files::isRegularFile)
                .filter(path -> {
                    String fileName = path.getFileName().toString();
                    return fileName.startsWith("Folder-Object-") && fileName.endsWith(".csv");
                })
                .collect(Collectors.toList());
        }
        
        return csvFiles;
    }
    
    /**
     * Import CSV files to SQLite database with hierarchical processing
     */
    private static void importToSQLite(List<Path> csvFiles, Path dbPath) throws Exception {
        boolean isNewDatabase = !Files.exists(dbPath);
        
        if (isNewDatabase) {
            System.out.println("💾 Creating new SQLite database: " + dbPath);
        } else {
            System.out.println("🔄 Updating existing SQLite database: " + dbPath);
        }
        
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            // Enable foreign key constraints and performance optimizations
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("PRAGMA foreign_keys = ON");
                stmt.execute("PRAGMA synchronous = NORMAL");
                stmt.execute("PRAGMA cache_size = -2000000"); // 2GB cache
                stmt.execute("PRAGMA temp_store = MEMORY");
                stmt.execute("PRAGMA journal_mode = WAL");
            }
            
            // Create table with all CSV columns plus hierarchical columns (if not exists)
            createTable(conn, isNewDatabase);
            
            // Import all CSV files
            int totalRowsProcessed = 0;
            int totalRowsInserted = 0;
            int totalRowsUpdated = 0;
            
            for (Path csvFile : csvFiles) {
                System.out.println("📥 Processing: " + csvFile.getFileName());
                
                // Get row count before import
                int rowsBefore = getRowCount(conn);
                
                int rowsProcessed = importCsvFile(conn, csvFile);
                totalRowsProcessed += rowsProcessed;
                
                // Get row count after import
                int rowsAfter = getRowCount(conn);
                int newRows = rowsAfter - rowsBefore;
                int updatedRows = rowsProcessed - newRows;
                
                totalRowsInserted += newRows;
                totalRowsUpdated += updatedRows;
                
                System.out.println("   ✅ Processed " + String.format("%,d", rowsProcessed) + 
                                 " rows (" + String.format("%,d", newRows) + " new, " + 
                                 String.format("%,d", updatedRows) + " updated)");
            }
            
            printSuccess("Total rows processed: " + String.format("%,d", totalRowsProcessed) + 
                             " (" + String.format("%,d", totalRowsInserted) + " new, " + 
                             String.format("%,d", totalRowsUpdated) + " updated)");
            
            // Calculate parent IDs for all records (LEVEL and PARENT_FOLDER are set during import)
            printInfo("Calculating parent ID relationships...");
            calculateParentIDs(conn);
            
            // Create indexes for performance
            createIndexes(conn);
            
            System.out.println("✅ SQLite import completed successfully!");
            System.out.println("📍 Database location: " + dbPath.toAbsolutePath());
            
            // Display sample statistics
            displayStatistics(conn);
            
        } catch (SQLException e) {
            System.err.println("[ERROR] Database error: " + e.getMessage());
            throw e;
        }
    }
    
    /**
     * Create the folder_objects table with all columns including hierarchical ones
     */
    private static void createTable(Connection conn, boolean isNewDatabase) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ");
        if (!isNewDatabase) {
            createTableSQL.append("IF NOT EXISTS ");
        }
        createTableSQL.append(TABLE_NAME).append(" (");
        createTableSQL.append("id INTEGER PRIMARY KEY AUTOINCREMENT, ");
        
        // Add all original CSV columns with appropriate data types
        for (String column : DB_COLUMNS) {
            if (DATE_COLUMNS.contains(column)) {
                createTableSQL.append(column).append(" DATETIME, ");
            } else if (column.equals("Source_File_Size") || column.equals("Target_File_Size")) {
                createTableSQL.append(column).append(" BIGINT, ");
            } else {
                createTableSQL.append(column).append(" TEXT, ");
            }
        }
        
        // Add hierarchical columns
        createTableSQL.append("PARENT_FOLDER TEXT, ");
        createTableSQL.append("PARENT_ID TEXT, ");
        createTableSQL.append("LEVEL INTEGER, ");
        createTableSQL.append("import_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, ");
        
        // Add unique constraint to prevent duplicates
        createTableSQL.append("UNIQUE(File_Name, Target_File_ID)");
        createTableSQL.append(")");
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL.toString());
            printProgress("Created table: " + TABLE_NAME);
        }
    }
    
    /**
     * Import a single CSV file into the database
     */
    private static int importCsvFile(Connection conn, Path csvFile) throws Exception {
        String insertSQL = buildInsertSQL();
        
        int rowCount = 0;
        conn.setAutoCommit(false); // Use transactions for better performance
        
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL);
             Reader reader = Files.newBufferedReader(csvFile);
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            
            for (CSVRecord record : parser) {
                // Set all original CSV columns with appropriate data type conversion
                for (int i = 0; i < DB_COLUMNS.length; i++) {
                    String csvHeaderName = CSV_HEADERS[i]; // Use exact CSV header name
                    String dbColumnName = DB_COLUMNS[i];   // Use database column name for logic
                    String value = record.get(csvHeaderName);
                    
                    if (DATE_COLUMNS.contains(dbColumnName)) {
                        // Convert Excel date to SQLite datetime
                        String convertedDate = convertExcelDateToSQLite(value);
                        if (convertedDate != null) {
                            pstmt.setString(i + 1, convertedDate);
                        } else {
                            pstmt.setNull(i + 1, Types.VARCHAR);
                        }
                    } else if (dbColumnName.equals("Source_File_Size") || dbColumnName.equals("Target_File_Size")) {
                        // Convert file sizes to integers
                        try {
                            if (value != null && !value.trim().isEmpty()) {
                                long fileSize = Long.parseLong(value.trim());
                                pstmt.setLong(i + 1, fileSize);
                            } else {
                                pstmt.setNull(i + 1, Types.BIGINT);
                            }
                        } catch (NumberFormatException e) {
                            pstmt.setNull(i + 1, Types.BIGINT);
                        }
                    } else {
                        // Regular text fields
                        pstmt.setString(i + 1, value);
                    }
                }
                
                // Initialize hierarchical columns (will be calculated later)
                String fileName = record.get("File Name");
                int level = calculateLevel(fileName);
                String parentFolder = getParentFolder(fileName);
                
                pstmt.setString(DB_COLUMNS.length + 1, parentFolder); // PARENT_FOLDER
                pstmt.setString(DB_COLUMNS.length + 2, null); // PARENT_ID (will be set in second pass)
                pstmt.setInt(DB_COLUMNS.length + 3, level); // LEVEL
                
                pstmt.addBatch();
                rowCount++;
                
                // Execute batch every BATCH_SIZE records
                if (rowCount % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.print(".");
                    System.out.flush();
                }
            }
            
            // Execute remaining batch
            pstmt.executeBatch();
            conn.commit();
            System.out.println(); // New line after progress dots
            
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
        
        return rowCount;
    }
    
    /**
     * Build the UPSERT SQL statement for all columns
     */
    private static String buildInsertSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT OR REPLACE INTO ").append(TABLE_NAME).append(" (");
        
        // Add all column names
        for (String column : DB_COLUMNS) {
            sql.append(column).append(", ");
        }
        sql.append("PARENT_FOLDER, PARENT_ID, LEVEL) VALUES (");
        
        // Add placeholders
        for (int i = 0; i < DB_COLUMNS.length + 3; i++) {
            sql.append("?, ");
        }
        sql.setLength(sql.length() - 2); // Remove last comma and space
        sql.append(")");
        
        return sql.toString();
    }
    
    /**
     * Calculate parent IDs for records that have parent folders
     */
    private static void calculateParentIDs(Connection conn) throws SQLException {
        updateParentIDs(conn);
        System.out.println("✅ Parent ID calculation completed");
    }
    
    /**
     * Update PARENT_ID for records that have parent folders
     * Uses optimized single UPDATE query with subquery to avoid N+1 performance problem
     */
    private static void updateParentIDs(Connection conn) throws SQLException {
        // Single UPDATE with subquery - much more efficient than N+1 loop approach
        String updateSQL = "UPDATE " + TABLE_NAME + " SET PARENT_ID = (" +
                "SELECT p2.Target_File_ID FROM " + TABLE_NAME + " p2 " +
                "WHERE p2.File_Name = " + TABLE_NAME + ".PARENT_FOLDER LIMIT 1" +
                ") WHERE PARENT_FOLDER IS NOT NULL";

        try (Statement stmt = conn.createStatement()) {
            long startTime = System.currentTimeMillis();
            int updatedRows = stmt.executeUpdate(updateSQL);
            long duration = System.currentTimeMillis() - startTime;
            System.out.println(); // New line after any previous progress dots
            
            if (SUPPORTS_EMOJI) {
                System.out.println("👨‍👩‍👧‍👦 Updated parent IDs for " + String.format("%,d", updatedRows) + " records in " + duration + "ms");
            } else {
                System.out.println("[SUCCESS] Updated parent IDs for " + String.format("%,d", updatedRows) + " records in " + duration + "ms");
            }
            
        } catch (Exception e) {
            // In auto-commit mode, each statement is its own transaction
            // Database handles rollbacks for failed statements automatically
            throw e;
        }
    }
    
    /**
     * Calculate the level (depth) of a folder path based on token count
     */
    private static int calculateLevel(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return 0;
        }
        
        // Remove leading slash and split by '/'
        String cleanPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        if (cleanPath.isEmpty()) {
            return 0;
        }
        
        String[] tokens = cleanPath.split("/");
        return tokens.length;
    }
    
    /**
     * Get the parent folder path by removing the last token
     */
    private static String getParentFolder(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return null;
        }
        
        // For level 1 folders (e.g., "/Clients"), there's no parent
        if (calculateLevel(filePath) <= 1) {
            return null;
        }
        
        // Remove the last path segment
        int lastSlashIndex = filePath.lastIndexOf('/');
        if (lastSlashIndex > 0) {
            return filePath.substring(0, lastSlashIndex);
        }
        
        return null;
    }
    
    /**
     * Create indexes for efficient querying
     */
    private static void createIndexes(Connection conn) throws SQLException {
        printInfo("Creating database indexes...");
        
        String[] indexQueries = {
            "CREATE INDEX IF NOT EXISTS idx_file_name ON " + TABLE_NAME + " (File_Name)",
            "CREATE INDEX IF NOT EXISTS idx_target_file_id ON " + TABLE_NAME + " (Target_File_ID)",
            "CREATE INDEX IF NOT EXISTS idx_parent_folder ON " + TABLE_NAME + " (PARENT_FOLDER)",
            "CREATE INDEX IF NOT EXISTS idx_parent_id ON " + TABLE_NAME + " (PARENT_ID)",
            "CREATE INDEX IF NOT EXISTS idx_level ON " + TABLE_NAME + " (LEVEL)",
            "CREATE INDEX IF NOT EXISTS idx_file_status ON " + TABLE_NAME + " (File_Status)"
        };
        
        try (Statement stmt = conn.createStatement()) {
            for (String indexQuery : indexQueries) {
                stmt.execute(indexQuery);
                if (SUPPORTS_EMOJI) {
                    System.out.println("   ✅ " + indexQuery.split(" ")[2]); // Extract index name
                } else {
                    System.out.println("   [OK] " + indexQuery.split(" ")[2]); // Extract index name
                }
            }
        }
        
        if (SUPPORTS_EMOJI) {
            System.out.println("🚀 Database indexes created for optimal query performance");
        } else {
            System.out.println("[SUCCESS] Database indexes created for optimal query performance");
        }
        
        // Create analytical views
        createAnalyticalViews(conn);
    }
    
    /**
     * Create analytical views for data analysis
     */
    private static void createAnalyticalViews(Connection conn) throws SQLException {
        printInfo("Creating analytical views...");
        
        boolean isWindows = System.getProperty("os.name").toLowerCase().contains("windows");
        
        try (Statement stmt = conn.createStatement()) {
            // Customer View
            String customerView = "CREATE VIEW IF NOT EXISTS Customer AS " +
                "SELECT id, File_Name, " +
                "SUBSTR(File_Name, 10) AS Customer_Name, " +
                "Target_File_ID, Source_File_Size, Target_File_Size, Source_Account, Target_Account, " +
                "Source_Created_By, Creation_Time, Source_Last_Modified_By, " +
                "Source_Last_Modification_Time, Target_Last_Modification_Time, " +
                "Last_Access_Time, Start_Time, Transfer_Time, File_Status, Status, " +
                "Translated_File_Name, PARENT_FOLDER, PARENT_ID, LEVEL, import_timestamp " +
                "FROM " + TABLE_NAME + " " +
                "WHERE LEVEL = 2 AND File_Name LIKE '/Clients/%' AND File_Name NOT LIKE '/Clients/%/%' " +
                "ORDER BY Customer_Name";
            
            stmt.execute(customerView);
            printListItem("Customer view (Level 2 folders)");
            
            // Claims View  
            String claimsView = "CREATE VIEW IF NOT EXISTS Claims AS " +
                "SELECT id, File_Name, " +
                "SUBSTR(File_Name, 10, INSTR(SUBSTR(File_Name, 10), '/') - 1) AS Customer_Name, " +
                "SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/'), " +
                "INSTR(SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')), '/') - 1) AS Policy_Reference, " +
                "SUBSTR(File_Name, INSTR(File_Name, '/Claim Documents/') + 17) AS Claim_Number, " +
                "Target_File_ID, Source_File_Size, Target_File_Size, Source_Account, Target_Account, " +
                "Source_Created_By, Creation_Time, Source_Last_Modified_By, " +
                "Source_Last_Modification_Time, Target_Last_Modification_Time, " +
                "Last_Access_Time, Start_Time, Transfer_Time, File_Status, Status, " +
                "Translated_File_Name, PARENT_FOLDER, PARENT_ID, LEVEL, import_timestamp " +
                "FROM " + TABLE_NAME + " " +
                "WHERE LEVEL = 5 AND File_Name LIKE '/Clients/%/%/Claim Documents/%' " +
                "ORDER BY Customer_Name, Policy_Reference, Claim_Number";
            
            stmt.execute(claimsView);
            printListItem("Claims view (Level 5 claim folders)");
            
            // Policy Reference View - Windows compatible version
            String policyView;
            if (isWindows) {
                // Windows-compatible version using simple LIKE patterns for numeric validation
                policyView = "CREATE VIEW IF NOT EXISTS Policy_Reference AS " +
                    "SELECT id, File_Name, " +
                    "SUBSTR(File_Name, 10, INSTR(SUBSTR(File_Name, 10), '/') - 1) AS Customer_Name, " +
                    "SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) AS Policy_Reference, " +
                    "Target_File_ID, Source_File_Size, Target_File_Size, Source_Account, Target_Account, " +
                    "Source_Created_By, Creation_Time, Source_Last_Modified_By, " +
                    "Source_Last_Modification_Time, Target_Last_Modification_Time, " +
                    "Last_Access_Time, Start_Time, Transfer_Time, File_Status, Status, " +
                    "Translated_File_Name, PARENT_FOLDER, PARENT_ID, LEVEL, import_timestamp " +
                    "FROM " + TABLE_NAME + " " +
                    "WHERE LEVEL = 3 AND File_Name LIKE '/Clients/%/%' AND File_Name NOT LIKE '/Clients/%/%/%' " +
                    // Windows-compatible numeric validation: checks that the string contains only digits and dots
                    "AND trim(SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')), '0123456789.') = '' " +
                    // Exclude obvious text patterns
                    "AND SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) NOT LIKE '%General%' " +
                    "AND SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) NOT LIKE '%Documents%' " +
                    "AND SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) NOT LIKE '%Correspondence%' " +
                    "AND SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) NOT LIKE '%Folder%' " +
                    "ORDER BY Customer_Name, Policy_Reference";
            } else {
                // Unix/Linux/macOS version using REGEXP
                policyView = "CREATE VIEW IF NOT EXISTS Policy_Reference AS " +
                    "SELECT id, File_Name, " +
                    "SUBSTR(File_Name, 10, INSTR(SUBSTR(File_Name, 10), '/') - 1) AS Customer_Name, " +
                    "SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) AS Policy_Reference, " +
                    "Target_File_ID, Source_File_Size, Target_File_Size, Source_Account, Target_Account, " +
                    "Source_Created_By, Creation_Time, Source_Last_Modified_By, " +
                    "Source_Last_Modification_Time, Target_Last_Modification_Time, " +
                    "Last_Access_Time, Start_Time, Transfer_Time, File_Status, Status, " +
                    "Translated_File_Name, PARENT_FOLDER, PARENT_ID, LEVEL, import_timestamp " +
                    "FROM " + TABLE_NAME + " " +
                    "WHERE LEVEL = 3 AND File_Name LIKE '/Clients/%/%' AND File_Name NOT LIKE '/Clients/%/%/%' " +
                    "AND SUBSTR(File_Name, 10 + INSTR(SUBSTR(File_Name, 10), '/')) REGEXP '^[0-9.]+$' " +
                    "ORDER BY Customer_Name, Policy_Reference";
            }
            
            stmt.execute(policyView);
            printListItem("Policy_Reference view (Level 3 numeric policy folders) - " + 
                         (isWindows ? "Windows compatible" : "REGEXP enabled"));
        }
        
        printSuccess("Analytical views created successfully");
    }
    
    /**
     * Get the current row count in the database (returns 0 if table doesn't exist)
     */
    private static int getRowCount(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            // Table doesn't exist yet
            return 0;
        }
    }
    
    /**
     * Convert Excel serial date number to SQLite datetime string
     * Excel dates start from January 1, 1900 (with a leap year bug - day 60 is Feb 29, 1900)
     */
    private static String convertExcelDateToSQLite(String excelDateStr) {
        if (excelDateStr == null || excelDateStr.trim().isEmpty()) {
            return null;
        }
        
        try {
            double excelDate = Double.parseDouble(excelDateStr.trim());
            if (excelDate == 0) {
                return null;
            }
            
            // Excel epoch starts at 1900-01-01, but Excel treats 1900 as a leap year (it's not)
            // So we need to account for this. Excel day 1 = 1900-01-01, day 60 = 1900-02-29 (invalid)
            LocalDateTime excelEpoch = LocalDateTime.of(1899, 12, 30, 0, 0); // Day 1 = 1900-01-01
            
            long wholeDays = (long) excelDate;
            double fractionDay = excelDate - wholeDays;
            
            // Account for Excel's leap year bug (day 60 doesn't exist but Excel thinks it does)
            if (wholeDays > 59) {
                wholeDays -= 1; // Subtract one day to account for the non-existent Feb 29, 1900
            }
            
            LocalDateTime dateTime = excelEpoch.plusDays(wholeDays);
            
            // Add the time portion (fraction of day)
            long seconds = Math.round(fractionDay * 24 * 60 * 60);
            dateTime = dateTime.plusSeconds(seconds);
            
            // Format as SQLite datetime string
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            
        } catch (NumberFormatException e) {
            return null; // Return null for invalid numbers
        }
    }
    
    /**
     * Display database statistics
     */
    private static void displayStatistics(Connection conn) throws SQLException {
        System.out.println("\n📈 Database Statistics:");
        System.out.println("═══════════════════════════════════════");
        
        try (Statement stmt = conn.createStatement()) {
            // Total records
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM " + TABLE_NAME)) {
                if (rs.next()) {
                    printSuccess("Total records: " + String.format("%,d", rs.getInt("total")));
                }
            }
            
            // Records by level
            try (ResultSet rs = stmt.executeQuery("SELECT LEVEL, COUNT(*) as count FROM " + TABLE_NAME + " GROUP BY LEVEL ORDER BY LEVEL")) {
                System.out.println("📏 Records by folder level:");
                while (rs.next()) {
                    System.out.println("   Level " + rs.getInt("LEVEL") + ": " + String.format("%,d", rs.getInt("count")) + " folders");
                }
            }
            
            // Records with parents
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as with_parents FROM " + TABLE_NAME + " WHERE PARENT_FOLDER IS NOT NULL")) {
                if (rs.next()) {
                    System.out.println("👨‍👩‍👧‍👦 Records with parent relationships: " + String.format("%,d", rs.getInt("with_parents")));
                }
            }
            
            // Top-level folders
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as root_folders FROM " + TABLE_NAME + " WHERE LEVEL = 1")) {
                if (rs.next()) {
                    printInfo("Root-level folders: " + String.format("%,d", rs.getInt("root_folders")));
                }
            }
        }
        
        System.out.println("═══════════════════════════════════════");
        System.out.println("💡 Use SQL queries to explore the hierarchy:");
        System.out.println("   SELECT * FROM folder_objects WHERE LEVEL = 1; -- Root folders");
        System.out.println("   SELECT * FROM folder_objects WHERE PARENT_FOLDER = '/Clients'; -- Client subfolders");
        System.out.println("   SELECT File_Name, PARENT_FOLDER, LEVEL FROM folder_objects ORDER BY LEVEL, File_Name;");
    }
}