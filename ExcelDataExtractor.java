///usr/bin/env jbang "$0" "$@" ; exit $?

// JVM Options for handling large Excel files
//JAVA_OPTIONS -Xmx8g -XX:+UseG1GC -Djdk.xml.maxGeneralEntitySizeLimit=0 -Djdk.xml.totalEntitySizeLimit=0 -Djdk.xml.maxParameterEntitySizeLimit=0 -Djdk.xml.entityExpansionLimit=0 -Djdk.xml.maxElementDepth=0

// Dependencies for Excel processing
//DEPS org.apache.poi:poi:5.3.0
//DEPS org.apache.poi:poi-ooxml:5.3.0
//DEPS org.apache.poi:poi-scratchpad:5.3.0
//DEPS commons-io:commons-io:2.16.1
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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Excel Data Extractor for extracting comprehensive data from Transfer Report sheets.
 * 
 * This application processes Excel files containing "Transfer Report" sheets and extracts
 * all rows, categorizing them into different CSV files based on criteria:
 * 
 * 1. Folder Objects: File Source Size = 0 → Folder-Object-[filename].csv
 * 2. File Objects: File Source Size > 0 → File-Object-[filename].csv  
 * 3. Status Groups: Unique File Status values (Column S) → [STATUS]-Status-[filename].csv
 * 
 * Uses streaming processing for memory efficiency with large files (700MB+).
 */
public class ExcelDataExtractor {
    
    /**
     * Main entry point for the Excel data extractor.
     * Processes Excel files in the specified directory (or current directory if none specified).
     * 
     * @param args Command line arguments - optional directory path
     */
    public static void main(String[] args) throws Exception {
        // Start timing the entire application
        long applicationStartTime = System.currentTimeMillis();
        
        // Increase Apache POI memory limit for large Excel files (1GB limit)
        IOUtils.setByteArrayMaxOverride(1024 * 1024 * 1024);
        
        String folderPath = args.length > 0 ? args[0] : ".";
        
        List<Path> excelFiles = findExcelFiles(Paths.get(folderPath));
        System.out.println("Found " + excelFiles.size() + " Excel files");
        
        // Statistics tracking
        int filesProcessed = 0;
        int totalRowsProcessed = 0;
        int totalFoldersFound = 0;
        int totalFilesFound = 0;
        int totalClaimsFound = 0;
        int totalCustomerFoldersFound = 0;
        int totalPolicyReferencesFound = 0;
        int totalStatusGroupsFound = 0;
        
        // Process files sequentially to avoid memory issues with large files
        for (Path file : excelFiles) {
            try {
                System.out.println("Processing file: " + file.getFileName());
                long startTime = System.currentTimeMillis();
                
                FileStats stats = processExcelFile(file);
                filesProcessed++;
                totalRowsProcessed += stats.rowsProcessed;
                totalFoldersFound += stats.foldersFound;
                totalFilesFound += stats.filesFound;
                totalClaimsFound += stats.claimsFound;
                totalCustomerFoldersFound += stats.customerFoldersFound;
                totalPolicyReferencesFound += stats.policyReferencesFound;
                totalStatusGroupsFound += stats.statusGroups.size();
                
                long endTime = System.currentTimeMillis();
                long processingTime = endTime - startTime;
                System.out.println("Completed file: " + file.getFileName() + " in " + formatTime(processingTime));
                
                // Force garbage collection after each file to free memory
                System.gc();
            } catch (Exception e) {
                System.err.println("Error processing " + file + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        System.out.println("Processing completed");
        
        // Calculate total application time
        long applicationEndTime = System.currentTimeMillis();
        long totalApplicationTime = applicationEndTime - applicationStartTime;
        
        // Print final statistics
        System.out.println();
        System.out.println("========================================");
        System.out.println("======== FINAL STATISTICS ========");
        System.out.println("========================================");
        System.out.println("📁 Files processed: " + filesProcessed + " of " + excelFiles.size());
        System.out.println("⏱️  Total application time: " + formatTime(totalApplicationTime));
        System.out.println("📄 Total rows processed: " + String.format("%,d", totalRowsProcessed));
        System.out.println("📂 Total folder objects found: " + String.format("%,d", totalFoldersFound));
        System.out.println("📋 Total file objects found: " + String.format("%,d", totalFilesFound));
        System.out.println("💼 Total claims found: " + String.format("%,d", totalClaimsFound));
        System.out.println("👥 Total customer folders found: " + String.format("%,d", totalCustomerFoldersFound));
        System.out.println("📋 Total policy references found: " + String.format("%,d", totalPolicyReferencesFound));
        System.out.println("🏷️  Total status groups found: " + String.format("%,d", totalStatusGroupsFound));
        System.out.println("========================================");
    }
    
    /**
     * Pattern matching for Claims folders: /Clients/AAA/BBB/Claim Documents/CCC where AAA is customer name, BBB is claim number, CCC is claim sub identifier
     */
    private static final Pattern CLAIMS_PATTERN = Pattern.compile(".*/Clients/[^/]+/[^/]+/Claim Documents/[^/]+/?$");
    
    /**
     * Pattern matching for Customer Folders: /Clients/XXX where XXX is the customer name (no children)
     */
    private static final Pattern CUSTOMER_FOLDERS_PATTERN = Pattern.compile(".*/Clients/[^/]+/?$");
    
    /**
     * Pattern matching for Policy Reference: /Clients/XXX/YYY where XXX is customer name and YYY is policy reference (no children)
     */
    private static final Pattern POLICY_REFERENCE_PATTERN = Pattern.compile(".*/Clients/[^/]+/[^/]+/?$");
    
    /**
     * Check if a file path matches the Claims pattern
     * @param filePath The file path to check
     * @return true if matches Claims pattern
     */
    private static boolean matchesClaimsPattern(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        return CLAIMS_PATTERN.matcher(filePath).matches();
    }
    
    /**
     * Check if a file path matches the Customer Folders pattern
     * @param filePath The file path to check
     * @return true if matches Customer Folders pattern
     */
    private static boolean matchesCustomerFoldersPattern(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        return CUSTOMER_FOLDERS_PATTERN.matcher(filePath).matches();
    }
    
    /**
     * Check if a file path matches the Policy Reference pattern
     * @param filePath The file path to check
     * @return true if it matches the pattern /Clients/XXX/YYY (customer/policy reference)
     */
    private static boolean matchesPolicyReferencePattern(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        return POLICY_REFERENCE_PATTERN.matcher(filePath).matches();
    }
    
    /**
     * Enhanced statistics holder for file processing results
     */
    private static class FileStats {
        final int rowsProcessed;
        final int foldersFound;
        final int filesFound;
        final int claimsFound;
        final int customerFoldersFound;
        final int policyReferencesFound;
        final Set<String> statusGroups;
        
        FileStats(int rowsProcessed, int foldersFound, int filesFound, int claimsFound, int customerFoldersFound, int policyReferencesFound, Set<String> statusGroups) {
            this.rowsProcessed = rowsProcessed;
            this.foldersFound = foldersFound;
            this.filesFound = filesFound;
            this.claimsFound = claimsFound;
            this.customerFoldersFound = customerFoldersFound;
            this.policyReferencesFound = policyReferencesFound;
            this.statusGroups = statusGroups;
        }
    }
    
    /**
     * Result holder for sheet processing that includes header row
     */
    private static class SheetProcessingResult {
        final int rowsProcessed;
        final String[] headerRow;
        
        SheetProcessingResult(int rowsProcessed, String[] headerRow) {
            this.rowsProcessed = rowsProcessed;
            this.headerRow = headerRow;
        }
    }
    
    /**
     * Format milliseconds into a human-readable time string
     */
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
    
    /**
     * Find all Excel files in the specified directory (non-recursive).
     * Excludes temporary Excel files that start with ~$ or ~.
     * 
     * @param directory The directory to search
     * @return List of Excel file paths
     */
    private static List<Path> findExcelFiles(Path directory) throws IOException {
        List<Path> excelFiles = new ArrayList<>();
        
        // Only search the specified directory, not subdirectories
        try (var stream = Files.list(directory)) {
            stream.filter(Files::isRegularFile)
                  .forEach(file -> {
                      String fileName = file.getFileName().toString();
                      String lowerFileName = fileName.toLowerCase();
                      
                      // Skip temporary Excel files (start with ~$ or ~)
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
     * Process a single Excel file using the appropriate method based on file type.
     * XLSX files use streaming processing for memory efficiency.
     * XLS files use traditional processing.
     * 
     * @param filePath Path to the Excel file to process
     * @return FileStats containing processing statistics
     */
    private static FileStats processExcelFile(Path filePath) throws Exception {
        System.out.println("⚙️  Processing: " + filePath);
        
        FileStats stats;
        if (filePath.toString().toLowerCase().endsWith(".xlsx")) {
            System.out.println("🌊 Using streaming XLSX processing...");
            stats = processXLSXStreaming(filePath);
        } else {
            System.out.println("📋 Using traditional XLS processing...");
            stats = processXLSTraditional(filePath);
        }
        
        // Clear references to help garbage collection
        System.gc();
        return stats;
    }
    
    /**
     * Process XLSX files using Apache POI's streaming API for memory efficiency.
     * Creates CSV files as data is found and writes rows immediately.
     * 
     * @param filePath Path to the XLSX file to process
     * @return FileStats containing processing statistics
     */
    private static FileStats processXLSXStreaming(Path filePath) throws Exception {
        // Initialize CSV writers for hybrid approach (write after each sheet)
        PrintWriter folderWriter = null;
        PrintWriter fileWriter = null;
        PrintWriter claimsWriter = null;
        PrintWriter customerFoldersWriter = null;
        PrintWriter policyReferenceWriter = null;
        Map<String, PrintWriter> statusWriters = new HashMap<>();
        
        boolean folderFileCreated = false;
        boolean fileFileCreated = false;
        boolean claimsFileCreated = false;
        boolean policyReferenceFileCreated = false;
        boolean customerFoldersFileCreated = false;
        Set<String> statusFilesCreated = new HashSet<>();
        
        int totalFolderRows = 0;
        int totalFileRows = 0;
        int totalClaimsRows = 0;
        int totalCustomerFoldersRows = 0;
        int totalPolicyReferenceRows = 0;
        Map<String, Integer> statusRowCounts = new HashMap<>();
        int totalRowsProcessed = 0;
        
        try (OPCPackage pkg = OPCPackage.open(filePath.toFile())) {
            System.out.println("📦 OPC Package opened successfully");
            XSSFReader reader = new XSSFReader(pkg);
            System.out.println("🔧 XSSFReader created");
            SharedStrings sst = reader.getSharedStringsTable();
            System.out.println("📚 SharedStrings table loaded");
            
            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            System.out.println("📑 Sheet iterator created");
            
            int sheetCount = 0;
            
            while (sheets.hasNext()) {
                sheetCount++;
                try (InputStream sheet = sheets.next()) {
                    String sheetName = sheets.getSheetName();
                    System.out.println("📋 Found sheet " + sheetCount + ": '" + sheetName + "'");
                    
                    if (sheetName.startsWith("Transfer Report")) {
                        System.out.println("⚡ Processing sheet: " + sheetName);
                        
                        // Create CSV writers on first Transfer Report sheet
                        if (folderWriter == null) {
                            String folderCsvPath = getCsvPath(filePath, "Folder-Object-");
                            folderWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(folderCsvPath)));
                            folderFileCreated = true;
                        }
                        if (fileWriter == null) {
                            String fileCsvPath = getCsvPath(filePath, "File-Object-");
                            fileWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(fileCsvPath)));
                            fileFileCreated = true;
                        }
                        if (claimsWriter == null) {
                            String claimsCsvPath = getCsvPath(filePath, "Claims-");
                            claimsWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(claimsCsvPath)));
                            claimsFileCreated = true;
                        }
                        if (customerFoldersWriter == null) {
                            String customerFoldersCsvPath = getCsvPath(filePath, "Customer-Folders-");
                            customerFoldersWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(customerFoldersCsvPath)));
                            customerFoldersFileCreated = true;
                        }
                        if (policyReferenceWriter == null) {
                            String policyReferenceCsvPath = getCsvPath(filePath, "Policy-Reference-");
                            policyReferenceWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(policyReferenceCsvPath)));
                            policyReferenceFileCreated = true;
                        }
                        
                        StreamingSheetHandler handler = new StreamingSheetHandler(folderWriter, fileWriter, claimsWriter, customerFoldersWriter, policyReferenceWriter, statusWriters, filePath, statusFilesCreated);
                        processSheet(sheet, sst, handler);
                        
                        // Flush after each sheet to ensure data is written
                        folderWriter.flush();
                        fileWriter.flush();
                        claimsWriter.flush();
                        customerFoldersWriter.flush();
                        policyReferenceWriter.flush();
                        for (PrintWriter statusWriter : statusWriters.values()) {
                            statusWriter.flush();
                        }
                        
                        totalFolderRows += handler.getFolderRowsWritten();
                        totalFileRows += handler.getFileRowsWritten();
                        totalClaimsRows += handler.getClaimsRowsWritten();
                        totalCustomerFoldersRows += handler.getCustomerFoldersRowsWritten();
                        totalPolicyReferenceRows += handler.getPolicyReferenceRowsWritten();
                        for (Map.Entry<String, Integer> entry : handler.getStatusRowCounts().entrySet()) {
                            statusRowCounts.merge(entry.getKey(), entry.getValue(), Integer::sum);
                        }
                        totalRowsProcessed += handler.getTotalRowsProcessed();
                        
                        System.out.println("✨ Sheet '" + sheetName + "' completed: " + handler.getTotalRowsProcessed() + " rows processed, " + handler.getExtractedRows() + " rows extracted");
                    } else {
                        System.out.println("⏩ Skipping sheet (not Transfer Report): " + sheetName);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing sheet " + sheetCount + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
            System.out.println("📊 Total sheets processed: " + sheetCount);
            
            System.out.println("🎯 Extraction complete. Found " + totalFolderRows + " folder objects, " + totalFileRows + " file objects, " + totalClaimsRows + " claims, " + totalCustomerFoldersRows + " customer folders, " + totalPolicyReferenceRows + " policy references, and " + statusRowCounts.size() + " unique status groups");
            
            if (totalFolderRows > 0) {
                System.out.println("💾 Created Folder Object CSV with " + totalFolderRows + " rows");
            }
            
            if (totalFileRows > 0) {
                System.out.println("💾 Created File Object CSV with " + totalFileRows + " rows");
            }
            
            if (totalClaimsRows > 0) {
                System.out.println("💾 Created Claims CSV with " + totalClaimsRows + " rows");
            }
            
            if (totalCustomerFoldersRows > 0) {
                System.out.println("💾 Created Customer Folders CSV with " + totalCustomerFoldersRows + " rows");
            }
            
            if (totalPolicyReferenceRows > 0) {
                System.out.println("💾 Created Policy Reference CSV with " + totalPolicyReferenceRows + " rows");
            }
            
            for (Map.Entry<String, Integer> entry : statusRowCounts.entrySet()) {
                System.out.println("💾 Created Status CSV '" + entry.getKey() + "' with " + entry.getValue() + " rows");
            }
            
            if (totalFolderRows == 0 && totalFileRows == 0 && totalClaimsRows == 0 && totalCustomerFoldersRows == 0 && statusRowCounts.isEmpty()) {
                System.out.println("No matching rows found - no CSV files created");
            }
            
        } catch (Exception e) {
            System.err.println("Error in streaming processing: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            System.out.println("🔧 Finalizing CSV files and cleaning up resources...");
            
            // Close CSV writers and ensure all data is written to disk
            if (folderWriter != null) {
                folderWriter.close();
            }
            if (fileWriter != null) {
                fileWriter.close();
            }
            if (claimsWriter != null) {
                claimsWriter.close();
            }
            if (customerFoldersWriter != null) {
                customerFoldersWriter.close();
            }
            if (policyReferenceWriter != null) {
                policyReferenceWriter.close();
            }
            for (PrintWriter statusWriter : statusWriters.values()) {
                statusWriter.close();
            }
            
            // Remove empty files if no data was written
            if (folderFileCreated && totalFolderRows == 0) {
                try {
                    Files.deleteIfExists(Paths.get(getCsvPath(filePath, "Folder-Object-")));
                } catch (IOException e) {
                    System.err.println("Could not delete empty Folder Object CSV file");
                }
            }
            if (fileFileCreated && totalFileRows == 0) {
                try {
                    Files.deleteIfExists(Paths.get(getCsvPath(filePath, "File-Object-")));
                } catch (IOException e) {
                    System.err.println("Could not delete empty File Object CSV file");
                }
            }
            if (claimsFileCreated && totalClaimsRows == 0) {
                try {
                    Files.deleteIfExists(Paths.get(getCsvPath(filePath, "Claims-")));
                } catch (IOException e) {
                    System.err.println("Could not delete empty Claims CSV file");
                }
            }
            if (customerFoldersFileCreated && totalCustomerFoldersRows == 0) {
                try {
                    Files.deleteIfExists(Paths.get(getCsvPath(filePath, "Customer-Folders-")));
                } catch (IOException e) {
                    System.err.println("Could not delete empty Customer Folders CSV file");
                }
            }
            
            if (policyReferenceFileCreated && totalPolicyReferenceRows == 0) {
                try {
                    Files.deleteIfExists(Paths.get(getCsvPath(filePath, "Policy-Reference-")));
                } catch (IOException e) {
                    System.err.println("Could not delete empty Policy Reference CSV file");
                }
            }
            
            // Remove empty status files
            for (Map.Entry<String, Integer> entry : statusRowCounts.entrySet()) {
                if (entry.getValue() == 0) {
                    try {
                        Files.deleteIfExists(Paths.get(getCsvPath(filePath, entry.getKey() + "-Status-")));
                    } catch (IOException e) {
                        System.err.println("Could not delete empty Status CSV file for: " + entry.getKey());
                    }
                }
            }
            
            System.out.println("✅ Resource cleanup completed");
        }
        
        return new FileStats(totalRowsProcessed, totalFolderRows, totalFileRows, totalClaimsRows, totalCustomerFoldersRows, totalPolicyReferenceRows, statusRowCounts.keySet());
    }
    
    /**
     * Process XLS files using traditional Apache POI approach.
     * Loads entire workbook into memory.
     * 
     * @param filePath Path to the XLS file to process
     * @return FileStats containing processing statistics
     */
    private static FileStats processXLSTraditional(Path filePath) throws Exception {
        List<String[]> folderRows = new ArrayList<>();
        List<String[]> fileRows = new ArrayList<>();
        List<String[]> claimsRows = new ArrayList<>();
        List<String[]> customerFoldersRows = new ArrayList<>();
        List<String[]> policyReferencesRows = new ArrayList<>();
        Map<String, List<String[]>> statusRows = new HashMap<>();
        int totalRowsProcessed = 0;
        String[] headerRow = null;
        
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            Workbook workbook = new HSSFWorkbook(fis);
            System.out.println("📖 XLS Workbook loaded successfully. Found " + workbook.getNumberOfSheets() + " sheets");
            
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                String sheetName = sheet.getSheetName();
                System.out.println("📋 Found sheet: '" + sheetName + "'");
                
                if (sheetName.startsWith("Transfer Report")) {
                    System.out.println("⚡ Processing sheet: " + sheetName);
                    SheetProcessingResult result = extractRowsFromSheet(sheet, folderRows, fileRows, claimsRows, customerFoldersRows, policyReferencesRows, statusRows);
                    totalRowsProcessed += result.rowsProcessed;
                    if (headerRow == null) {
                        headerRow = result.headerRow; // Capture header from first Transfer Report sheet
                    }
                    System.out.println("✨ Sheet '" + sheetName + "' completed: " + result.rowsProcessed + " rows processed");
                } else {
                    System.out.println("⏩ Skipping sheet (not Transfer Report): " + sheetName);
                }
            }
            
            System.out.println("🎯 Extraction complete. Found " + folderRows.size() + " folder objects, " + fileRows.size() + " file objects, " + claimsRows.size() + " claims, " + customerFoldersRows.size() + " customer folders, " + policyReferencesRows.size() + " policy references, and " + statusRows.size() + " status groups");
            
            if (!folderRows.isEmpty()) {
                writeToCsvWithHeaders(filePath, folderRows, "Folder-Object-", headerRow);
                System.out.println("💾 Created Folder Object CSV with " + folderRows.size() + " rows");
            }
            
            if (!fileRows.isEmpty()) {
                writeToCsvWithHeaders(filePath, fileRows, "File-Object-", headerRow);
                System.out.println("💾 Created File Object CSV with " + fileRows.size() + " rows");
            }
            
            if (!claimsRows.isEmpty()) {
                writeToCsvWithHeaders(filePath, claimsRows, "Claims-", headerRow);
                System.out.println("💾 Created Claims CSV with " + claimsRows.size() + " rows");
            }
            
            if (!customerFoldersRows.isEmpty()) {
                writeToCsvWithHeaders(filePath, customerFoldersRows, "Customer-Folders-", headerRow);
                System.out.println("💾 Created Customer Folders CSV with " + customerFoldersRows.size() + " rows");
            }
            
            if (!policyReferencesRows.isEmpty()) {
                writeToCsvWithHeaders(filePath, policyReferencesRows, "Policy-Reference-", headerRow);
                System.out.println("💾 Created Policy Reference CSV with " + policyReferencesRows.size() + " rows");
            }
            
            for (Map.Entry<String, List<String[]>> entry : statusRows.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    writeToCsvWithHeaders(filePath, entry.getValue(), entry.getKey() + "-Status-", headerRow);
                    System.out.println("💾 Created Status CSV '" + entry.getKey() + "' with " + entry.getValue().size() + " rows");
                }
            }
            
            if (folderRows.isEmpty() && fileRows.isEmpty() && claimsRows.isEmpty() && customerFoldersRows.isEmpty() && policyReferencesRows.isEmpty() && statusRows.isEmpty()) {
                System.out.println("No matching rows found - no CSV files created");
            }
            
            workbook.close();
        }
        
        return new FileStats(totalRowsProcessed, folderRows.size(), fileRows.size(), claimsRows.size(), customerFoldersRows.size(), policyReferencesRows.size(), statusRows.keySet());
    }
    
    /**
     * Process a single sheet using SAX parsing for streaming.
     * 
     * @param sheetInputStream Input stream for the sheet XML
     * @param sst Shared strings table
     * @param handler Handler for processing sheet events
     */
    private static void processSheet(InputStream sheetInputStream, SharedStrings sst, StreamingSheetHandler handler) throws Exception {
        XMLReader parser = XMLReaderFactory.createXMLReader();
        DataFormatter formatter = new DataFormatter();
        ContentHandler contentHandler = new XSSFSheetXMLHandler(null, sst, handler, formatter, false);
        parser.setContentHandler(contentHandler);
        parser.parse(new org.xml.sax.InputSource(sheetInputStream));
    }
    
    /**
     * Streaming event handler for processing Excel sheet data.
     * Processes rows as they are parsed and writes matching rows immediately to CSV.
     */
    private static class StreamingSheetHandler implements SheetContentsHandler {
        private final PrintWriter folderWriter;
        private final PrintWriter fileWriter;
        private final PrintWriter claimsWriter;
        private final PrintWriter customerFoldersWriter;
        private final PrintWriter policyReferenceWriter;
        private final Map<String, PrintWriter> statusWriters;
        private final Path filePath;
        private final Set<String> statusFilesCreated;
        
        private List<String> currentRow;
        private String[] headerRow;
        private boolean isFirstRow = true;
        private boolean headersWritten = false;
        private Set<String> statusFilesWithHeaders = new HashSet<>();
        private int totalRowsProcessed = 0;
        private int extractedRows = 0;
        private int folderRowsWritten = 0;
        private int fileRowsWritten = 0;
        private int claimsRowsWritten = 0;
        private int customerFoldersRowsWritten = 0;
        private int policyReferenceRowsWritten = 0;
        private Map<String, Integer> statusRowCounts = new HashMap<>();
        
        public StreamingSheetHandler(PrintWriter folderWriter, PrintWriter fileWriter, 
                                   PrintWriter claimsWriter, PrintWriter customerFoldersWriter,
                                   PrintWriter policyReferenceWriter, Map<String, PrintWriter> statusWriters, 
                                   Path filePath, Set<String> statusFilesCreated) {
            this.folderWriter = folderWriter;
            this.fileWriter = fileWriter;
            this.claimsWriter = claimsWriter;
            this.customerFoldersWriter = customerFoldersWriter;
            this.policyReferenceWriter = policyReferenceWriter;
            this.statusWriters = statusWriters;
            this.filePath = filePath;
            this.statusFilesCreated = statusFilesCreated;
        }
        
        public int getTotalRowsProcessed() {
            return totalRowsProcessed;
        }
        
        public int getExtractedRows() {
            return extractedRows;
        }
        
        public int getFolderRowsWritten() {
            return folderRowsWritten;
        }
        
        public int getFileRowsWritten() {
            return fileRowsWritten;
        }
        
        public int getClaimsRowsWritten() {
            return claimsRowsWritten;
        }
        
        public int getCustomerFoldersRowsWritten() {
            return customerFoldersRowsWritten;
        }
        
        public int getPolicyReferenceRowsWritten() {
            return policyReferenceRowsWritten;
        }
        
        public Map<String, Integer> getStatusRowCounts() {
            return statusRowCounts;
        }
        
        /**
         * Write headers to CSV files if not already written
         */
        private void writeHeadersIfNeeded() {
            if (!headersWritten && headerRow != null) {
                // Write headers to all CSV files
                if (folderWriter != null) {
                    writeCsvRow(folderWriter, headerRow);
                }
                if (fileWriter != null) {
                    writeCsvRow(fileWriter, headerRow);
                }
                if (claimsWriter != null) {
                    writeCsvRow(claimsWriter, headerRow);
                }
                if (customerFoldersWriter != null) {
                    writeCsvRow(customerFoldersWriter, headerRow);
                }
                if (policyReferenceWriter != null) {
                    writeCsvRow(policyReferenceWriter, headerRow);
                }
                
                // Note: Status files get headers written when they are first created
                
                headersWritten = true;
            }
        }
        
        @Override
        public void startRow(int rowNum) {
            currentRow = new ArrayList<>();
        }
        
        @Override
        public void cell(String cellReference, String formattedValue, XSSFComment comment) {
            // Parse cell reference to get column index (e.g., "A1" -> 0, "B1" -> 1, etc.)
            int columnIndex = getColumnIndex(cellReference);
            
            // Ensure the currentRow list is large enough to accommodate this column
            while (currentRow.size() <= columnIndex) {
                currentRow.add("");
            }
            
            // Set the value at the correct column index
            currentRow.set(columnIndex, formattedValue != null ? formattedValue : "");
        }
        
        /**
         * Convert Excel cell reference to column index (e.g., "A1" -> 0, "B1" -> 1, "AA1" -> 26)
         */
        private int getColumnIndex(String cellReference) {
            if (cellReference == null || cellReference.isEmpty()) return 0;
            
            // Extract column letters from cell reference (e.g., "B10" -> "B")
            String colStr = cellReference.replaceAll("\\d", "");
            return CellReference.convertColStringToIndex(colStr);
        }
        
        /**
         * Called when a row is completed. Processes the row data and writes to appropriate CSV files.
         */
        @Override
        public void endRow(int rowNum) {
            if (isFirstRow) {
                // Capture header row for CSV files
                headerRow = currentRow.toArray(new String[0]);
                isFirstRow = false;
                return; // Skip processing header row as data
            }
            
            totalRowsProcessed++;
            
            // Process rows with at least File Name (Column A), Source File Size (Column B), and Status (Column S)
            // Note: Due to empty columns, we need to ensure we have enough columns and handle sparse data
            if (currentRow.size() >= 2) {
                String fileName = currentRow.size() > 0 ? currentRow.get(0) : "";
                String fileSizeStr = currentRow.size() > 1 ? currentRow.get(1) : "";
                
                // Look for status in Column S (index 18) - the actual status field
                String fileStatus = "";
                if (currentRow.size() > 18) {
                    fileStatus = currentRow.get(18); // Column S - File Status (success, match-exists, etc.)
                } else if (currentRow.size() > 17) {
                    fileStatus = currentRow.get(17); // Column R - Status (fallback)
                } else if (currentRow.size() > 15) {
                    fileStatus = currentRow.get(15); // Column P - File Status (fallback)
                }
                
                if (fileStatus == null) fileStatus = "";
                
                try {
                    double fileSize = fileSizeStr.isEmpty() ? -1 : Double.parseDouble(fileSizeStr);
                    String[] rowData = currentRow.toArray(new String[0]);
                    
                    // Write headers to CSV files if this is the first data row
                    writeHeadersIfNeeded();
                    
                    // Extract folder objects (File Source Size = 0)
                    if (fileSize == 0.0) {
                        writeCsvRow(folderWriter, rowData);
                        folderRowsWritten++;
                        extractedRows++;
                        
                        // Additional extraction for Claims folders (Python logic: fileSize == 0 AND matches Claims pattern)
                        if (matchesClaimsPattern(fileName)) {
                            writeCsvRow(claimsWriter, rowData);
                            claimsRowsWritten++;
                            extractedRows++;
                        }
                        
                        // Additional extraction for Customer Folders (Python logic: fileSize == 0 AND matches Customer pattern)  
                        if (matchesCustomerFoldersPattern(fileName)) {
                            writeCsvRow(customerFoldersWriter, rowData);
                            customerFoldersRowsWritten++;
                            extractedRows++;
                        }
                        
                        // Additional extraction for Policy Reference (fileSize == 0 AND matches Policy Reference pattern)
                        if (matchesPolicyReferencePattern(fileName)) {
                            writeCsvRow(policyReferenceWriter, rowData);
                            policyReferenceRowsWritten++;
                            extractedRows++;
                        }
                    } 
                    // Extract file objects (File Source Size > 0)
                    else if (fileSize > 0.0) {
                        writeCsvRow(fileWriter, rowData);
                        fileRowsWritten++;
                        extractedRows++;
                    }
                    
                    // Extract by status (if file status is not empty)
                    if (!fileStatus.trim().isEmpty()) {
                        
                        String cleanStatus = sanitizeFileName(fileStatus.trim());
                        
                        // Skip if the status is still too long after cleaning (likely not a real status)
                        if (cleanStatus.length() > 50) {
                            // Don't process this as a status
                        } else {
                            // Create status writer if it doesn't exist
                            getOrCreateStatusWriter(cleanStatus);
                        }
                        
                        // Write to status CSV
                        PrintWriter statusWriter = statusWriters.get(cleanStatus);
                        if (statusWriter != null) {
                            writeCsvRow(statusWriter, rowData);
                            statusRowCounts.merge(cleanStatus, 1, Integer::sum);
                            extractedRows++;
                        }
                    }
                    
                } catch (NumberFormatException nfe) {
                    // Still process by status even if file size isn't a valid number
                    if (!fileStatus.trim().isEmpty()) {
                        String cleanStatus = sanitizeFileName(fileStatus.trim());
                        String[] rowData = currentRow.toArray(new String[0]);
                        
                        // Skip if the status is still too long after cleaning (likely not a real status)
                        if (cleanStatus.length() <= 50) {
                            getOrCreateStatusWriter(cleanStatus);
                        
                            PrintWriter statusWriter = statusWriters.get(cleanStatus);
                            if (statusWriter != null) {
                                writeCsvRow(statusWriter, rowData);
                                statusRowCounts.merge(cleanStatus, 1, Integer::sum);
                                extractedRows++;
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Helper method to create or retrieve a PrintWriter for a given status.
     * Handles the creation of status CSV files and writes headers if available.
     * 
     * @param cleanStatus The sanitized status name
     * @return PrintWriter for the status, or null if creation failed
     */
    private PrintWriter getOrCreateStatusWriter(String cleanStatus) {
        if (!statusWriters.containsKey(cleanStatus)) {
            try {
                String statusCsvPath = getCsvPath(filePath, cleanStatus + "-Status-");
                PrintWriter statusWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(statusCsvPath)));
                statusWriters.put(cleanStatus, statusWriter);
                statusFilesCreated.add(cleanStatus);
                statusRowCounts.put(cleanStatus, 0);
                
                // Write header to new status CSV file if we have it
                if (headerRow != null) {
                    writeCsvRow(statusWriter, headerRow);
                    statusFilesWithHeaders.add(cleanStatus);
                }
                return statusWriter;
            } catch (IOException e) {
                System.err.println("Error creating status CSV for: '" + cleanStatus + "' - " + e.getMessage());
                return null;
            }
        }
        return statusWriters.get(cleanStatus);
    }
    
    /**
     * Extract matching rows from a sheet using traditional POI approach (for XLS files).
     * 
     * @param sheet The Excel sheet to process
     * @param folderRows List to collect folder object rows
     * @param fileRows List to collect file object rows
     * @param statusRows Map to collect status-based rows
     * @return Number of rows processed
     */
    private static SheetProcessingResult extractRowsFromSheet(Sheet sheet, List<String[]> folderRows, 
                                          List<String[]> fileRows, List<String[]> claimsRows, 
                                          List<String[]> customerFoldersRows, List<String[]> policyReferencesRows, Map<String, List<String[]>> statusRows) {
        int rowsProcessed = 0;
        String[] headerRow = null;
        
        for (Row row : sheet) {
            if (row.getRowNum() == 0) {
                // Capture header row
                headerRow = new String[row.getLastCellNum()];
                for (int i = 0; i < row.getLastCellNum(); i++) {
                    Cell cell = row.getCell(i);
                    headerRow[i] = getCellValueAsString(cell);
                }
                continue; // Skip processing header row as data
            }
            
            rowsProcessed++;
            
            Cell cellA = row.getCell(0); // Column A - File Name
            Cell cellB = row.getCell(1); // Column B - Source File Size
            
            if (cellA == null || cellB == null) continue;
            
            String fileName = getCellValueAsString(cellA);
            double fileSize = getCellValueAsDouble(cellB);
            
            // Look for status in Column S (index 18) - the actual status field
            String fileStatus = "";
            Cell statusCell = row.getCell(18); // Column S - File Status (success, match-exists, etc.)
            if (statusCell != null) {
                fileStatus = getCellValueAsString(statusCell);
            } else {
                statusCell = row.getCell(15); // Column P - File Status (fallback)
                if (statusCell != null) {
                    fileStatus = getCellValueAsString(statusCell);
                } else {
                    statusCell = row.getCell(17); // Column R - Status (fallback)
                    if (statusCell != null) {
                        fileStatus = getCellValueAsString(statusCell);
                    }
                }
            }
            
            // Extract entire row data
            String[] rowData = new String[row.getLastCellNum()];
            for (int i = 0; i < row.getLastCellNum(); i++) {
                Cell cell = row.getCell(i);
                rowData[i] = getCellValueAsString(cell);
            }
            
            // Categorize by file size
            if (fileSize == 0.0) {
                folderRows.add(rowData);
                
                // Additional extraction for Claims folders (Python logic: fileSize == 0 AND matches Claims pattern)
                if (matchesClaimsPattern(fileName)) {
                    claimsRows.add(rowData);
                }
                
                // Additional extraction for Customer Folders (Python logic: fileSize == 0 AND matches Customer pattern)
                if (matchesCustomerFoldersPattern(fileName)) {
                    customerFoldersRows.add(rowData);
                }
                
                // Additional extraction for Policy Reference (fileSize == 0 AND matches Policy Reference pattern)
                if (matchesPolicyReferencePattern(fileName)) {
                    policyReferencesRows.add(rowData);
                }
            } else if (fileSize > 0.0) {
                fileRows.add(rowData);
            }
            
            // Categorize by status
            if (!fileStatus.trim().isEmpty()) {
                String cleanStatus = sanitizeFileName(fileStatus.trim());
                statusRows.computeIfAbsent(cleanStatus, k -> new ArrayList<>()).add(rowData);
            }
        }
        
        return new SheetProcessingResult(rowsProcessed, headerRow);
    }
    
    /**
     * Sanitize file status values to create valid file names
     */
    private static String sanitizeFileName(String status) {
        if (status == null || status.trim().isEmpty()) {
            return "Unknown";
        }
        
        // Truncate extremely long status values (they might be file paths by mistake)
        String cleaned = status.trim();
        if (cleaned.length() > 50) {
            System.err.println("Warning: Status value is very long (" + cleaned.length() + " chars): " + cleaned.substring(0, 50) + "...");
            cleaned = cleaned.substring(0, 50);
        }
        
        return cleaned.replaceAll("[^a-zA-Z0-9\\-_\\s]", "")
                     .replaceAll("\\s+", "-")
                     .replaceAll("-+", "-")
                     .trim();
    }
    
    /**
     * Convert Excel cell value to string representation.
     * 
     * @param cell The Excel cell to convert
     * @return String representation of the cell value
     */
    private static String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }
    
    /**
     * Convert Excel cell value to double, used for Source File Size column.
     * 
     * @param cell The Excel cell to convert
     * @return Double value of the cell, 0.0 if conversion fails
     */
    private static double getCellValueAsDouble(Cell cell) {
        if (cell == null) return 0.0;
        
        switch (cell.getCellType()) {
            case NUMERIC:
                return cell.getNumericCellValue();
            case STRING:
                try {
                    return Double.parseDouble(cell.getStringCellValue());
                } catch (NumberFormatException e) {
                    return 0.0;
                }
            default:
                return 0.0;
        }
    }
    
    /**
     * Generate organized CSV file path based on Excel file path and prefix.
     * Creates organized folder structure: report/[filename]/[category]/[file].csv
     * 
     * @param excelFilePath Path to the Excel file
     * @param prefix Prefix for the CSV file (e.g., "Folder-Object-", "File-Object-", "Claims-", "Customer-Folders-", "Status-Name-")
     * @return Full path to the CSV file in organized structure
     */
    private static String getCsvPath(Path excelFilePath, String prefix) {
        String excelFileName = excelFilePath.getFileName().toString();
        String baseFileName = excelFileName.replaceAll("\\.(xlsx|xls)$", "");
        String csvFileName = prefix + excelFileName.replaceAll("\\.(xlsx|xls)$", ".csv");
        
        // Create organized folder structure
        Path baseDir = excelFilePath.getParent();
        Path reportDir = baseDir.resolve("report");
        Path fileReportDir = reportDir.resolve(baseFileName);
        
        // Determine subfolder based on prefix
        Path targetDir;
        if (prefix.startsWith("Claims-")) {
            targetDir = fileReportDir.resolve("Claims");
        } else if (prefix.startsWith("Customer-Folders-")) {
            targetDir = fileReportDir.resolve("Customer");
        } else if (prefix.startsWith("Policy-Reference-")) {
            targetDir = fileReportDir.resolve("PolicyReference");
        } else if (prefix.contains("-Status-")) {
            targetDir = fileReportDir.resolve("Status");
        } else {
            // Folder-Object- and File-Object- go in the main report directory
            targetDir = fileReportDir;
        }
        
        // Create directories if they don't exist
        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            System.err.println("Warning: Could not create directory structure: " + e.getMessage());
            // Fallback to original behavior
            return excelFilePath.getParent().resolve(csvFileName).toString();
        }
        
        return targetDir.resolve(csvFileName).toString();
    }
    
    /**
     * Write a single row to CSV with proper escaping of quotes.
     * 
     * @param writer PrintWriter for the CSV file
     * @param row Array of cell values to write
     */
    private static void writeCsvRow(PrintWriter writer, String[] row) {
        String csvRow = String.join(",", Arrays.stream(row)
            .map(field -> "\"" + (field != null ? field.replace("\"", "\"\"") : "") + "\"")
            .toArray(String[]::new));
        writer.println(csvRow);
    }
    
    /**
     * Write multiple rows to CSV file with headers (used by traditional XLS processing).
     * 
     * @param excelFilePath Path to the source Excel file
     * @param rows List of rows to write
     * @param prefix Prefix for the CSV filename
     * @param headerRow Header row to write first
     */
    private static void writeToCsvWithHeaders(Path excelFilePath, List<String[]> rows, String prefix, String[] headerRow) throws IOException {
        String csvPath = getCsvPath(excelFilePath, prefix);
        
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths.get(csvPath)))) {
            // Write header row first if available
            if (headerRow != null) {
                writeCsvRow(writer, headerRow);
            }
            
            // Write data rows
            for (String[] row : rows) {
                writeCsvRow(writer, row);
            }
        }
        
        System.out.println("📄 Created CSV: " + csvPath);
    }
    
    /**
     * Write multiple rows to CSV file (used by traditional XLS processing).
     * 
     * @param excelFilePath Path to the source Excel file
     * @param rows List of rows to write
     * @param prefix Prefix for the CSV filename
     */
    private static void writeToCsv(Path excelFilePath, List<String[]> rows, String prefix) throws IOException {
        String csvPath = getCsvPath(excelFilePath, prefix);
        
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths.get(csvPath)))) {
            for (String[] row : rows) {
                writeCsvRow(writer, row);
            }
        }
        
        System.out.println("📄 Created CSV: " + csvPath);
    }
}