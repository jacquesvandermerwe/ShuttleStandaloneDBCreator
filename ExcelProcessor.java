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
 * Excel Processor for extracting specific data patterns from large Excel files.
 * 
 * This application processes Excel files containing "Transfer Report" sheets and extracts
 * rows where Source File Size = 0 and File Name matches specific path patterns.
 * 
 * Supports two path patterns:
 * 1. Folder pattern: //UKDOCDWNPSFS102/PI_Folders/D/DATA/HCCD/Folders/[NUMBER]
 * 2. Claims pattern: //UKDOCDWNPSFS102/PI_Folders/D/DATA/HCCD/Folders/[NUMBER]/Claims/[NUMBER]
 * 
 * Uses streaming processing for memory efficiency with large files (700MB+).
 */
public class ExcelProcessor {
    
    // Regex pattern for folder-level paths
    private static final Pattern FOLDER_PATTERN = Pattern.compile("^//UKDOCDWNPSFS102/PI_Folders/D/DATA/HCCD/Folders/\\d+$");
    
    // Regex pattern for claims-level paths  
    private static final Pattern CLAIMS_PATTERN = Pattern.compile("^//UKDOCDWNPSFS102/PI_Folders/D/DATA/HCCD/Folders/\\d+/Claims/\\d+$");
    
    /**
     * Main entry point for the Excel processor.
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
        int totalClaimsFound = 0;
        
        // Process files sequentially to avoid memory issues with large files
        for (Path file : excelFiles) {
            try {
                System.out.println("Processing file: " + file.getFileName());
                long startTime = System.currentTimeMillis();
                
                FileStats stats = processExcelFile(file);
                filesProcessed++;
                totalRowsProcessed += stats.rowsProcessed;
                totalFoldersFound += stats.foldersFound;
                totalClaimsFound += stats.claimsFound;
                
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
        System.out.println("Files processed: " + filesProcessed + " of " + excelFiles.size());
        System.out.println("Total application time: " + formatTime(totalApplicationTime));
        System.out.println("Total rows processed: " + String.format("%,d", totalRowsProcessed));
        System.out.println("Total folders found: " + String.format("%,d", totalFoldersFound));
        System.out.println("Total claims found: " + String.format("%,d", totalClaimsFound));
        System.out.println("========================================");
    }
    
    /**
     * Simple statistics holder for file processing results
     */
    private static class FileStats {
        final int rowsProcessed;
        final int foldersFound;
        final int claimsFound;
        
        FileStats(int rowsProcessed, int foldersFound, int claimsFound) {
            this.rowsProcessed = rowsProcessed;
            this.foldersFound = foldersFound;
            this.claimsFound = claimsFound;
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
        System.out.println("Processing: " + filePath);
        
        FileStats stats;
        if (filePath.toString().toLowerCase().endsWith(".xlsx")) {
            System.out.println("Using streaming XLSX processing...");
            stats = processXLSXStreaming(filePath);
        } else {
            System.out.println("Using traditional XLS processing...");
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
        PrintWriter claimsWriter = null;
        boolean folderFileCreated = false;
        boolean claimsFileCreated = false;
        int totalFolderRows = 0;
        int totalClaimsRows = 0;
        int totalRowsProcessed = 0;
        
        try (OPCPackage pkg = OPCPackage.open(filePath.toFile())) {
            System.out.println("OPC Package opened successfully");
            XSSFReader reader = new XSSFReader(pkg);
            System.out.println("XSSFReader created");
            SharedStrings sst = reader.getSharedStringsTable();
            System.out.println("SharedStrings table loaded");
            
            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            System.out.println("Sheet iterator created");
            
            int sheetCount = 0;
            
            while (sheets.hasNext()) {
                sheetCount++;
                try (InputStream sheet = sheets.next()) {
                    String sheetName = sheets.getSheetName();
                    System.out.println("Found sheet " + sheetCount + ": '" + sheetName + "'");
                    
                    if (sheetName.startsWith("Transfer Report")) {
                        System.out.println("Processing sheet: " + sheetName);
                        
                        // Create CSV writers on first Transfer Report sheet
                        if (folderWriter == null) {
                            String folderCsvPath = getCsvPath(filePath, "Folder-");
                            folderWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(folderCsvPath)));
                            folderFileCreated = true;
                        }
                        if (claimsWriter == null) {
                            String claimsCsvPath = getCsvPath(filePath, "Claims-");
                            claimsWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(claimsCsvPath)));
                            claimsFileCreated = true;
                        }
                        
                        StreamingSheetHandler handler = new StreamingSheetHandler(folderWriter, claimsWriter);
                        processSheet(sheet, sst, handler);
                        
                        // Flush after each sheet to ensure data is written
                        folderWriter.flush();
                        claimsWriter.flush();
                        
                        totalFolderRows += handler.getFolderRowsWritten();
                        totalClaimsRows += handler.getClaimsRowsWritten();
                        totalRowsProcessed += handler.getTotalRowsProcessed();
                        
                        System.out.println("Sheet '" + sheetName + "' completed: " + handler.getTotalRowsProcessed() + " rows processed, " + handler.getExtractedRows() + " rows extracted");
                    } else {
                        System.out.println("Skipping sheet (not Transfer Report): " + sheetName);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing sheet " + sheetCount + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
            System.out.println("Total sheets processed: " + sheetCount);
            
            System.out.println("Extraction complete. Found " + totalFolderRows + " folder rows and " + totalClaimsRows + " claims rows");
            
            if (totalFolderRows > 0) {
                System.out.println("Created Folder CSV with " + totalFolderRows + " rows");
            }
            
            if (totalClaimsRows > 0) {
                System.out.println("Created Claims CSV with " + totalClaimsRows + " rows");
            }
            
            if (totalFolderRows == 0 && totalClaimsRows == 0) {
                System.out.println("No matching rows found - no CSV files created");
            }
            
        } catch (Exception e) {
            System.err.println("Error in streaming processing: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            System.out.println("Finalizing CSV files and cleaning up resources...");
            
            // Close CSV writers and ensure all data is written to disk
            if (folderWriter != null) {
                folderWriter.close();
            }
            if (claimsWriter != null) {
                claimsWriter.close();
            }
            
            // Remove empty files if no data was written
            cleanupOutputFile(folderFileCreated, totalFolderRows, filePath, "Folder-");
            cleanupOutputFile(claimsFileCreated, totalClaimsRows, filePath, "Claims-");
            
            System.out.println("Resource cleanup completed");
        }
        
        return new FileStats(totalRowsProcessed, totalFolderRows, totalClaimsRows);
    }
    
    /**
     * Helper method to clean up empty output files.
     * Removes CSV files that were created but contain no data.
     * 
     * @param created Whether the file was created
     * @param rowCount Number of rows written to the file
     * @param filePath Path to the source Excel file
     * @param prefix Prefix for the CSV filename
     */
    private static void cleanupOutputFile(boolean created, int rowCount, Path filePath, String prefix) {
        if (created && rowCount == 0) {
            try {
                Files.deleteIfExists(Paths.get(getCsvPath(filePath, prefix)));
            } catch (IOException e) {
                System.err.println("Could not delete empty " + prefix + "CSV file");
            }
        }
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
        List<String[]> claimsRows = new ArrayList<>();
        int totalRowsProcessed = 0;
        
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            Workbook workbook = new HSSFWorkbook(fis);
            System.out.println("XLS Workbook loaded successfully. Found " + workbook.getNumberOfSheets() + " sheets");
            
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                String sheetName = sheet.getSheetName();
                System.out.println("Found sheet: '" + sheetName + "'");
                
                if (sheetName.startsWith("Transfer Report")) {
                    System.out.println("Processing sheet: " + sheetName);
                    int sheetRowsProcessed = extractRowsFromSheet(sheet, folderRows, claimsRows);
                    totalRowsProcessed += sheetRowsProcessed;
                    System.out.println("Sheet '" + sheetName + "' completed: " + sheetRowsProcessed + " rows processed");
                } else {
                    System.out.println("Skipping sheet (not Transfer Report): " + sheetName);
                }
            }
            
            System.out.println("Extraction complete. Found " + folderRows.size() + " folder rows and " + claimsRows.size() + " claims rows");
            
            if (!folderRows.isEmpty()) {
                writeToCsv(filePath, folderRows, "Folder-");
                System.out.println("Created Folder CSV with " + folderRows.size() + " rows");
            }
            
            if (!claimsRows.isEmpty()) {
                writeToCsv(filePath, claimsRows, "Claims-");
                System.out.println("Created Claims CSV with " + claimsRows.size() + " rows");
            }
            
            if (folderRows.isEmpty() && claimsRows.isEmpty()) {
                System.out.println("No matching rows found - no CSV files created");
            }
            
            workbook.close();
        }
        
        return new FileStats(totalRowsProcessed, folderRows.size(), claimsRows.size());
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
        private final PrintWriter claimsWriter;
        private List<String> currentRow;
        private boolean isFirstRow = true;
        private int totalRowsProcessed = 0;
        private int extractedRows = 0;
        private int folderRowsWritten = 0;
        private int claimsRowsWritten = 0;
        
        public StreamingSheetHandler(PrintWriter folderWriter, PrintWriter claimsWriter) {
            this.folderWriter = folderWriter;
            this.claimsWriter = claimsWriter;
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
        
        public int getClaimsRowsWritten() {
            return claimsRowsWritten;
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
        
        private int getColumnIndex(String cellReference) {
            if (cellReference == null || cellReference.isEmpty()) return 0;
            
            // Extract column letters from cell reference (e.g., "B10" -> "B")
            StringBuilder columnLetters = new StringBuilder();
            for (char c : cellReference.toCharArray()) {
                if (Character.isLetter(c)) {
                    columnLetters.append(c);
                } else {
                    break; // Stop at first non-letter (the row number)
                }
            }
            
            // Convert column letters to index
            int columnIndex = 0;
            for (char c : columnLetters.toString().toCharArray()) {
                columnIndex = columnIndex * 26 + (c - 'A' + 1);
            }
            return columnIndex - 1; // Convert to 0-based index
        }
        
        /**
         * Called when a row is completed. Processes the row data and writes to CSV if it matches criteria.
         */
        @Override
        public void endRow(int rowNum) {
            if (isFirstRow) {
                isFirstRow = false;
                return; // Skip header row
            }
            
            totalRowsProcessed++;
            
            // Process rows with at least File Name (Column A) and Source File Size (Column B)
            if (currentRow.size() >= 2) {
                String fileName = currentRow.size() > 0 ? currentRow.get(0) : "";
                String fileSizeStr = currentRow.size() > 1 ? currentRow.get(1) : "";
                
                try {
                    double fileSize = fileSizeStr.isEmpty() ? -1 : Double.parseDouble(fileSizeStr);
                    
                    // Only process rows where Source File Size = 0
                    if (fileSize == 0.0) {
                        String[] rowData = currentRow.toArray(new String[0]);
                        
                        // Check for folder pattern and write to folder CSV
                        if (FOLDER_PATTERN.matcher(fileName).matches()) {
                            writeCsvRow(folderWriter, rowData);
                            folderRowsWritten++;
                            extractedRows++;
                        } 
                        // Check for claims pattern and write to claims CSV
                        else if (CLAIMS_PATTERN.matcher(fileName).matches()) {
                            writeCsvRow(claimsWriter, rowData);
                            claimsRowsWritten++;
                            extractedRows++;
                        }
                    }
                } catch (NumberFormatException e) {
                    // Ignore rows where file size isn't a valid number
                }
            }
        }
    }
    
    /**
     * Extract matching rows from a sheet using traditional POI approach (for XLS files).
     * 
     * @param sheet The Excel sheet to process
     * @param folderRows List to collect folder pattern rows
     * @param claimsRows List to collect claims pattern rows
     * @return Number of rows processed
     */
    private static int extractRowsFromSheet(Sheet sheet, List<String[]> folderRows, List<String[]> claimsRows) {
        int rowsProcessed = 0;
        
        for (Row row : sheet) {
            if (row.getRowNum() == 0) continue; // Skip header row
            
            rowsProcessed++;
            
            Cell cellA = row.getCell(0); // Column A - File Name
            Cell cellB = row.getCell(1); // Column B - Source File Size
            
            if (cellA == null || cellB == null) continue;
            
            String fileName = getCellValueAsString(cellA);
            double fileSize = getCellValueAsDouble(cellB);
            
            // Only process rows where Source File Size = 0
            if (fileSize == 0.0) {
                // Extract entire row data
                String[] rowData = new String[row.getLastCellNum()];
                for (int i = 0; i < row.getLastCellNum(); i++) {
                    Cell cell = row.getCell(i);
                    rowData[i] = getCellValueAsString(cell);
                }
                
                // Categorize by pattern
                if (FOLDER_PATTERN.matcher(fileName).matches()) {
                    folderRows.add(rowData);
                } else if (CLAIMS_PATTERN.matcher(fileName).matches()) {
                    claimsRows.add(rowData);
                }
            }
        }
        
        return rowsProcessed;
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
     * Generate CSV file path based on Excel file path and prefix.
     * 
     * @param excelFilePath Path to the Excel file
     * @param prefix Prefix for the CSV file (e.g., "Folder-" or "Claims-")
     * @return Full path to the CSV file
     */
    private static String getCsvPath(Path excelFilePath, String prefix) {
        String excelFileName = excelFilePath.getFileName().toString();
        String csvFileName = prefix + excelFileName.replaceAll("\\.(xlsx|xls)$", ".csv");
        return excelFilePath.getParent().resolve(csvFileName).toString();
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
        
        System.out.println("Created CSV: " + csvPath);
    }
}