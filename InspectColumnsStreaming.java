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
import java.util.*;

/**
 * Streaming Column Inspector - Shows the first few rows of large Excel files using streaming API
 */
public class InspectColumnsStreaming {
    
    public static void main(String[] args) throws Exception {
        // Increase Apache POI memory limit for large Excel files (1GB limit)
        IOUtils.setByteArrayMaxOverride(1024 * 1024 * 1024);
        
        if (args.length == 0) {
            System.out.println("Usage: jbang InspectColumnsStreaming.java <excel_file>");
            System.out.println("Shows the first few rows and column headers using streaming to handle large files");
            return;
        }
        
        String filePath = args[0];
        Path path = Paths.get(filePath);
        
        if (!Files.exists(path)) {
            System.err.println("File not found: " + filePath);
            return;
        }
        
        System.out.println("Inspecting Excel file (streaming): " + path.getFileName());
        System.out.println("========================================================");
        
        try (OPCPackage pkg = OPCPackage.open(path.toFile())) {
            XSSFReader reader = new XSSFReader(pkg);
            SharedStrings sst = reader.getSharedStringsTable();
            
            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            int sheetCount = 0;
            
            while (sheets.hasNext()) {
                sheetCount++;
                try (InputStream sheet = sheets.next()) {
                    String sheetName = sheets.getSheetName();
                    System.out.println("\n--- Sheet " + sheetCount + ": " + sheetName + " ---");
                    
                    if (sheetName.startsWith("Transfer Report")) {
                        System.out.println("This is a Transfer Report sheet - inspecting structure:");
                        
                        InspectorHandler handler = new InspectorHandler();
                        processSheet(sheet, sst, handler);
                        
                        System.out.println("Inspection complete for sheet: " + sheetName);
                    } else {
                        System.out.println("Not a Transfer Report sheet - skipping");
                    }
                }
            }
        }
    }
    
    /**
     * Process a single sheet using SAX parsing for streaming.
     */
    private static void processSheet(InputStream sheetInputStream, SharedStrings sst, InspectorHandler handler) throws java.io.IOException, org.xml.sax.SAXException {
        XMLReader parser = XMLReaderFactory.createXMLReader();
        DataFormatter formatter = new DataFormatter();
        ContentHandler contentHandler = new XSSFSheetXMLHandler(null, sst, handler, formatter, false);
        parser.setContentHandler(contentHandler);
        parser.parse(new org.xml.sax.InputSource(sheetInputStream));
    }
    
    /**
     * Handler for inspecting the first few rows of a sheet
     */
    private static class InspectorHandler implements SheetContentsHandler {
        private List<String> currentRow;
        private int rowNumber = 0;
        private final int MAX_ROWS_TO_SHOW = 1000;
        
        @Override
        public void startRow(int rowNum) {
            this.rowNumber = rowNum;
            this.currentRow = new ArrayList<>();
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
        
        @Override
        public void endRow(int rowNum) {
            if (rowNumber < MAX_ROWS_TO_SHOW) {
                System.out.println("\nRow " + (rowNumber + 1) + ":");
                
                int maxCol = Math.min(currentRow.size(), 25); // Show max 25 columns (A-Y)
                for (int colIndex = 0; colIndex < maxCol; colIndex++) {
                    String cellValue = currentRow.get(colIndex);
                    
                    if (cellValue != null && !cellValue.trim().isEmpty()) {
                        String columnLetter = getColumnLetter(colIndex);
                        String prefix = "  " + columnLetter + " (" + (colIndex + 1) + "): ";
                        
                        // Highlight important status columns
                        if (colIndex == 15) { // Column P - File Status
                            prefix = "  ★ " + columnLetter + " (" + (colIndex + 1) + ") [File Status]: ";
                        } else if (colIndex == 17) { // Column R - Status  
                            prefix = "  ◆ " + columnLetter + " (" + (colIndex + 1) + ") [Status]: ";
                        }
                        
                        System.out.println(prefix + 
                            (cellValue.length() > 80 ? cellValue.substring(0, 80) + "..." : cellValue));
                    }
                }
            }
        }
        
        /**
         * Convert column index to Excel column letter (0=A, 1=B, etc.)
         */
        private String getColumnLetter(int columnIndex) {
            StringBuilder columnLetter = new StringBuilder();
            while (columnIndex >= 0) {
                columnLetter.insert(0, (char) ('A' + columnIndex % 26));
                columnIndex = columnIndex / 26 - 1;
            }
            return columnLetter.toString();
        }
    }
}