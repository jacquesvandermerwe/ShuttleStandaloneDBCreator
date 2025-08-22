///usr/bin/env jbang "$0" "$@" ; exit $?

// JVM Options for handling very large Excel files (increased heap size for 100MB+ files)
//JAVA_OPTIONS -Xmx32g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Djdk.xml.maxGeneralEntitySizeLimit=0 -Djdk.xml.totalEntitySizeLimit=0 -Djdk.xml.maxParameterEntitySizeLimit=0 -Djdk.xml.entityExpansionLimit=0 -Djdk.xml.maxElementDepth=0

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
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.openxml4j.opc.PackageRelationship;
import org.apache.poi.openxml4j.opc.PackageRelationshipTypes;

import java.io.*;
import java.nio.file.*;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.zip.*;
import java.nio.charset.StandardCharsets;

/**
 * Overview Extractor for extracting "Overview" sheets from Excel files.
 * 
 * This application processes Excel files and extracts "Overview" sheets,
 * creating new Excel files with only the Overview sheet data.
 * 
 * Output files are named: Transfer-Overview-[original-filename].xlsx
 * 
 * Uses memory-efficient processing for large files (700MB+).
 */
public class TransferOverviewExtractor {
    
    /**
     * Main entry point for the Overview extractor.
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
        System.out.println("📊 Found " + excelFiles.size() + " Excel files");
        
        // Statistics tracking
        int filesProcessed = 0;
        int overviewSheetsExtracted = 0;
        
        // Process files sequentially to avoid memory issues with large files
        for (Path file : excelFiles) {
            try {
                System.out.println("🔍 Processing file: " + file.getFileName());
                long startTime = System.currentTimeMillis();
                
                boolean extracted = extractTransferOverview(file);
                filesProcessed++;
                if (extracted) {
                    overviewSheetsExtracted++;
                }
                
                long endTime = System.currentTimeMillis();
                long processingTime = endTime - startTime;
                System.out.println("✅ Completed file: " + file.getFileName() + " in " + formatTime(processingTime));
                
                // Force garbage collection after each file to free memory
                System.gc();
            } catch (Exception e) {
                System.err.println("Error processing " + file + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        System.out.println("🎉 Processing completed");
        
        // Calculate total application time
        long applicationEndTime = System.currentTimeMillis();
        long totalApplicationTime = applicationEndTime - applicationStartTime;
        
        // Print final statistics
        System.out.println();
        System.out.println("========================================");
        System.out.println("📈         FINAL STATISTICS         📈");
        System.out.println("========================================");
        System.out.println("📁 Files processed: " + filesProcessed + " of " + excelFiles.size());
        System.out.println("⏱️  Total application time: " + formatTime(totalApplicationTime));
        System.out.println("📋 Overview sheets extracted: " + String.format("%,d", overviewSheetsExtracted));
        System.out.println("========================================");
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
     * Generate organized output path for Overview Excel files.
     * Creates organized folder structure: report/[filename]/Overview/Overview-[file].xlsx
     * 
     * @param excelFilePath Path to the Excel file
     * @return Full path to the Overview file in organized structure
     */
    private static Path getOverviewOutputPath(Path excelFilePath) {
        String excelFileName = excelFilePath.getFileName().toString();
        String baseFileName = excelFileName.replaceAll("\\.(xlsx|xls)$", "");
        String outputFileName = "Overview-" + excelFileName;
        
        // Create organized folder structure
        Path baseDir = excelFilePath.getParent();
        Path reportDir = baseDir.resolve("report");
        Path fileReportDir = reportDir.resolve(baseFileName);
        Path overviewDir = fileReportDir.resolve("Overview");
        
        // Create directories if they don't exist
        try {
            Files.createDirectories(overviewDir);
        } catch (IOException e) {
            System.err.println("Warning: Could not create Overview directory structure: " + e.getMessage());
            // Fallback to original behavior
            return excelFilePath.getParent().resolve("Transfer-Overview-" + excelFileName);
        }
        
        return overviewDir.resolve(outputFileName);
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
                      
                      // Skip already processed Overview files (both old and new naming)
                      if (fileName.startsWith("Transfer-Overview-") || fileName.startsWith("Overview-")) {
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
     * Extract Overview sheet from an Excel file and create a new Excel file.
     * 
     * @param filePath Path to the Excel file to process
     * @return true if an Overview sheet was found and extracted
     */
    private static boolean extractTransferOverview(Path filePath) throws Exception {
        System.out.println("⚙️  Processing: " + filePath);
        
        // Check file size and warn about very large files
        long fileSizeBytes = Files.size(filePath);
        long fileSizeMB = fileSizeBytes / (1024 * 1024);
        System.out.println("📏 File size: " + fileSizeMB + " MB");
        
        if (fileSizeMB > 200) {
            System.out.println("⚠️  WARNING: File exceeds 200MB - processing may require significant memory and time");
            System.out.println("💡 Consider processing this file individually with increased system memory");
        }
        
        boolean overviewFound = false;
        
        try {
            if (filePath.toString().toLowerCase().endsWith(".xlsx")) {
                System.out.println("📋 Processing XLSX file...");
                overviewFound = extractFromXLSX(filePath);
            } else {
                System.out.println("📋 Processing XLS file...");
                overviewFound = extractFromXLS(filePath);
            }
            
            if (overviewFound) {
                System.out.println("✨ Overview sheet extracted successfully");
            } else {
                System.out.println("⚠️  No Overview sheet found in file");
            }
        } catch (OutOfMemoryError e) {
            System.err.println("❌ MEMORY ERROR: File too large for current heap size (" + fileSizeMB + " MB)");
            System.err.println("💡 Try processing this file individually or increase heap size to -Xmx32g");
            throw new Exception("File too large: " + filePath.getFileName(), e);
        }
        
        // Clear references to help garbage collection
        System.gc();
        return overviewFound;
    }
    
    /**
     * Extract Overview sheet from XLSX file using low-level OOXML manipulation.
     * 
     * @param filePath Path to the XLSX file to process
     * @return true if Overview sheet was found and extracted
     */
    private static boolean extractFromXLSX(Path filePath) throws Exception {
        String overviewSheetName = null;
        String overviewRelId = null;
        Set<String> sheetsToKeep = new HashSet<>();
        
        // First pass: Use streaming API to find Overview sheet without loading full workbook
        try (OPCPackage pkg = OPCPackage.open(filePath.toFile(), PackageAccess.READ)) {
            XSSFReader reader = new XSSFReader(pkg);
            XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            
            System.out.println("📖 XLSX Package opened with streaming API");
            
            while (sheets.hasNext()) {
                try (InputStream sheet = sheets.next()) {
                    String sheetName = sheets.getSheetName();
                    System.out.println("📋 Found sheet: '" + sheetName + "'");
                    
                    if (sheetName.equals("Overview")) {
                        System.out.println("⚡ Found Overview sheet: " + sheetName);
                        overviewSheetName = sheetName;
                        
                        // Get the sheet part for relationship mapping
                        PackagePart sheetPart = sheets.getSheetPart();
                        // We'll determine the sheet file name from the part name
                        String partName = sheetPart.getPartName().getName();
                        overviewRelId = partName; // Use part name as identifier
                        sheetsToKeep.add(partName);
                        break;
                    }
                }
            }
        }
        
        if (overviewSheetName == null) {
            return false;
        }
        
        // Second pass: Create new XLSX file using ZIP-level manipulation
        System.out.println("🔄 Creating Overview-only workbook using ZIP manipulation...");
        
        Path outputPath = getOverviewOutputPath(filePath);
        
        System.out.println("🔍 Overview sheet part name: " + overviewRelId);
        System.out.println("🔄 Using ZIP-based extraction to preserve all charts and formatting...");
        
        try {
            extractOverviewSheetUsingZip(filePath, outputPath, overviewRelId);
            System.out.println("💾 Created Overview file: " + outputPath.getFileName());
            System.out.println("📊 Preserved complete Overview sheet with all formatting, charts, and images");
            return true;
        } catch (Exception e) {
            System.err.println("❌ ZIP extraction failed: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    
    /**
     * Extract Overview sheet using direct ZIP file manipulation.
     * This avoids loading the entire workbook into memory.
     */
    private static void extractOverviewSheetUsingZip(Path inputPath, Path outputPath, String overviewPartName) throws Exception {
        // Extract the sheet filename from the part name (e.g., "/xl/worksheets/sheet3.xml" -> "sheet3.xml")
        String overviewSheetFile = overviewPartName.substring(overviewPartName.lastIndexOf('/') + 1);
        System.out.println("🔍 Target Overview sheet file: " + overviewSheetFile);
        
        try (FileInputStream fis = new FileInputStream(inputPath.toFile());
             ZipInputStream zis = new ZipInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            
            byte[] buffer = new byte[8192];
            ZipEntry entry;
            boolean workbookXmlFound = false;
            boolean overviewSheetFound = false;
            
            System.out.println("🔄 Starting ZIP-level file manipulation...");
            
            while ((entry = zis.getNextEntry()) != null) {
                String entryName = entry.getName();
                
                // Always copy these essential OOXML files
                if (entryName.equals("[Content_Types].xml") ||
                    entryName.equals("_rels/.rels") ||
                    entryName.startsWith("docProps/") ||
                    entryName.equals("xl/sharedStrings.xml") ||
                    entryName.equals("xl/styles.xml") ||
                    entryName.equals("xl/theme/theme1.xml") ||
                    entryName.startsWith("xl/media/") ||
                    entryName.startsWith("xl/drawings/") ||
                    entryName.startsWith("xl/charts/")) {
                    
                    System.out.println("📋 Copying essential file: " + entryName);
                    copyZipEntry(zis, zos, entry, buffer);
                    zis.closeEntry();
                    continue;
                }
                
                // Handle workbook.xml - modify to only include Overview sheet
                if (entryName.equals("xl/workbook.xml")) {
                    System.out.println("📝 Modifying workbook.xml to include only Overview sheet");
                    copyModifiedWorkbook(zis, zos, entry, overviewSheetFile);
                    workbookXmlFound = true;
                    zis.closeEntry();
                    continue;
                }
                
                // Handle workbook relationships - modify to only include Overview sheet
                if (entryName.equals("xl/_rels/workbook.xml.rels")) {
                    System.out.println("📝 Modifying workbook relationships");
                    copyModifiedWorkbookRels(zis, zos, entry, overviewSheetFile);
                    zis.closeEntry();
                    continue;
                }
                
                // Copy only the Overview sheet XML file
                if (entryName.startsWith("xl/worksheets/") && entryName.endsWith(".xml")) {
                    String sheetFileName = entryName.substring(entryName.lastIndexOf('/') + 1);
                    if (sheetFileName.equals(overviewSheetFile)) {
                        System.out.println("📋 Copying Overview sheet: " + entryName);
                        copyZipEntry(zis, zos, entry, buffer);
                        overviewSheetFound = true;
                    } else {
                        System.out.println("⏭️  Skipping sheet: " + entryName);
                    }
                    zis.closeEntry();
                    continue;
                }
                
                // Copy sheet relationships for Overview sheet only
                if (entryName.startsWith("xl/worksheets/_rels/") && entryName.endsWith(".xml.rels")) {
                    String baseSheetName = entryName.substring(entryName.lastIndexOf('/') + 1).replace(".xml.rels", ".xml");
                    if (baseSheetName.equals(overviewSheetFile)) {
                        System.out.println("📋 Copying Overview sheet relationships: " + entryName);
                        copyZipEntry(zis, zos, entry, buffer);
                    }
                    zis.closeEntry();
                    continue;
                }
                
                // Skip all other entries
                zis.closeEntry();
            }
            
            if (!workbookXmlFound) {
                throw new Exception("Failed to find workbook.xml");
            }
            if (!overviewSheetFound) {
                throw new Exception("Failed to find Overview sheet: " + overviewSheetFile);
            }
            
            System.out.println("✅ ZIP extraction completed successfully");
        }
    }
    
    /**
     * Check if a sheet file corresponds to the Overview sheet
     */
    private static boolean isOverviewSheet(String sheetFileName, String overviewPartName) {
        // Extract the sheet file name from the part name
        if (overviewPartName != null && overviewPartName.contains("/")) {
            String expectedFileName = overviewPartName.substring(overviewPartName.lastIndexOf('/') + 1);
            return sheetFileName.equals(expectedFileName);
        }
        // Fallback - assume the first sheet file is the Overview
        return sheetFileName.equals("sheet1.xml");
    }
    
    /**
     * Copy a ZIP entry as-is
     */
    private static void copyZipEntry(ZipInputStream zis, ZipOutputStream zos, ZipEntry entry, byte[] buffer) throws IOException {
        ZipEntry newEntry = new ZipEntry(entry.getName());
        zos.putNextEntry(newEntry);
        
        int len;
        while ((len = zis.read(buffer)) > 0) {
            zos.write(buffer, 0, len);
        }
        zos.closeEntry();
    }
    
    /**
     * Copy and modify workbook.xml to only include Overview sheet
     */
    private static void copyModifiedWorkbook(ZipInputStream zis, ZipOutputStream zos, ZipEntry entry, String overviewSheetFile) throws IOException {
        // Read the entire workbook.xml content
        StringBuilder content = new StringBuilder();
        byte[] buffer = new byte[8192];
        int len;
        while ((len = zis.read(buffer)) > 0) {
            content.append(new String(buffer, 0, len, StandardCharsets.UTF_8));
        }
        
        String xml = content.toString();
        
        // Keep only the Overview sheet definition
        // Find the sheet element with name="Overview" and keep only that one
        String modifiedXml = xml;
        
        // Remove all sheet elements except the Overview sheet
        modifiedXml = modifiedXml.replaceAll("<sheet[^>]*name=\"(?!Overview\")[^\"]*\"[^>]*/>", "");
        
        // Also ensure the sheet references are correct for the single remaining sheet
        // Update sheetId to 1 for the Overview sheet
        modifiedXml = modifiedXml.replaceAll("(<sheet[^>]*name=\"Overview\"[^>]*sheetId=\")[^\"]*", "$11");
        
        ZipEntry newEntry = new ZipEntry(entry.getName());
        zos.putNextEntry(newEntry);
        zos.write(modifiedXml.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
    }
    
    /**
     * Copy and modify workbook relationships to only include Overview sheet
     */
    private static void copyModifiedWorkbookRels(ZipInputStream zis, ZipOutputStream zos, ZipEntry entry, String overviewSheetFile) throws IOException {
        // Read the relationships XML
        StringBuilder content = new StringBuilder();
        byte[] buffer = new byte[8192];
        int len;
        while ((len = zis.read(buffer)) > 0) {
            content.append(new String(buffer, 0, len, StandardCharsets.UTF_8));
        }
        
        String xml = content.toString();
        
        // Keep only the Overview sheet relationship and essential relationships (styles, sharedStrings, theme, etc.)
        // Remove worksheet relationships that don't target our Overview sheet
        String targetSheet = "worksheets/" + overviewSheetFile;
        String modifiedXml = xml.replaceAll("<Relationship(?![^>]*Target=\"" + java.util.regex.Pattern.quote(targetSheet) + "\")[^>]*Type=\"[^\"]*worksheet\"[^>]*/>", "");
        
        ZipEntry newEntry = new ZipEntry(entry.getName());
        zos.putNextEntry(newEntry);
        zos.write(modifiedXml.getBytes(StandardCharsets.UTF_8));
        zos.closeEntry();
    }
    
    
    /**
     * Extract Overview sheet from XLS file.
     * 
     * @param filePath Path to the XLS file to process
     * @return true if Overview sheet was found and extracted
     */
    private static boolean extractFromXLS(Path filePath) throws Exception {
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             HSSFWorkbook workbook = new HSSFWorkbook(fis)) {

            System.out.println("📖 XLS Workbook loaded successfully. Found " + workbook.getNumberOfSheets() + " sheets");

            int overviewSheetIndex = -1;
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                String sheetName = workbook.getSheetName(i);
                System.out.println("📋 Found sheet: '" + sheetName + "'");
                
                if ("Overview".equals(sheetName)) {
                    System.out.println("⚡ Found Overview sheet: " + sheetName);
                    overviewSheetIndex = i;
                    break;
                }
            }

            if (overviewSheetIndex == -1) {
                return false;
            }

            // Remove all sheets except the Overview sheet
            for (int i = workbook.getNumberOfSheets() - 1; i >= 0; i--) {
                if (i != overviewSheetIndex) {
                    System.out.println("🗑️  Removing sheet: " + workbook.getSheetName(i));
                    workbook.removeSheetAt(i);
                }
            }

            Path outputPath = getOverviewOutputPath(filePath);
            try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {
                workbook.write(fos);
            }

            System.out.println("💾 Created Overview file: " + outputPath.getFileName());
            System.out.println("📊 Preserved complete Overview sheet with all formatting, charts, and images");
            return true;
        }
    }
}