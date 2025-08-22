-- SQLite Direct Importer Database Schema
-- This file documents the database structure created by SQLiteDirectImporter.java

-- Main table for transfer report data imported directly from Excel files
CREATE TABLE transfer_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT,
    source_file_size BIGINT,
    target_file_size BIGINT,
    source_file_id TEXT,
    target_file_id TEXT,
    source_account TEXT,
    source_namespace TEXT,
    target_account TEXT,
    source_created_by TEXT,
    creation_time DATETIME,
    source_last_modified_by TEXT,
    source_last_modification_time DATETIME,
    target_last_modification_time DATETIME,
    last_access_time DATETIME,
    start_time DATETIME,
    transfer_time DATETIME,
    checksum_method TEXT,
    checksum TEXT,
    file_status TEXT,
    errors TEXT,
    status TEXT,
    translated_file_name TEXT,
    parent_folder TEXT,
    parent_id TEXT,
    level INTEGER,
    job_name TEXT,
    import_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_name, target_file_id)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_file_name ON transfer_data (file_name);
CREATE INDEX IF NOT EXISTS idx_target_file_id ON transfer_data (target_file_id);
CREATE INDEX IF NOT EXISTS idx_file_status ON transfer_data (file_status);
CREATE INDEX IF NOT EXISTS idx_status ON transfer_data (status);
CREATE INDEX IF NOT EXISTS idx_parent_folder ON transfer_data (parent_folder);
CREATE INDEX IF NOT EXISTS idx_parent_id ON transfer_data (parent_id);
CREATE INDEX IF NOT EXISTS idx_level ON transfer_data (level);
CREATE INDEX IF NOT EXISTS idx_source_file_size ON transfer_data (source_file_size);
CREATE INDEX IF NOT EXISTS idx_job_name ON transfer_data (job_name);

-- ANALYSIS VIEWS
-- ===============

-- Files View: All records where source_file_size > 0 (actual files)
CREATE VIEW IF NOT EXISTS files_view AS 
SELECT * FROM transfer_data WHERE source_file_size > 0;

-- Folders View: All records where source_file_size = 0 or NULL (folders)
CREATE VIEW IF NOT EXISTS folders_view AS 
SELECT * FROM transfer_data WHERE source_file_size = 0 OR source_file_size IS NULL;

-- Status Summary: Aggregation of all status types with counts
CREATE VIEW IF NOT EXISTS status_summary AS 
SELECT 
  COALESCE(file_status, 'Unknown') as status_name,
  COUNT(*) as record_count,
  COUNT(CASE WHEN source_file_size > 0 THEN 1 END) as file_count,
  COUNT(CASE WHEN source_file_size = 0 OR source_file_size IS NULL THEN 1 END) as folder_count
FROM transfer_data 
GROUP BY COALESCE(file_status, 'Unknown') 
ORDER BY record_count DESC;

-- Hierarchical Children View: Recursive view showing parent-child relationships
CREATE VIEW IF NOT EXISTS hierarchy_children AS 
WITH RECURSIVE hierarchy_tree(id, file_name, target_file_id, parent_id, level, depth, path) AS (
  SELECT id, file_name, target_file_id, parent_id, level, 0 as depth, file_name as path 
  FROM transfer_data 
  WHERE parent_id IS NULL 
  UNION ALL 
  SELECT t.id, t.file_name, t.target_file_id, t.parent_id, t.level, h.depth + 1, h.path || ' > ' || t.file_name 
  FROM transfer_data t 
  INNER JOIN hierarchy_tree h ON t.parent_id = h.target_file_id 
) 
SELECT * FROM hierarchy_tree ORDER BY path;


-- DYNAMIC STATUS VIEWS
-- ====================
-- Note: Individual status views are created dynamically by SQLiteDirectImporter.java
-- Examples of what gets created:
-- CREATE VIEW IF NOT EXISTS status_match_exists AS SELECT * FROM transfer_data WHERE file_status = 'match-exists';
-- CREATE VIEW IF NOT EXISTS status_filtered AS SELECT * FROM transfer_data WHERE file_status = 'filtered';
-- CREATE VIEW IF NOT EXISTS status_success AS SELECT * FROM transfer_data WHERE file_status = 'success';

-- SAMPLE QUERIES
-- ==============
-- To view all status types: SELECT * FROM status_summary;
-- To see hierarchy for a specific parent: SELECT * FROM hierarchy_children WHERE target_file_id = 'some-parent-id';
-- To find all files with specific status: SELECT * FROM status_match_exists;
-- To see all files: SELECT * FROM files_view LIMIT 10;
-- To see all folders: SELECT * FROM folders_view LIMIT 10;
-- To count records by level: SELECT level, COUNT(*) FROM transfer_data GROUP BY level ORDER BY level;
-- To see records by job: SELECT job_name, COUNT(*) FROM transfer_data GROUP BY job_name ORDER BY job_name;
-- To find specific job data: SELECT * FROM transfer_data WHERE job_name = 'MyExcelFileName' LIMIT 10;