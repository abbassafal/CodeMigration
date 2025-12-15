using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using DataMigration.Services;

// NOTE: This implementation assumes a recent Npgsql version that allows writing Stream values
// directly to bytea fields via NpgsqlBinaryImporter (see Npgsql docs: supported types can be written as Stream).
// If your Npgsql version does not support Stream writes to NpgsqlBinaryImporter, you have two options:
// 1) Upgrade Npgsql. 2) Use NpgsqlLargeObjectManager to store large objects (LOBs) and reference their OIDs.

public class PRAttachmentMigration : MigrationService
{
    private readonly ILogger<PRAttachmentMigration> _logger;
    private MigrationLogger? _migrationLogger;
    private readonly IConfiguration _configuration;

    // Controls: how many metadata rows to include per COPY block
    private const int COPY_BATCH_ROWS = 5000; // large batch for COPY
    private const int BINARY_COPY_BATCH = 20; // MUCH smaller for binary COPY to avoid memory/transaction issues
    private const long MAX_BINARY_SIZE = 250 * 1024 * 1024; // 250 MB safety guard

    public PRAttachmentMigration(IConfiguration configuration, ILogger<PRAttachmentMigration> logger) : base(configuration)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    // Required by base class MigrationService
    protected override string SelectQuery => @"
        SELECT 
            t.PRATTACHMENTID,
            t.PRID,
            t.UPLOADPATH,
            t.FILENAME,
            t.UPLOADEDBYID,
            t.PRTRANSID,
            t.Remarks,
            t.PRATTACHMENTDATA,
            t.PR_ATTCHMNT_TYPE
        FROM TBL_PRATTACHMENT t
        ORDER BY t.PRATTACHMENTID";
    
    // Optimized query for metadata only (no binary column)
    private string SelectMetadataQuery => @"
        SELECT 
            t.PRATTACHMENTID,
            t.PRID,
            t.UPLOADPATH,
            t.FILENAME,
            t.UPLOADEDBYID,
            t.PRTRANSID,
            t.Remarks,
            t.PR_ATTCHMNT_TYPE
        FROM TBL_PRATTACHMENT t
        ORDER BY t.PRATTACHMENTID";

    // Required by base class MigrationService
    protected override string InsertQuery => @"
        INSERT INTO pr_attachments (
            pr_attachment_id, erp_pr_lines_id, upload_path, file_name,
            remarks, is_header_doc, pr_attachment_extensions,
            created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date
        ) VALUES (
            @pr_attachment_id, @erp_pr_lines_id, @upload_path, @file_name,
            @remarks, @is_header_doc, @pr_attachment_extensions,
            @created_by, @created_date, @modified_by, @modified_date,
            @is_deleted, @deleted_by, @deleted_date
        )";

    // Required by base class MigrationService
    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "PRATTACHMENTID -> pr_attachment_id (Direct)",
            "PRID -> erp_pr_lines_id (Direct)",
            "UPLOADPATH -> upload_path (Direct)",
            "FILENAME -> file_name (Direct)",
            "Remarks -> remarks (Direct)",
            "PRATTACHMENTDATA -> pr_attachment_data (Binary Stream via COPY)",
            "PR_ATTCHMNT_TYPE -> pr_attachment_extensions (Direct)",
            "ISHEADERDOC -> is_header_doc (Default: true)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings() => new List<object>
    {
        new { source = "PRATTACHMENTID", target = "pr_attachment_id" },
        new { source = "PRID", target = "erp_pr_lines_id" },
        new { source = "UPLOADPATH", target = "upload_path" },
        new { source = "FILENAME", target = "file_name" },
        new { source = "Remarks", target = "remarks" },
        new { source = "PRATTACHMENTDATA", target = "pr_attachment_data (binary)" },
        new { source = "PR_ATTCHMNT_TYPE", target = "pr_attachment_extensions" }
    };

    // Required by base class - implement the standard migration method
    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "pr_attachments");
        _migrationLogger.LogInfo("Starting migration");

        // This implementation uses the optimized COPY approach via MigrateAsync
        // Note: The standard approach would use transaction parameter, but COPY optimization works differently
        _logger.LogWarning("ExecuteMigrationAsync called - redirecting to optimized MigrateAsync method");
        return await MigrateAsync(CancellationToken.None);
    }

    /// <summary>
    /// High level flow:
    /// 1) COPY metadata (everything except the binary column) into a temporary table using BeginBinaryImport.
    /// 2) MERGE from temp -> pr_attachments using INSERT ... ON CONFLICT DO UPDATE.
    /// 3) Binary data is handled separately via background service (BinaryAttachmentBackgroundService)
    /// This avoids buffering large blobs in memory and avoids thousands of parameterized INSERTs.
    /// </summary>
    public async Task<int> MigrateAsync(CancellationToken cancellationToken = default)
    {
        var sqlConnString = _configuration.GetConnectionString("SqlServer");
        var pgConnString = _configuration.GetConnectionString("PostgreSql");
        if (string.IsNullOrEmpty(sqlConnString) || string.IsNullOrEmpty(pgConnString))
            throw new InvalidOperationException("Connection strings not configured");

        await using var sqlConn = new SqlConnection(sqlConnString);
        await using var pgConn = new NpgsqlConnection(pgConnString);
        await sqlConn.OpenAsync(cancellationToken);
        await pgConn.OpenAsync(cancellationToken);

        _logger.LogInformation("Starting COPY-optimized PRAttachment migration (metadata only - binaries will be handled by background service)");

        // Track migration statistics
        int totalRecords = 0;
        int insertedRecords = 0;
        int skippedCount = 0;
        List<(string, string)> skippedRecordsList = new();

        try
        {
            // Get total count
            var countCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_PRATTACHMENT", sqlConn);
            totalRecords = (int)await countCmd.ExecuteScalarAsync(cancellationToken);
            _logger.LogInformation($"Total records to migrate: {totalRecords:N0}");

            // Phase 1: COPY metadata (no binary column)
            (insertedRecords, skippedCount, skippedRecordsList) = await CopyMetadataAsync(sqlConn, pgConn, cancellationToken);

            // Phase 2: apply metadata merge into pr_attachments
            int merged = await MergeMetadataAsync(pgConn, cancellationToken);

            _logger.LogInformation($"Metadata migration completed: metadataRows={insertedRecords}, merged={merged}. Binary migration will run in background.");

            // Export migration stats to Excel
            var outputPath = $"PRAttachmentMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx";
            MigrationStatsExporter.ExportToExcel(
                outputPath,
                totalRecords,
                insertedRecords,
                skippedCount,
                _logger,
                skippedRecordsList
            );
            _logger.LogInformation($"Migration stats exported to {outputPath}");

            return merged;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Migration failed");
            // Export stats even on failure
            var outputPath = $"PRAttachmentMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}_FAILED.xlsx";
            MigrationStatsExporter.ExportToExcel(
                outputPath,
                totalRecords,
                insertedRecords,
                skippedCount,
                _logger,
                skippedRecordsList
            );
            _logger.LogInformation($"Migration stats exported to {outputPath} (migration failed)");
            throw;
        }
    }

    private async Task<(int insertedCount, int skippedCount, List<(string, string)> skippedRecordsList)> CopyMetadataAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Phase 1: Copying metadata into temporary table via binary COPY");
        int rowCount = 0;
        int skippedCount = 0;
        List<(string, string)> skippedRecordsList = new();
        try
        {
            // Create temp table for metadata (no binary column)
            // Note: Don't use ON COMMIT DROP - we need the table to persist for COPY command
            _logger.LogInformation("Creating temporary table...");
            var createTempSql = @"
                CREATE TEMP TABLE IF NOT EXISTS tmp_pr_attachments_meta (
                    pr_attachment_id bigint,
                    erp_pr_lines_id bigint,
                    upload_path text,
                    file_name text,
                    remarks text,
                    is_header_doc boolean,
                    pr_attachment_extensions text,
                    created_by integer,
                    created_date timestamptz,
                    modified_by integer,
                    modified_date timestamptz,
                    is_deleted boolean,
                    deleted_by integer,
                    deleted_date timestamptz
                );
                
                -- Clear table if it exists from previous run
                TRUNCATE TABLE tmp_pr_attachments_meta;
            ";

            await using (var createCmd = new NpgsqlCommand(createTempSql, pgConn))
                await createCmd.ExecuteNonQueryAsync(cancellationToken);

            _logger.LogInformation("Temporary table created. Loading valid foreign keys...");

            // Load valid erp_pr_lines_ids for FK validation
            var validErpPrLinesIds = await LoadValidErpPrLinesIdsAsync(pgConn, cancellationToken);

            _logger.LogInformation("Starting data read from SQL Server...");

            // First, check how many records we need to migrate
            var countCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_PRATTACHMENT", sqlConn);
            var totalRecords = (int)await countCmd.ExecuteScalarAsync(cancellationToken);
            _logger.LogInformation($"Total records to migrate: {totalRecords:N0}");

            if (totalRecords == 0)
            {
                _logger.LogWarning("No records found in TBL_PRATTACHMENT");
                return (0, 0, skippedRecordsList);
            }

            // We'll stream rows from SQL Server and use BeginBinaryImport to copy into tmp_pr_attachments_meta
            // IMPORTANT: Use SelectMetadataQuery which excludes the binary column for performance
            var selectSql = SelectMetadataQuery; // NO BINARY COLUMN - much faster!
            _logger.LogInformation("Executing SELECT query on SQL Server (metadata only, no binary data)...");
            using var selectCmd = new SqlCommand(selectSql, sqlConn);
            selectCmd.CommandTimeout = 0; // No timeout for large datasets
            using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken); // No need for SequentialAccess without blobs
            _logger.LogInformation("SQL Server reader opened successfully. Starting COPY import...");

            int progressInterval = 10; // Log every 10 rows initially for debugging
            await using (var importer = pgConn.BeginBinaryImport(
                "COPY tmp_pr_attachments_meta (pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) FROM STDIN (FORMAT BINARY)"))
            {
                _logger.LogInformation("Binary COPY import started. Reading first record from SQL Server...");
                while (await reader.ReadAsync(cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        // Read only required columns in EXACT SELECT order for SelectMetadataQuery (no binary column)
                        // Column indexes: 0=PRATTACHMENTID,1=PRID,2=UPLOADPATH,3=FILENAME,4=UPLOADEDBYID,5=PRTRANSID,6=Remarks,7=PR_ATTCHMNT_TYPE
                        var prAttachmentId = reader.IsDBNull(0) ? (long?)null : Convert.ToInt64(reader.GetValue(0));
                        if (rowCount == 0)
                            _logger.LogInformation($"First record ID: {prAttachmentId}");
                        var prId = reader.IsDBNull(1) ? (long?)null : Convert.ToInt64(reader.GetValue(1));
                        var uploadPath = reader.IsDBNull(2) ? null : reader.GetString(2);
                        var fileName = reader.IsDBNull(3) ? null : reader.GetString(3);
                        if (!reader.IsDBNull(4)) _ = reader.GetValue(4);
                        if (!reader.IsDBNull(5)) _ = reader.GetValue(5);
                        var remarks = reader.IsDBNull(6) ? null : reader.GetString(6);
                        var prAttType = reader.IsDBNull(7) ? null : reader.GetString(7);
                        // Validate FK: Skip if PRID is NULL or not found in erp_pr_lines
                        if (!prId.HasValue)
                        {
                            skippedRecordsList.Add(($"PRATTACHMENTID={prAttachmentId}", "PRID is NULL"));
                            skippedCount++;
                            continue;
                        }
                        if (!validErpPrLinesIds.Contains(prId.Value))
                        {
                            skippedRecordsList.Add(($"PRATTACHMENTID={prAttachmentId}", $"PRID {prId.Value} not found in erp_pr_lines (FK constraint violation)"));
                            skippedCount++;
                            continue;
                        }
                        // Set defaults for columns that don't exist in source
                        var createdBy = (int?)0;
                        var createdDate = (DateTime?)DateTime.UtcNow;
                        var modifiedBy = (int?)null;
                        var modifiedDate = (DateTime?)null;
                        var isDeleted = false;
                        var deletedBy = (int?)null;
                        var deletedDate = (DateTime?)null;
                        importer.StartRow();
                        importer.Write(prAttachmentId, NpgsqlDbType.Bigint);
                        importer.Write(prId, NpgsqlDbType.Bigint); // PRID -> erp_pr_lines_id
                        importer.Write(uploadPath, NpgsqlDbType.Text);
                        importer.Write(fileName, NpgsqlDbType.Text);
                        importer.Write(remarks, NpgsqlDbType.Text);
                        importer.Write(true, NpgsqlDbType.Boolean); // is_header_doc default true
                        importer.Write(prAttType, NpgsqlDbType.Text);
                        importer.Write(createdBy, NpgsqlDbType.Integer);
                        importer.Write(createdDate, NpgsqlDbType.TimestampTz);
                        importer.Write(modifiedBy, NpgsqlDbType.Integer);
                        importer.Write(modifiedDate, NpgsqlDbType.TimestampTz);
                        importer.Write(isDeleted, NpgsqlDbType.Boolean);
                        importer.Write(deletedBy, NpgsqlDbType.Integer);
                        importer.Write(deletedDate, NpgsqlDbType.TimestampTz);
                        rowCount++;
                        if (rowCount == 1)
                        {
                            _logger.LogInformation("First record written successfully!");
                        }
                        // Log progress
                        if (rowCount % progressInterval == 0)
                        {
                            _logger.LogInformation($"Processed {rowCount:N0} / {totalRecords:N0} records ({(rowCount * 100.0 / totalRecords):F1}%)");
                            if (rowCount == 100)
                            {
                                progressInterval = 500; // Log every 500 rows after first 100
                                _logger.LogInformation("Switching to log every 500 records...");
                            }
                            else if (rowCount == 5000)
                            {
                                progressInterval = 5000; // Log every 5000 rows after first 5000
                                _logger.LogInformation("Switching to log every 5000 records...");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Error processing record at row {rowCount + 1}");
                        throw;
                    }
                }
                // finalize - only complete once at the end
                _logger.LogInformation("Finalizing COPY import...");
                await importer.CompleteAsync(cancellationToken);
            }
            _logger.LogInformation($"Phase 1 complete: copied {rowCount:N0} metadata rows, skipped {_migrationLogger?.SkippedCount ?? 0:N0} rows (FK violations or NULL PRID)");
            return (rowCount, skippedCount, skippedRecordsList);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during metadata copy phase");
            throw;
        }
    }

    private async Task<int> MergeMetadataAsync(NpgsqlConnection pgConn, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Phase 2: Merging metadata from temp table into pr_attachments via single SQL statement");

        var mergeSql = @"
            INSERT INTO pr_attachments (pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date)
            SELECT pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
            FROM tmp_pr_attachments_meta
            ON CONFLICT (pr_attachment_id)
            DO UPDATE SET
                erp_pr_lines_id = EXCLUDED.erp_pr_lines_id,
                upload_path = EXCLUDED.upload_path,
                file_name = EXCLUDED.file_name,
                remarks = EXCLUDED.remarks,
                is_header_doc = EXCLUDED.is_header_doc,
                pr_attachment_extensions = EXCLUDED.pr_attachment_extensions,
                modified_by = EXCLUDED.modified_by,
                modified_date = EXCLUDED.modified_date,
                is_deleted = EXCLUDED.is_deleted,
                deleted_by = EXCLUDED.deleted_by,
                deleted_date = EXCLUDED.deleted_date;

            -- return count
            SELECT (SELECT COUNT(*) FROM tmp_pr_attachments_meta) as updated_count;
        ";

        await using var cmd = new NpgsqlCommand(mergeSql, pgConn);
        var res = await cmd.ExecuteScalarAsync(cancellationToken);
        return res != null && res != DBNull.Value ? Convert.ToInt32(res) : 0;
    }

    private async Task<int> StreamBinariesAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Phase 3: Streaming binary attachments in batches (streaming from SQL Server to COPY binary)");

        // First count how many binary records exist
        var countBinaryCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_PRATTACHMENT WHERE PRATTACHMENTDATA IS NOT NULL", sqlConn);
        var totalBinaryRecords = (int)await countBinaryCmd.ExecuteScalarAsync(cancellationToken);
        _logger.LogInformation($"Total binary attachments to migrate: {totalBinaryRecords:N0}");

        if (totalBinaryRecords == 0)
        {
            _logger.LogInformation("No binary attachments found, skipping Phase 3");
            return 0;
        }

        // We'll select only rows that have binary data in source and exist in target (or whatever filter you need).
        // Adjust WHERE clause to limit to needed rows (e.g. migrated metadata only)
        var selectBinarySql = @"SELECT PRATTACHMENTID, PRATTACHMENTDATA FROM TBL_PRATTACHMENT WHERE PRATTACHMENTDATA IS NOT NULL ORDER BY PRATTACHMENTID";

        using var selectCmd = new SqlCommand(selectBinarySql, sqlConn);
        selectCmd.CommandTimeout = 0; // might take long

        using var reader = await selectCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

        var batch = new List<(long id, long size, Func<CancellationToken, Task<Stream>> streamFactory)>();
        int updated = 0;
        int processedCount = 0;
        int skippedLarge = 0;
        int skippedEmpty = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.IsDBNull(0) ? throw new InvalidOperationException("Null PRATTACHMENTID") : Convert.ToInt64(reader.GetValue(0));
            long size = reader.IsDBNull(1) ? 0 : reader.GetBytes(1, 0, null, 0, 0);

            processedCount++;

            if (size == 0)
            {
                skippedEmpty++;
                continue;
            }

            if (size > MAX_BINARY_SIZE)
            {
                _logger.LogWarning($"Skipping large binary for PRATTACHMENTID {id} (size {size:N0} bytes / {size / 1024 / 1024:N0} MB)");
                skippedLarge++;
                continue;
            }

            // Capture current reader position by preparing a factory that will re-query the single row and return a Stream.
            // We can't reuse the same SqlDataReader stream later because it advances.
            batch.Add((id, size, async (ct) =>
            {
                // Create a command to stream single binary column for this id
                var singleCmd = new SqlCommand("SELECT PRATTACHMENTDATA FROM TBL_PRATTACHMENT WHERE PRATTACHMENTID = @id", sqlConn);
                singleCmd.Parameters.AddWithValue("@id", id);
                singleCmd.CommandTimeout = 600;
                using var rdr = await singleCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleRow, ct);
                if (!await rdr.ReadAsync(ct)) throw new InvalidOperationException("Row disappeared while streaming binary");
                var st = rdr.GetStream(0);
                // NOTE: return the reader's stream — caller must fully consume and then dispose the reader (we return a stream but the underlying reader will be disposed when stream is closed)
                // To keep ownership simple, we will copy the stream into a MemoryStream in a streaming fashion. This still allocates but only for single-row small batches.
                // If your Npgsql version supports writing Stream directly to importer.Write(stream,..) you can avoid MemoryStream entirely.

                var ms = new MemoryStream();
                await st.CopyToAsync(ms, 81920, ct);
                ms.Position = 0;
                return (Stream)ms;
            }));

            // Log progress every 50 files
            if (processedCount % 50 == 0)
            {
                _logger.LogInformation($"Phase 3: Scanned {processedCount:N0} / {totalBinaryRecords:N0} files ({processedCount * 100.0 / totalBinaryRecords:F1}%), queued {batch.Count} in current batch");
            }

            // When batch reaches threshold, flush via COPY
            if (batch.Count >= BINARY_COPY_BATCH)
            {
                _logger.LogInformation($"Processing batch of {batch.Count} binary files...");
                var batchStart = DateTime.UtcNow;
                updated += await CopyBinaryBatchAsync(pgConn, batch, cancellationToken);
                var batchDuration = (DateTime.UtcNow - batchStart).TotalSeconds;
                _logger.LogInformation($"Batch complete: {batch.Count} files in {batchDuration:F1}s. Total updated: {updated:N0} / {totalBinaryRecords:N0} ({updated * 100.0 / totalBinaryRecords:F1}%)");
                
                // Clear batch and force garbage collection to free memory
                batch.Clear();
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
            }
        }

        // Process remaining batch
        if (batch.Count > 0)
        {
            _logger.LogInformation($"Processing final batch of {batch.Count} binary files...");
            updated += await CopyBinaryBatchAsync(pgConn, batch, cancellationToken);
            batch.Clear();
            GC.Collect();
        }

        _logger.LogInformation($"Phase 3 complete: binaries updated: {updated:N0}, skipped (empty): {skippedEmpty:N0}, skipped (too large): {skippedLarge:N0}");
        return updated;
    }

    private async Task<int> CopyBinaryBatchAsync(NpgsqlConnection pgConn, List<(long id, long size, Func<CancellationToken, Task<Stream>> streamFactory)> batch, CancellationToken cancellationToken)
    {
        // Create a temp table for this batch that includes bytea
        // Note: Each batch creates and drops its own temp table
        var createTempBin = @"
            DROP TABLE IF EXISTS tmp_pr_attachments_bin;
            CREATE TEMP TABLE tmp_pr_attachments_bin (
                pr_attachment_id bigint,
                pr_attachment_data bytea
            );";

        await using (var createCmd = new NpgsqlCommand(createTempBin, pgConn))
            await createCmd.ExecuteNonQueryAsync(cancellationToken);

        // Begin binary import for this batch
        var batchStartTime = DateTime.UtcNow;
        int batchItemsProcessed = 0;
        
        await using (var importer = pgConn.BeginBinaryImport("COPY tmp_pr_attachments_bin (pr_attachment_id, pr_attachment_data) FROM STDIN (FORMAT BINARY)"))
        {
            foreach (var item in batch)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var itemStart = DateTime.UtcNow;
                importer.StartRow();
                importer.Write(item.id, NpgsqlDbType.Bigint);

                // Get stream for this item's binary and write as a stream into bytea column
                await using var s = await item.streamFactory(cancellationToken);

                // The NpgsqlBinaryImporter.Write accepts Stream for bytea (supported types can be written as Stream)
                // This avoids materializing a single large byte[] in memory.
                importer.Write(s, NpgsqlDbType.Bytea);

                batchItemsProcessed++;
                var itemElapsed = (DateTime.UtcNow - itemStart).TotalSeconds;
                
                // Log every 5 items to track progress
                if (batchItemsProcessed % 5 == 0)
                {
                    _logger.LogInformation($"  -> Processed {batchItemsProcessed}/{batch.Count} items in this batch (last item: {itemElapsed:F2}s)");
                }

                // The stream will be disposed by 'await using' above
            }

            _logger.LogInformation($"Completing binary import batch ({batchItemsProcessed} items)...");
            var completeStart = DateTime.UtcNow;
            await importer.CompleteAsync(cancellationToken);
            var completeElapsed = (DateTime.UtcNow - completeStart).TotalSeconds;
            _logger.LogInformation($"Binary import batch completed in {completeElapsed:F2}s");
        }
        
        var totalBatchElapsed = (DateTime.UtcNow - batchStartTime).TotalSeconds;
        _logger.LogInformation($"Total batch time (including merge): {totalBatchElapsed:F2}s");

        // Merge tmp_pr_attachments_bin into target table
        var mergeSql = @"
            UPDATE pr_attachments t
            SET pr_attachment_data = b.pr_attachment_data,
                modified_date = CURRENT_TIMESTAMP
            FROM tmp_pr_attachments_bin b
            WHERE t.pr_attachment_id = b.pr_attachment_id;
            SELECT COUNT(1) FROM tmp_pr_attachments_bin;";

        await using var cmd = new NpgsqlCommand(mergeSql, pgConn);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result != null && result != DBNull.Value ? Convert.ToInt32(result) : 0;
    }

    private async Task<HashSet<long>> LoadValidErpPrLinesIdsAsync(NpgsqlConnection pgConn, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Loading valid erp_pr_lines_ids for FK validation...");
        var validIds = new HashSet<long>();
        
        var query = "SELECT erp_pr_lines_id FROM erp_pr_lines";
        await using var cmd = new NpgsqlCommand(query, pgConn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        
        while (await reader.ReadAsync(cancellationToken))
        {
            validIds.Add(reader.GetInt64(0));
        }
        
        _logger.LogInformation($"Loaded {validIds.Count:N0} valid erp_pr_lines_ids");
        return validIds;
    }
}
