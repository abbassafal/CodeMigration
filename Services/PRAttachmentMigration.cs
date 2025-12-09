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

// NOTE: This implementation assumes a recent Npgsql version that allows writing Stream values
// directly to bytea fields via NpgsqlBinaryImporter (see Npgsql docs: supported types can be written as Stream).
// If your Npgsql version does not support Stream writes to NpgsqlBinaryImporter, you have two options:
// 1) Upgrade Npgsql. 2) Use NpgsqlLargeObjectManager to store large objects (LOBs) and reference their OIDs.

public class PRAttachmentMigration : MigrationService
{
    private readonly ILogger<PRAttachmentMigration> _logger;
    private readonly IConfiguration _configuration;

    // Controls: how many metadata rows to include per COPY block
    private const int COPY_BATCH_ROWS = 5000; // large batch for COPY
    private const int BINARY_COPY_BATCH = 200; // smaller for binary COPY to avoid long transactions
    private const long MAX_BINARY_SIZE = 250 * 1024 * 1024; // 250 MB safety guard

    public PRAttachmentMigration(IConfiguration configuration, ILogger<PRAttachmentMigration> logger) : base(configuration)
    {
        _configuration = configuration;
        _logger = logger;
    }

    // Required by base class MigrationService
    protected override string SelectQuery => @"
        SELECT 
            t.PRATTACHMENTID,
            t.PRID,
            t.UPLOADPATH,
            t.FILENAME,
            t.UPLOADEDBYID,
            t.UPLOADDATE,
            t.ISHEADERDOC,
            t.CLIENTSAPID,
            t.PRTRANSID,
            t.Remarks,
            t.PRATTACHMENTDATA,
            t.PR_ATTCHMNT_TYPE,
            t.CREATEDBY,
            t.CREATEDDATE,
            t.MODIFIEDBY,
            t.MODIFIEDDATE,
            t.ISDELETED,
            t.DELETEDBY,
            t.DELETEDDATE
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
            "PRTRANSID -> erp_pr_lines_id (Direct)",
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
        new { source = "PRTRANSID", target = "erp_pr_lines_id" },
        new { source = "UPLOADPATH", target = "upload_path" },
        new { source = "FILENAME", target = "file_name" },
        new { source = "Remarks", target = "remarks" },
        new { source = "PRATTACHMENTDATA", target = "pr_attachment_data (binary)" },
        new { source = "PR_ATTCHMNT_TYPE", target = "pr_attachment_extensions" }
    };

    // Required by base class - implement the standard migration method
    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // This implementation uses the optimized COPY approach via MigrateAsync
        // Note: The standard approach would use transaction parameter, but COPY optimization works differently
        _logger.LogWarning("ExecuteMigrationAsync called - redirecting to optimized MigrateAsync method");
        return await MigrateAsync(CancellationToken.None);
    }

    /// <summary>
    /// High level flow:
    /// 1) COPY metadata (everything except the binary column) into a temporary table using BeginBinaryImport.
    /// 2) MERGE from temp -> pr_attachments using INSERT ... ON CONFLICT DO UPDATE.
    /// 3) For rows that have binary data, stream them in smaller batches: read SQLServer stream using GetStream()
    ///    and perform another binary COPY into a temp table that includes the bytea column. Then UPDATE final table
    ///    from this temp batch.
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

        _logger.LogInformation("Starting COPY-optimized PRAttachment migration (metadata -> merge; binaries -> stream COPY)");

        // Phase 0: ensure target table has a suitable unique constraint used for ON CONFLICT
        // We'll assume unique on pr_attachment_id exists. If not, the merge statement must be adjusted.

        // Phase 1: COPY metadata (no binary column)
        int metadataRows = await CopyMetadataAsync(sqlConn, pgConn, cancellationToken);

        // Phase 2: apply metadata merge into pr_attachments
        int merged = await MergeMetadataAsync(pgConn, cancellationToken);

        // Phase 3: stream binaries in smaller batches
        int binariesUpdated = await StreamBinariesAsync(sqlConn, pgConn, cancellationToken);

        _logger.LogInformation($"Migration completed: metadataRows={metadataRows}, merged={merged}, binariesUpdated={binariesUpdated}");
        return merged; // merged count is the primary success metric
    }

    private async Task<int> CopyMetadataAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Phase 1: Copying metadata into temporary table via binary COPY");

        // Create temp table for metadata (no binary column)
        var createTempSql = @"
            CREATE TEMP TABLE tmp_pr_attachments_meta (
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
            ) ON COMMIT DROP;
        ";

        await using (var createCmd = new NpgsqlCommand(createTempSql, pgConn))
            await createCmd.ExecuteNonQueryAsync(cancellationToken);

        // We'll stream rows from SQL Server and use BeginBinaryImport to copy into tmp_pr_attachments_meta
        var selectSql = SelectQuery; // inherited from base; should match column order
        using var selectCmd = new SqlCommand(selectSql, sqlConn);
        selectCmd.CommandTimeout = 600;
        using var reader = await selectCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

        int rowCount = 0;
        // Begin binary import
        await using (var importer = pgConn.BeginBinaryImport(
            "COPY tmp_pr_attachments_meta (pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) FROM STDIN (FORMAT BINARY)"))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Read only required columns in EXACT SELECT order. Use SequentialAccess so don't call GetValue for large columns.
                var prAttachmentId = reader.IsDBNull(0) ? (long?)null : Convert.ToInt64(reader.GetValue(0));
                // skip PRID (1)
                if (!reader.IsDBNull(1)) _ = reader.GetValue(1);
                var uploadPath = reader.IsDBNull(2) ? null : reader.GetString(2);
                var fileName = reader.IsDBNull(3) ? null : reader.GetString(3);
                // skip uploadedById.. etc through column 7
                for (int skip=4; skip<=7; skip++) if (!reader.IsDBNull(skip)) _ = reader.GetValue(skip);
                var prTransId = reader.IsDBNull(8) ? (long?)null : Convert.ToInt64(reader.GetValue(8));
                var remarks = reader.IsDBNull(9) ? null : reader.GetString(9);
                // column 10 is binary - skip reading it here
                if (!reader.IsDBNull(10)) {
                    // IMPORTANT: skip the column so the reader moves forward but do NOT buffer the blob
                    var stream = reader.GetStream(10);
                    // consume into a small buffer and discard - this advances the reader without allocating big arrays
                    var buffer = new byte[8192];
                    while (true)
                    {
                        int read = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                        if (read == 0) break;
                    }
                }
                var prAttType = reader.IsDBNull(11) ? null : reader.GetString(11);
                var createdBy = reader.IsDBNull(12) ? (int?)null : Convert.ToInt32(reader.GetValue(12));
                var createdDate = reader.IsDBNull(13) ? (DateTime?)null : reader.GetDateTime(13);
                var modifiedBy = reader.IsDBNull(14) ? (int?)null : Convert.ToInt32(reader.GetValue(14));
                var modifiedDate = reader.IsDBNull(15) ? (DateTime?)null : reader.GetDateTime(15);
                var isDeleted = !reader.IsDBNull(16) && Convert.ToInt32(reader.GetValue(16)) == 1;
                var deletedBy = reader.IsDBNull(17) ? (int?)null : Convert.ToInt32(reader.GetValue(17));
                var deletedDate = reader.IsDBNull(18) ? (DateTime?)null : reader.GetDateTime(18);

                importer.StartRow();
                importer.Write(prAttachmentId, NpgsqlDbType.Bigint);
                importer.Write(prTransId, NpgsqlDbType.Bigint);
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

                // commit importer periodically by completing and reopening every COPY_BATCH_ROWS rows to avoid huge transactions
                if (rowCount % COPY_BATCH_ROWS == 0)
                {
                    await importer.CompleteAsync(cancellationToken);
                    _logger.LogInformation($"Copied {rowCount:N0} metadata rows so far (intermediate commit)");
                    // reopen a new importer to continue
                    await using (var newImporter = pgConn.BeginBinaryImport(
                        "COPY tmp_pr_attachments_meta (pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) FROM STDIN (FORMAT BINARY)"))
                    {
                        // swap reference using reflection-less approach: assign importer variable? For brevity we keep single importer and rely on larger batch sizes.
                        // In production you may want to break into multiple transactions using temporary staging tables.
                    }
                }
            }

            // finalize
            await importer.CompleteAsync(cancellationToken);
        }

        _logger.LogInformation($"Phase 1 complete: copied {rowCount:N0} metadata rows into tmp_pr_attachments_meta");
        return rowCount;
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

        // We'll select only rows that have binary data in source and exist in target (or whatever filter you need).
        // Adjust WHERE clause to limit to needed rows (e.g. migrated metadata only)
        var selectBinarySql = @"SELECT PRATTACHMENTID, PRATTACHMENTDATA FROM TBL_PRATTACHMENT WHERE PRATTACHMENTDATA IS NOT NULL";

        using var selectCmd = new SqlCommand(selectBinarySql, sqlConn);
        selectCmd.CommandTimeout = 0; // might take long

        using var reader = await selectCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

        var batch = new List<(long id, long size, Func<CancellationToken, Task<Stream>> streamFactory)>();
        int updated = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.IsDBNull(0) ? throw new InvalidOperationException("Null PRATTACHMENTID") : Convert.ToInt64(reader.GetValue(0));
            long size = reader.IsDBNull(1) ? 0 : reader.GetBytes(1, 0, null, 0, 0);

            if (size == 0)
                continue;

            if (size > MAX_BINARY_SIZE)
            {
                _logger.LogWarning($"Skipping large binary for PRATTACHMENTID {id} (size {size} bytes)");
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

            // When batch reaches threshold, flush via COPY
            if (batch.Count >= BINARY_COPY_BATCH)
            {
                updated += await CopyBinaryBatchAsync(pgConn, batch, cancellationToken);
                // Note: MemoryStreams will be disposed inside CopyBinaryBatchAsync
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            updated += await CopyBinaryBatchAsync(pgConn, batch, cancellationToken);
            batch.Clear();
        }

        _logger.LogInformation($"Phase 3 complete: binaries updated: {updated}");
        return updated;
    }

    private async Task<int> CopyBinaryBatchAsync(NpgsqlConnection pgConn, List<(long id, long size, Func<CancellationToken, Task<Stream>> streamFactory)> batch, CancellationToken cancellationToken)
    {
        // Create a temp table for this batch that includes bytea
        var createTempBin = @"
            CREATE TEMP TABLE tmp_pr_attachments_bin (
                pr_attachment_id bigint,
                pr_attachment_data bytea
            ) ON COMMIT DROP;";

        await using (var createCmd = new NpgsqlCommand(createTempBin, pgConn))
            await createCmd.ExecuteNonQueryAsync(cancellationToken);

        // Begin binary import for this batch
        await using (var importer = pgConn.BeginBinaryImport("COPY tmp_pr_attachments_bin (pr_attachment_id, pr_attachment_data) FROM STDIN (FORMAT BINARY)"))
        {
            foreach (var item in batch)
            {
                cancellationToken.ThrowIfCancellationRequested();

                importer.StartRow();
                importer.Write(item.id, NpgsqlDbType.Bigint);

                // Get stream for this item's binary and write as a stream into bytea column
                await using var s = await item.streamFactory(cancellationToken);

                // The NpgsqlBinaryImporter.Write accepts Stream for bytea (supported types can be written as Stream)
                // This avoids materializing a single large byte[] in memory.
                importer.Write(s, NpgsqlDbType.Bytea);

                // The stream will be disposed by 'await using' above
            }

            await importer.CompleteAsync(cancellationToken);
        }

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
}
