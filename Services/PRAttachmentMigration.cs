using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class PRAttachmentMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<PRAttachmentMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    PRATTACHMENTID,
    PRID,
    UPLOADPATH,
    FILENAME,
    UPLOADEDBYID,
    PRType,
    PRNo,
    ItemCode,
    PRTRANSID,
    Remarks,
    PRATTACHMENTDATA,
    PR_ATTCHMENT_TYPE,
    0 AS created_by,
    NULL AS created_date,
    0 AS modified_by,
    NULL AS modified_date,
    0 AS is_deleted,
    NULL AS deleted_by,
    NULL AS deleted_date
FROM TBL_PRATTACHMENT
";

    protected override string InsertQuery => @"
INSERT INTO pr_attachments (
    pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_data, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @pr_attachment_id, @erp_pr_lines_id, @upload_path, @file_name, @remarks, @is_header_doc, @pr_attachment_data, @pr_attachment_extensions, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)";

    public PRAttachmentMigration(IConfiguration configuration, ILogger<PRAttachmentMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // pr_attachment_id
        "Direct", // erp_pr_lines_id
        "Direct", // upload_path
        "Direct", // file_name
        "Direct", // remarks
        "Direct", // is_header_doc
        "Direct", // pr_attachment_data
        "Direct", // pr_attachment_extensions
        "Direct", // created_by
        "Direct", // created_date
        "Direct", // modified_by
        "Direct", // modified_date
        "Direct", // is_deleted
        "Direct", // deleted_by
        "Direct"  // deleted_date
    };

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        int insertedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var record = new Dictionary<string, object>
            {
                ["pr_attachment_id"] = reader["PRATTACHMENTID"] ?? DBNull.Value,
                ["erp_pr_lines_id"] = reader["PRID"] ?? DBNull.Value,
                ["upload_path"] = reader["UPLOADPATH"] ?? DBNull.Value,
                ["file_name"] = reader["FILENAME"] ?? DBNull.Value,
                ["remarks"] = reader["Remarks"] ?? DBNull.Value,
                ["is_header_doc"] = true,
                ["pr_attachment_data"] = reader["PRATTACHMENTDATA"] ?? DBNull.Value,
                ["pr_attachment_extensions"] = reader["PR_ATTCHMENT_TYPE"] ?? DBNull.Value,
                ["created_by"] = reader["created_by"] ?? DBNull.Value,
                ["created_date"] = reader["created_date"] ?? DBNull.Value,
                ["modified_by"] = reader["modified_by"] ?? DBNull.Value,
                ["modified_date"] = reader["modified_date"] ?? DBNull.Value,
                ["is_deleted"] = Convert.ToInt32(reader["is_deleted"]) == 1,
                ["deleted_by"] = reader["deleted_by"] ?? DBNull.Value,
                ["deleted_date"] = reader["deleted_date"] ?? DBNull.Value
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
        }

        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}");
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        var columns = new List<string> {
            "pr_attachment_id", "erp_pr_lines_id", "upload_path", "file_name", "remarks", "is_header_doc", "pr_attachment_data", "pr_attachment_extensions", "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in batch)
        {
            var valuePlaceholders = new List<string>();
            foreach (var col in columns)
            {
                var paramName = $"@p{paramIndex}";
                valuePlaceholders.Add(paramName);
                parameters.Add(new NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }

        var sql = $"INSERT INTO pr_attachments ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {result} records into pr_attachments.");
        return result;
    }
}
