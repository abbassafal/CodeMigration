using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class PRAttachmentMigration : MigrationService
{
    private const int BATCH_SIZE = 500; // Increased for faster migration
    private readonly ILogger<PRAttachmentMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    pa.PRATTACHMENTID,
    pa.PRID,
    pa.UPLOADPATH,
    pa.FILENAME,
    pa.UPLOADEDBYID,
    pa.PRType,
    pa.PRNo,
    pa.ItemCode,
    pt.PRTRANSID,
    pa.Remarks,
    pa.PRATTACHMENTDATA,
    pa.PR_ATTCHMNT_TYPE,
    0 AS created_by,
    NULL AS created_date,
    0 AS modified_by,
    NULL AS modified_date,
    0 AS is_deleted,
    NULL AS deleted_by,
    NULL AS deleted_date
FROM TBL_PRATTACHMENT pa
LEFT JOIN TBL_PRTRANSACTION pt ON pt.PRID = pa.PRID
";

    protected override string InsertQuery => @"
INSERT INTO pr_attachments (
    pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_data, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @pr_attachment_id, @erp_pr_lines_id, @upload_path, @file_name, @remarks, @is_header_doc, @pr_attachment_data, @pr_attachment_extensions, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)
ON CONFLICT (pr_attachment_id) DO UPDATE SET
    erp_pr_lines_id = EXCLUDED.erp_pr_lines_id,
    upload_path = EXCLUDED.upload_path,
    file_name = EXCLUDED.file_name,
    remarks = EXCLUDED.remarks,
    is_header_doc = EXCLUDED.is_header_doc,
    pr_attachment_data = EXCLUDED.pr_attachment_data,
    pr_attachment_extensions = EXCLUDED.pr_attachment_extensions,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

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

    private async Task<HashSet<int>> LoadValidErpPrLinesIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT erp_pr_lines_id FROM erp_pr_lines";
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        return validIds;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // Load valid ERP PR Lines IDs
        var validErpPrLinesIds = await LoadValidErpPrLinesIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validErpPrLinesIds.Count} valid ERP PR Lines IDs from erp_pr_lines.");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300; // Increase timeout for binary data
        
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            // Read columns by name (SequentialAccess removed for simplicity with binary data)
            var prAttachmentId = reader["PRATTACHMENTID"] ?? DBNull.Value;
            var prTransId = reader["PRTRANSID"] ?? DBNull.Value;
            
            // Validate ERP PR Lines ID (using PRTRANSID which maps to erp_pr_lines_id)
            if (prTransId != DBNull.Value)
            {
                int erpPrLinesId = Convert.ToInt32(prTransId);
                
                // Skip if ERP PR Lines ID not present in erp_pr_lines
                if (!validErpPrLinesIds.Contains(erpPrLinesId))
                {
                    _logger.LogWarning($"Skipping PRATTACHMENTID {prAttachmentId}: ERP PR Lines ID {erpPrLinesId} not found in erp_pr_lines.");
                    skippedCount++;
                    continue;
                }
            }
            else
            {
                _logger.LogWarning($"Skipping PRATTACHMENTID {prAttachmentId}: ERP PR Lines ID (PRTRANSID) is NULL.");
                skippedCount++;
                continue;
            }
            
            var uploadPath = reader["UPLOADPATH"] ?? DBNull.Value;
            var fileName = reader["FILENAME"] ?? DBNull.Value;
            var remarks = reader["Remarks"] ?? DBNull.Value;
            
            // Read binary data
            byte[]? binaryData = null;
            int prAttachmentDataOrdinal = reader.GetOrdinal("PRATTACHMENTDATA");
            if (!reader.IsDBNull(prAttachmentDataOrdinal))
            {
                long dataLength = reader.GetBytes(prAttachmentDataOrdinal, 0, null, 0, 0);
                if (dataLength > 0)
                {
                    binaryData = new byte[dataLength];
                    reader.GetBytes(prAttachmentDataOrdinal, 0, binaryData, 0, (int)dataLength);
                }
            }
            
            var prAttchmntType = reader["PR_ATTCHMNT_TYPE"] ?? DBNull.Value;
            var createdBy = reader["created_by"] ?? DBNull.Value;
            var createdDate = reader["created_date"] ?? DBNull.Value;
            var modifiedBy = reader["modified_by"] ?? DBNull.Value;
            var modifiedDate = reader["modified_date"] ?? DBNull.Value;
            var isDeleted = Convert.ToInt32(reader["is_deleted"]) == 1;
            var deletedBy = reader["deleted_by"] ?? DBNull.Value;
            var deletedDate = reader["deleted_date"] ?? DBNull.Value;

            var record = new Dictionary<string, object>
            {
                ["pr_attachment_id"] = prAttachmentId,
                ["erp_pr_lines_id"] = prTransId,
                ["upload_path"] = uploadPath,
                ["file_name"] = fileName,
                ["remarks"] = remarks,
                ["is_header_doc"] = true,
                ["pr_attachment_data"] = binaryData != null ? (object)binaryData : DBNull.Value,
                ["pr_attachment_extensions"] = prAttchmntType,
                ["created_by"] = createdBy,
                ["created_date"] = createdDate,
                ["modified_by"] = modifiedBy,
                ["modified_date"] = modifiedDate,
                ["is_deleted"] = isDeleted,
                ["deleted_by"] = deletedBy,
                ["deleted_date"] = deletedDate
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
                
                // Optimize binary data parameter
                if (col == "pr_attachment_data" && record[col] is byte[] binaryData)
                {
                    parameters.Add(new NpgsqlParameter(paramName, NpgsqlTypes.NpgsqlDbType.Bytea) { Value = binaryData });
                }
                else
                {
                    parameters.Add(new NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                }
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }

        var updateColumns = columns.Where(c => c != "pr_attachment_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));
        
        var sql = $@"INSERT INTO pr_attachments ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (pr_attachment_id) DO UPDATE SET {updateSet}";
        
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300; // Increase timeout for binary inserts
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {result} records into pr_attachments.");
        return result;
    }
}
