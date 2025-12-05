using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

public class ARCApprovalAuthorityMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<ARCApprovalAuthorityMigration> _logger;

    protected override string SelectQuery => @"SELECT * FROM TBL_ARCApprovalAuthority ORDER BY ARCApprovalAuthorityId";
    protected override string InsertQuery => @"INSERT INTO arc_workflow (...) VALUES (...)"; // Not used, but required
    
    protected override List<string> GetLogics()
    {
        return new List<string> {
            "Direct", "Direct", "Direct", "Direct", "CreateDate->assign_date", "Direct", "Direct", "Direct", "Direct", "Direct", "Fixed: false", "Fixed: null", "Fixed: null"
        };
    }

    public ARCApprovalAuthorityMigration(IConfiguration configuration, ILogger<ARCApprovalAuthorityMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ARCApprovalAuthorityId", target = "arc_workflow_id" },
            new { source = "ARCId", target = "arc_header_id" },
            new { source = "ApprovedBy", target = "approved_by" },
            new { source = "AlternateApprovedBy", target = "assign_date" },
            new { source = "CreateDate", target = "assign_date" },
            new { source = "Level", target = "level" },
            new { source = "CreatedBy", target = "created_by" },
            new { source = "CreateDate", target = "created_date" },
            new { source = "CreatedBy", target = "modified_by" },
            new { source = "CreateDate", target = "modified_date" },
            new { source = "-", target = "is_deleted" },
            new { source = "-", target = "deleted_by" },
            new { source = "-", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // Load valid foreign key IDs
        var validArcHeaderIds = await LoadValidIdsAsync(pgConn, "arc_header", "arc_header_id");

        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var arcHeaderId = reader.IsDBNull(reader.GetOrdinal("ARCId")) ? (int?)null : Convert.ToInt32(reader["ARCId"]);

            // Skip record if foreign key is null or invalid
            if (!arcHeaderId.HasValue)
            {
                _logger.LogWarning($"Skipping record: arc_header_id is NULL");
                skippedCount++;
                continue;
            }
            
            if (!validArcHeaderIds.Contains(arcHeaderId.Value))
            {
                _logger.LogWarning($"Skipping record: arc_header_id={arcHeaderId} not found in arc_header");
                skippedCount++;
                continue;
            }

            var record = new Dictionary<string, object>
            {
                ["arc_header_id"] = arcHeaderId.Value,
                ["approved_by"] = reader.IsDBNull(reader.GetOrdinal("ApprovedBy")) ? (object)DBNull.Value : Convert.ToInt32(reader["ApprovedBy"]),
                ["assign_date"] = reader["CreateDate"] ?? (object)DBNull.Value,
                ["level"] = reader.IsDBNull(reader.GetOrdinal("Level")) ? (object)DBNull.Value : Convert.ToInt32(reader["Level"]),
                ["created_by"] = reader["CreatedBy"] ?? (object)DBNull.Value,
                ["created_date"] = reader["CreateDate"] ?? (object)DBNull.Value,
                ["modified_by"] = reader["CreatedBy"] ?? (object)DBNull.Value,
                ["modified_date"] = reader["CreateDate"] ?? (object)DBNull.Value,
                ["is_deleted"] = false,
                ["deleted_by"] = DBNull.Value,
                ["deleted_date"] = DBNull.Value
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
        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<HashSet<int>> LoadValidIdsAsync(NpgsqlConnection pgConn, string tableName, string columnName)
    {
        var validIds = new HashSet<int>();
        var sql = $"SELECT {columnName} FROM {tableName}";
        using var cmd = new NpgsqlCommand(sql, pgConn);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
                validIds.Add(reader.GetInt32(0));
        }
        _logger.LogInformation($"Loaded {validIds.Count} valid IDs from {tableName}.{columnName}");
        return validIds;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction = null, int batchNumber = 0)
    {
        if (batch.Count == 0) return 0;
        var columns = new List<string> {
            "arc_header_id", "approved_by", "assign_date", "level", "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
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
        var sql = $"INSERT INTO arc_workflow ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)} RETURNING arc_workflow_id";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());
        var insertedIds = new List<int>();
        using (var reader = await insertCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                insertedIds.Add(reader.GetInt32(0));
        }
        _logger.LogInformation($"Batch {batchNumber}: Inserted {insertedIds.Count} records into arc_workflow.");
        
        // Insert into arc_workflow_history
        var historyColumns = new List<string>(columns) { "arc_workflow_id" };
        var historyValueRows = new List<string>();
        var historyParameters = new List<NpgsqlParameter>();
        for (int j = 0; j < batch.Count; j++)
        {
            var rowParams = new List<string>();
            foreach (var col in columns)
                rowParams.Add($"@h_{col}_{j}");
            rowParams.Add($"@h_arc_workflow_id_{j}");
            for (int k = 0; k < columns.Count; k++)
                historyParameters.Add(new NpgsqlParameter($"@h_{columns[k]}_{j}", parameters[j * columns.Count + k].Value));
            historyParameters.Add(new NpgsqlParameter($"@h_arc_workflow_id_{j}", insertedIds[j]));
            historyValueRows.Add($"({string.Join(", ", rowParams)})");
        }
        var historySql = $"INSERT INTO arc_workflow_history ({string.Join(", ", historyColumns)}) VALUES {string.Join(", ", historyValueRows)}";
        using var historyCmd = new NpgsqlCommand(historySql, pgConn, transaction);
        historyCmd.Parameters.AddRange(historyParameters.ToArray());
        await historyCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {batch.Count} records into arc_workflow_history.");
        return batch.Count;
    }
}
