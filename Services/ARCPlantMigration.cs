using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

public class ARCPlantMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<ARCPlantMigration> _logger;

    protected override string SelectQuery => @"SELECT * FROM TBL_ARCPlant ORDER BY ARCPlantId";
    protected override string InsertQuery => @"INSERT INTO arc_plant (...) VALUES (...)"; // Not used, but required
    
    protected override List<string> GetLogics()
    {
        return new List<string> {
            "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Fixed: false", "Fixed: null", "Fixed: null"
        };
    }

    public ARCPlantMigration(IConfiguration configuration, ILogger<ARCPlantMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ARCPlantId", target = "arc_plant_id" },
            new { source = "ARCMainId", target = "arc_header_id" },
            new { source = "Plant", target = "plant_id" },
            new { source = "PaymentTerm", target = "payment_term_id" },
            new { source = "IncoTerm", target = "incoterm_id" },
            new { source = "AssignDistributor", target = "supplier_id" },
            new { source = "BlockDate", target = "block_date" },
            new { source = "BlockRemark", target = "block_remark" },
            new { source = "Status", target = "status" },
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
        var validPlantIds = await LoadValidIdsAsync(pgConn, "plant_master", "plant_id");
        var validArcHeaderIds = await LoadValidIdsAsync(pgConn, "arc_header", "arc_header_id");
        var validPaymentTermIds = await LoadValidIdsAsync(pgConn, "payment_term_master", "payment_term_id");
        var validIncotermIds = await LoadValidIdsAsync(pgConn, "incoterm_master", "incoterm_id");
        var validSupplierIds = await LoadValidIdsAsync(pgConn, "supplier_master", "supplier_id");

        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var plantId = reader.IsDBNull(reader.GetOrdinal("Plant")) ? (int?)null : Convert.ToInt32(reader["Plant"]);
            var arcHeaderId = reader.IsDBNull(reader.GetOrdinal("ARCMainId")) ? (int?)null : Convert.ToInt32(reader["ARCMainId"]);
            var paymentTermId = reader.IsDBNull(reader.GetOrdinal("PaymentTerm")) ? (int?)null : Convert.ToInt32(reader["PaymentTerm"]);
            var incotermId = reader.IsDBNull(reader.GetOrdinal("IncoTerm")) ? (int?)null : Convert.ToInt32(reader["IncoTerm"]);
            var supplierId = reader.IsDBNull(reader.GetOrdinal("AssignDistributor")) ? (int?)null : Convert.ToInt32(reader["AssignDistributor"]);

            // Skip record if any foreign key is invalid
            bool skipRecord = false;
            if (plantId.HasValue && !validPlantIds.Contains(plantId.Value))
            {
                _logger.LogWarning($"Skipping record: plant_id={plantId} not found in plant_master");
                skipRecord = true;
            }
            if (arcHeaderId.HasValue && !validArcHeaderIds.Contains(arcHeaderId.Value))
            {
                _logger.LogWarning($"Skipping record: arc_header_id={arcHeaderId} not found in arc_header");
                skipRecord = true;
            }
            if (paymentTermId.HasValue && !validPaymentTermIds.Contains(paymentTermId.Value))
            {
                _logger.LogWarning($"Skipping record: payment_term_id={paymentTermId} not found in payment_term_master");
                skipRecord = true;
            }
            if (incotermId.HasValue && !validIncotermIds.Contains(incotermId.Value))
            {
                _logger.LogWarning($"Skipping record: incoterm_id={incotermId} not found in incoterm_master");
                skipRecord = true;
            }
            if (supplierId.HasValue && !validSupplierIds.Contains(supplierId.Value))
            {
                _logger.LogWarning($"Skipping record: supplier_id={supplierId} not found in supplier_master");
                skipRecord = true;
            }

            if (skipRecord)
            {
                skippedCount++;
                continue;
            }

            var record = new Dictionary<string, object>
            {
                ["arc_header_id"] = arcHeaderId.HasValue ? (object)arcHeaderId.Value : DBNull.Value,
                ["plant_id"] = plantId.HasValue ? (object)plantId.Value : DBNull.Value,
                ["payment_term_id"] = paymentTermId.HasValue ? (object)paymentTermId.Value : DBNull.Value,
                ["incoterm_id"] = incotermId.HasValue ? (object)incotermId.Value : DBNull.Value,
                ["supplier_id"] = supplierId.HasValue ? (object)supplierId.Value : DBNull.Value,
                ["block_date"] = reader["BlockDate"] ?? (object)DBNull.Value,
                ["block_remark"] = reader["BlockRemark"] ?? (object)DBNull.Value,
                ["status"] = reader["Status"] ?? (object)DBNull.Value,
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
            "arc_header_id", "plant_id", "payment_term_id", "incoterm_id", "supplier_id", "block_date", "block_remark", "status", "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
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
        var sql = $"INSERT INTO arc_plant ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)} RETURNING arc_plant_id";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());
        var insertedIds = new List<int>();
        using (var reader = await insertCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                insertedIds.Add(reader.GetInt32(0));
        }
        _logger.LogInformation($"Batch {batchNumber}: Inserted {insertedIds.Count} records into arc_plant.");
        // Insert into arc_plant_history
        var historyColumns = new List<string>(columns) { "arc_plant_id" };
        var historyValueRows = new List<string>();
        var historyParameters = new List<NpgsqlParameter>();
        for (int j = 0; j < batch.Count; j++)
        {
            var rowParams = new List<string>();
            foreach (var col in columns)
                rowParams.Add($"@h_{col}_{j}");
            rowParams.Add($"@h_arc_plant_id_{j}");
            for (int k = 0; k < columns.Count; k++)
                historyParameters.Add(new NpgsqlParameter($"@h_{columns[k]}_{j}", parameters[j * columns.Count + k].Value));
            historyParameters.Add(new NpgsqlParameter($"@h_arc_plant_id_{j}", insertedIds[j]));
            historyValueRows.Add($"({string.Join(", ", rowParams)})");
        }
        var historySql = $"INSERT INTO arc_plant_history ({string.Join(", ", historyColumns)}) VALUES {string.Join(", ", historyValueRows)}";
        using var historyCmd = new NpgsqlCommand(historySql, pgConn, transaction);
        historyCmd.Parameters.AddRange(historyParameters.ToArray());
        await historyCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {batch.Count} records into arc_plant_history.");
        return batch.Count;
    }
}
