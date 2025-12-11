using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class ARCSubMigration : MigrationService
{
    private MigrationLogger? _migrationLogger;
    private const int BATCH_SIZE = 1000;
    private readonly Microsoft.Extensions.Logging.ILogger<ARCSubMigration> _logger;

    protected override string SelectQuery => @"
        SELECT 
            ARCSubId,
            ARCMainId,
            ItemId,
            ItemCode,
            ItemDescription,
            LongDescription,
            UOM,
            QTY,
            UnitPrice,
            DiscountPer,
            ActualPrice,
            PackingPer,
            TotalAmount,
            CreatedBy,
            CreatedDate,
            UpdatedBy,
            UpdatedDate,
            Status,
            BlockRemark,
            BlockDate,
            BlockRemark,
            BlockDate,
            UsedItemQty,
            UsedItemTotalValue,
            GSTID
        FROM TBL_ARCSub
        ORDER BY ARCSubId";

    protected override string InsertQuery => @"
        INSERT INTO arc_lines (
            arc_lines_id,
            arc_header_id,
            material_id,
            material_code,
            material_description,
            uom_code,
            qty,
            unit_price,
            discount_percentage,
            after_discount_value,
            packing_percentage,
            total_amount,
            status,
            block_remark,
            block_date,
            used_material_qty,
            used_material_value,
            tax_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @arc_lines_id,
            @arc_header_id,
            @material_id,
            @material_code,
            @material_description,
            @uom_code,
            @qty,
            @unit_price,
            @discount_percentage,
            @after_discount_value,
            @packing_percentage,
            @total_amount,
            @status,
            @block_remark,
            @block_date,
            @used_material_qty,
            @used_material_value,
            @tax_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public ARCSubMigration(IConfiguration configuration, Microsoft.Extensions.Logging.ILogger<ARCSubMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // arc_lines_id
            "Direct", // arc_header_id
            "Direct", // material_id
            "Direct", // material_code
            "Direct", // material_description
            "Direct", // uom_code
            "Direct", // qty
            "Direct", // unit_price
            "Direct", // discount_percentage
            "Direct", // after_discount_value
            "Direct", // packing_percentage
            "Direct", // total_amount
            "Direct", // status
            "Direct", // block_remark
            "Direct", // block_date
            "Direct", // used_material_qty
            "Direct", // used_material_value
            "Direct", // tax_id
            "Direct", // created_by (from CreatedBy)
            "Direct", // created_date (from CreatedDate)
            "Direct", // modified_by (from UpdatedBy)
            "Direct", // modified_date (from UpdatedDate)
            "Fixed: false", // is_deleted
            "Fixed: null", // deleted_by
            "Fixed: null"  // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ARCSubId", target = "arc_lines_id" },
            new { source = "ARCMainId", target = "arc_header_id" },
            new { source = "ItemId", target = "material_id" },
            new { source = "ItemCode", target = "material_code" },
            new { source = "ItemDescription", target = "material_description" },
            new { source = "UOM", target = "uom_code" },
            new { source = "QTY", target = "qty" },
            new { source = "UnitPrice", target = "unit_price" },
            new { source = "DiscountPer", target = "discount_percentage" },
            new { source = "ActualPrice", target = "after_discount_value" },
            new { source = "PackingPer", target = "packing_percentage" },
            new { source = "TotalAmount", target = "total_amount" },
            new { source = "Status", target = "status" },
            new { source = "BlockRemark", target = "block_remark" },
            new { source = "BlockDate", target = "block_date" },
            new { source = "UsedItemQty", target = "used_material_qty" },
            new { source = "UsedItemTotalValue", target = "used_material_value" },
            new { source = "GSTID", target = "tax_id" },
            new { source = "CreatedBy", target = "created_by" },
            new { source = "CreatedDate", target = "created_date" },
            new { source = "UpdatedBy", target = "modified_by" },
            new { source = "UpdatedDate", target = "modified_date" },
            new { source = "-", target = "is_deleted" },
            new { source = "-", target = "deleted_by" },
            new { source = "-", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "arc_lines");
        _migrationLogger.LogInfo("Starting migration");

        // Load valid user IDs
        var validUserIds = await LoadValidIdsAsync(pgConn, "users", "user_id");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int totalCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();
        var skippedRecords = new List<(string RecordId, string Reason)>();
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            totalCount++;
            var arcSubId = reader["ARCSubId"]?.ToString() ?? "NULL";
            // Validate created_by and modified_by
            var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? (int?)null : Convert.ToInt32(reader["CreatedBy"]);
            var updatedBy = reader.IsDBNull(reader.GetOrdinal("UpdatedBy")) ? (int?)null : Convert.ToInt32(reader["UpdatedBy"]);
            
            // Set to NULL if user ID is invalid
            object createdByValue = DBNull.Value;
            object modifiedByValue = DBNull.Value;
            
            if (createdBy.HasValue && validUserIds.Contains(createdBy.Value))
            {
                createdByValue = createdBy.Value;
            }
            else if (createdBy.HasValue)
            {
                _logger.LogWarning($"Created_by user_id={createdBy} not found in users table, setting to NULL");
            }
            
            if (updatedBy.HasValue && validUserIds.Contains(updatedBy.Value))
            {
                modifiedByValue = updatedBy.Value;
            }
            else if (updatedBy.HasValue)
            {
                _logger.LogWarning($"Modified_by user_id={updatedBy} not found in users table, setting to NULL");
            }
            
            var record = new Dictionary<string, object>
            {
                ["arc_lines_id"] = reader["ARCSubId"],
                ["arc_header_id"] = reader["ARCMainId"],
                ["material_id"] = reader["ItemId"] ?? (object)DBNull.Value,
                ["material_code"] = reader["ItemCode"] ?? (object)DBNull.Value,
                ["material_description"] = reader["ItemDescription"] ?? (object)DBNull.Value,
                ["uom_code"] = reader["UOM"] ?? (object)DBNull.Value,
                ["qty"] = reader["QTY"] ?? (object)DBNull.Value,
                ["unit_price"] = reader["UnitPrice"] ?? (object)DBNull.Value,
                ["discount_percentage"] = reader["DiscountPer"] ?? (object)DBNull.Value,
                ["after_discount_value"] = reader["ActualPrice"] ?? (object)DBNull.Value,
                ["packing_percentage"] = reader["PackingPer"] ?? (object)DBNull.Value,
                ["total_amount"] = reader["TotalAmount"] ?? (object)DBNull.Value,
                ["status"] = reader["Status"] ?? (object)DBNull.Value,
                ["block_remark"] = reader["BlockRemark"] ?? (object)DBNull.Value,
                ["block_date"] = reader["BlockDate"] ?? (object)DBNull.Value,
                ["used_material_qty"] = reader["UsedItemQty"] ?? (object)DBNull.Value,
                ["used_material_value"] = reader["UsedItemTotalValue"] ?? (object)DBNull.Value,
                ["tax_id"] = reader["GSTID"] ?? (object)DBNull.Value,
                ["created_by"] = createdByValue,
                ["created_date"] = reader["CreatedDate"] ?? (object)DBNull.Value,
                ["modified_by"] = modifiedByValue,
                ["modified_date"] = reader["UpdatedDate"] ?? (object)DBNull.Value,
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
        MigrationStatsExporter.ExportToExcel("arc_sub_migration_stats.xlsx", totalCount, insertedCount, skippedCount, _logger, skippedRecords);
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
            "arc_lines_id", "arc_header_id", "material_id", "material_code", "material_description", "uom_code", "qty", "unit_price", "discount_percentage", "after_discount_value", "packing_percentage", "total_amount", "status", "block_remark", "block_date", "used_material_qty", "used_material_value", "tax_id", "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
        };
        var valueRows = new List<string>();
        var parameters = new List<Npgsql.NpgsqlParameter>();
        int paramIndex = 0;
        foreach (var record in batch)
        {
            var valuePlaceholders = new List<string>();
            foreach (var col in columns)
            {
                var paramName = $"@p{paramIndex}";
                valuePlaceholders.Add(paramName);
                parameters.Add(new Npgsql.NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }
        var sql = $"INSERT INTO arc_lines ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new Npgsql.NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());
        int count = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {count} records into arc_lines.");

        // Insert into arc_lines_history (create a new parameter list to avoid parameter reuse error)
        var historySql = $"INSERT INTO arc_lines_history ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        var historyParameters = new List<Npgsql.NpgsqlParameter>();
        foreach (var p in parameters)
        {
            historyParameters.Add(new Npgsql.NpgsqlParameter(p.ParameterName, p.Value));
        }
        using var historyCmd = new Npgsql.NpgsqlCommand(historySql, pgConn, transaction);
        historyCmd.Parameters.AddRange(historyParameters.ToArray());
        int historyCount = await historyCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {historyCount} records into arc_lines_history.");

        return count;
    }
}
