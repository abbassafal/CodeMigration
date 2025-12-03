using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class EventBoqItemsMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<EventBoqItemsMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    PBSubId,
    PBID,
    PRBOQID,
    ItemId,
    ItemCode,
    ItemName,
    UOM,
    Rate,
    Quantity
FROM TBL_PB_BUYER_SUB
";

    protected override string InsertQuery => @"
INSERT INTO event_boq_items (
    event_boq_items_id, event_item_id, event_id, pr_boq_id, material_code, material_name, material_description, uom, boq_qty
) VALUES (
    @event_boq_items_id, @event_item_id, @event_id, @pr_boq_id, @material_code, @material_name, @material_description, @uom, @boq_qty
)
ON CONFLICT (event_boq_items_id) DO UPDATE SET
    event_item_id = EXCLUDED.event_item_id,
    event_id = EXCLUDED.event_id,
    pr_boq_id = EXCLUDED.pr_boq_id,
    material_code = EXCLUDED.material_code,
    material_name = EXCLUDED.material_name,
    material_description = EXCLUDED.material_description,
    uom = EXCLUDED.uom,
    boq_qty = EXCLUDED.boq_qty";

    public EventBoqItemsMigration(IConfiguration configuration, ILogger<EventBoqItemsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // event_boq_items_id
        "Direct", // event_item_id
        "Direct", // event_id
        "Direct", // pr_boq_id
        "Direct", // material_code
        "Direct", // material_name
        "Direct", // material_description
        "Direct", // uom
        "Direct"  // boq_qty
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PBSubId", logic = "PBSubId -> event_boq_items_id (Direct)", target = "event_boq_items_id" },
            new { source = "PBID", logic = "PBID -> event_item_id (Direct)", target = "event_item_id" },
            new { source = "PBID", logic = "PBID -> event_id (Direct, from UserPriceBid/EventItemsID)", target = "event_id" },
            new { source = "PRBOQID", logic = "PRBOQID -> pr_boq_id (Direct)", target = "pr_boq_id" },
            new { source = "ItemCode", logic = "ItemCode -> material_code (Direct)", target = "material_code" },
            new { source = "ItemName", logic = "ItemName -> material_name (Direct)", target = "material_name" },
            new { source = "ItemName", logic = "ItemName -> material_description (Direct)", target = "material_description" },
            new { source = "UOM", logic = "UOM -> uom (Direct)", target = "uom" },
            new { source = "Quantity", logic = "Quantity -> boq_qty (Direct)", target = "boq_qty" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting EventBoqItems migration...");
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var pbSubId = reader["PBSubId"] ?? DBNull.Value;
            var pbId = reader["PBID"] ?? DBNull.Value;
            var prBoqId = reader["PRBOQID"] ?? DBNull.Value;
            var itemId = reader["ItemId"] ?? DBNull.Value;
            var itemCode = reader["ItemCode"] ?? DBNull.Value;
            var itemName = reader["ItemName"] ?? DBNull.Value;
            var uom = reader["UOM"] ?? DBNull.Value;
            var rate = reader["Rate"] ?? DBNull.Value;
            var quantity = reader["Quantity"] ?? DBNull.Value;

            // Validate required keys
            if (pbSubId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping row: PBSubId is NULL.");
                skippedCount++;
                continue;
            }

            var record = new Dictionary<string, object>
            {
                ["event_boq_items_id"] = pbSubId,
                ["event_item_id"] = pbId,
                ["event_id"] = pbId, // As per mapping, PBID is used for both event_item_id and event_id
                ["pr_boq_id"] = prBoqId,
                ["material_code"] = itemCode,
                ["material_name"] = itemName,
                ["material_description"] = itemName,
                ["uom"] = uom,
                ["boq_qty"] = quantity
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Inserting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Inserting final batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
        }

        _logger.LogInformation($"EventBoqItems migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        // Deduplicate by event_boq_items_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["event_boq_items_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate event_boq_items_id records.");
        }

        var columns = new List<string> {
            "event_boq_items_id", "event_item_id", "event_id", "pr_boq_id", "material_code", "material_name", "material_description", "uom", "boq_qty"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in deduplicatedBatch)
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

        var updateColumns = columns.Where(c => c != "event_boq_items_id").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO event_boq_items ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (event_boq_items_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
