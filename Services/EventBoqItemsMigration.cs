using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services; // Ensure this is present for MigrationLogger

public class EventBoqItemsMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<EventBoqItemsMigration> _logger;
    private MigrationLogger? _migrationLogger;

    public EventBoqItemsMigration(IConfiguration configuration, ILogger<EventBoqItemsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override string SelectQuery => @"
SELECT
    sub.PBSubId,
    sub.PBID,
    sub.PRBOQID,
    sub.ItemId,
    sub.ItemCode,
    sub.ItemName,
    sub.UOM,
    sub.Rate,
    sub.Quantity,
    buyer.EVENTID
FROM TBL_PB_BUYER_SUB sub
INNER JOIN TBL_PB_BUYER buyer ON buyer.PBID = sub.PBID
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
            new { source = "EVENTID", logic = "EVENTID -> event_id (JOIN with TBL_PB_BUYER on PBID)", target = "event_id" },
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
        _migrationLogger = new MigrationLogger(_logger, "event_boq_items");
        _migrationLogger.LogInfo("Starting EventBoqItems migration");
        int result = await base.MigrateAsync(useTransaction: true);
        _migrationLogger.LogInfo($"Completed: {_migrationLogger.InsertedCount} inserted, {_migrationLogger.SkippedCount} skipped");
        return result;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger?.LogInfo("Loading valid event and event_item IDs...");
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        var validEventIds = await LoadValidEventIdsAsync(pgConn, transaction);
        var validEventItemIds = await LoadValidEventItemIdsAsync(pgConn, transaction);
        _migrationLogger?.LogInfo($"Loaded {validEventIds.Count} valid event IDs and {validEventItemIds.Count} valid event_item IDs.");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        int processedCount = 0;
        while (await reader.ReadAsync())
        {
            processedCount++;
            var pbSubId = reader["PBSubId"] ?? DBNull.Value;
            var pbId = reader["PBID"] ?? DBNull.Value;
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var prBoqId = reader["PRBOQID"] ?? DBNull.Value;
            var itemId = reader["ItemId"] ?? DBNull.Value;
            var itemCode = reader["ItemCode"] ?? DBNull.Value;
            var itemName = reader["ItemName"] ?? DBNull.Value;
            var uom = reader["UOM"] ?? DBNull.Value;
            var rate = reader["Rate"] ?? DBNull.Value;
            var quantity = reader["Quantity"] ?? DBNull.Value;

            string recordId = $"PBSubId={pbSubId}";

            // Validate required keys
            if (pbSubId == DBNull.Value)
            {
                _migrationLogger?.LogSkipped(
                    "PBSubId is NULL",
                    recordId,
                    new Dictionary<string, object> { { "PBSubId", pbSubId } }
                );
                skippedCount++;
                continue;
            }

            // Validate event_id (EVENTID) exists in event_master
            if (eventId != DBNull.Value)
            {
                int eventIdValue = Convert.ToInt32(eventId);
                if (!validEventIds.Contains(eventIdValue))
                {
                    _migrationLogger?.LogSkipped(
                        $"event_id (EVENTID) {eventIdValue} not found in event_master",
                        recordId,
                        new Dictionary<string, object> { { "EVENTID", eventIdValue } }
                    );
                    skippedCount++;
                    continue;
                }
            }
            else
            {
                _migrationLogger?.LogSkipped(
                    "EVENTID is NULL",
                    recordId,
                    new Dictionary<string, object> { { "EVENTID", eventId } }
                );
                skippedCount++;
                continue;
            }

            // Validate event_item_id (PBID) exists in event_items table
            if (pbId != DBNull.Value)
            {
                int eventItemIdValue = Convert.ToInt32(pbId);
                if (!validEventItemIds.Contains(eventItemIdValue))
                {
                    _migrationLogger?.LogSkipped(
                        $"event_item_id (PBID) {eventItemIdValue} not found in event_items",
                        recordId,
                        new Dictionary<string, object> { { "PBID", eventItemIdValue } }
                    );
                    skippedCount++;
                    continue;
                }
            }
            else
            {
                _migrationLogger?.LogSkipped(
                    "PBID is NULL",
                    recordId,
                    new Dictionary<string, object> { { "PBID", pbId } }
                );
                skippedCount++;
                continue;
            }

            var record = new Dictionary<string, object>
            {
                ["event_boq_items_id"] = pbSubId,
                ["event_item_id"] = pbId,
                ["event_id"] = eventId,
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
                _migrationLogger?.LogInfo($"Inserting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                batch.Clear();
            }

            if (processedCount % 1000 == 0)
            {
                _migrationLogger?.LogInfo($"Processed {processedCount} records");
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _migrationLogger?.LogInfo($"Inserting final batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
        }

        _migrationLogger?.LogInfo($"EventBoqItems migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");

        // Export migration stats to Excel
        var skippedLogEntries = _migrationLogger?.GetSkippedRecords() ?? new List<MigrationLogEntry>();
        var skippedRecords = skippedLogEntries.Select(e => (e.RecordIdentifier, e.Message)).ToList();
        var excelPath = Path.Combine("migration_outputs", $"EventBoqItemsMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
        MigrationStatsExporter.ExportToExcel(
            excelPath,
            insertedCount + skippedCount,
            insertedCount,
            skippedCount,
            _logger,
            skippedRecords
        );
        _logger?.LogInformation($"Migration stats exported to {excelPath}");

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
            _migrationLogger?.LogSkipped(
                $"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate event_boq_items_id records.",
                $"Batch={batchNumber}",
                new Dictionary<string, object> { { "DuplicatesRemoved", batch.Count - deduplicatedBatch.Count } }
            );
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

        foreach (var record in deduplicatedBatch)
        {
            string recordId = $"PBSubId={record["event_boq_items_id"]}";
            _migrationLogger?.LogInserted(recordId);
        }

        _migrationLogger?.LogInfo($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }

    private async Task<HashSet<int>> LoadValidEventIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT event_id FROM event_master";
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        return validIds;
    }

    private async Task<HashSet<int>> LoadValidEventItemIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT event_item_id FROM event_items";
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        return validIds;
    }
}
