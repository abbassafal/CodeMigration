using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class NfaBoqItemsMigration
    {
        private readonly ILogger<NfaBoqItemsMigration> _logger;
    private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public NfaBoqItemsMigration(IConfiguration configuration, ILogger<NfaBoqItemsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

    public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "SubItemID (Auto-increment)", target = "nfa_boq_items_id", type = "int -> integer (NOT NULL, Auto-increment)" },
                new { source = "PRTransId", target = "erp_pr_lines_id", type = "int -> integer (NOT NULL, FK)" },
                new { source = "Lookup: Tbl_AwardEventItem.AWARDEVENTMAINID via AwardItemId", target = "nfa_header_id", type = "Lookup -> integer (NOT NULL, FK)" },
                new { source = "ItemId", target = "pr_boq_id", type = "int -> integer (NOT NULL)" },
                new { source = "AwardItemId", target = "nfa_line_id", type = "int -> integer (NOT NULL)" },
                new { source = "Rate", target = "boq_item_unit_price", type = "decimal -> numeric (NOT NULL)" },
                new { source = "Lookup: tbl_PRBOQItems.IQty via ItemId", target = "boq_qty", type = "Lookup -> numeric (NOT NULL)" },
                new { source = "CreatedDate", target = "created_date", type = "datetime -> timestamp" }
            };
        }

        public async Task<int> MigrateAsync()
        {
        _migrationLogger = new MigrationLogger(_logger, "nfa_boq_items");
        _migrationLogger.LogInfo("Starting migration");

            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecordsCount = 0;
            var skippedRecords = new List<(string RecordId, string Reason)>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting NfaBoqItems migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE nfa_boq_items RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset nfa_boq_items table and restarted identity sequence");
                }

                // Build lookup for valid erp_pr_lines_id from PostgreSQL
                var validErpPrLinesIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT erp_pr_lines_id 
                    FROM erp_pr_lines 
                    WHERE erp_pr_lines_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validErpPrLinesIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built erp_pr_lines_id lookup with {validErpPrLinesIds.Count} entries");

                // Build lookup for valid nfa_line_id from PostgreSQL
                var validNfaLineIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT nfa_line_id 
                    FROM nfa_line 
                    WHERE nfa_line_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validNfaLineIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built nfa_line_id lookup with {validNfaLineIds.Count} entries");

                // Build lookup for boq_qty (IQty) from tbl_PRBOQItems in SQL Server
                var itemIdToBoqQty = new Dictionary<int, decimal>();
                using (var cmd = new SqlCommand(@"
                    SELECT ItemId, IQty 
                    FROM tbl_PRBOQItems 
                    WHERE ItemId IS NOT NULL AND IQty IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var itemId = reader.GetInt32(0);
                        var iQty = reader.GetDecimal(1);
                        if (!itemIdToBoqQty.ContainsKey(itemId))
                        {
                            itemIdToBoqQty[itemId] = iQty;
                        }
                    }
                }
                _logger.LogInformation($"Built ItemId -> boq_qty lookup with {itemIdToBoqQty.Count} entries");

                // Build lookup for AwardItemId -> AWARDEVENTMAINID from SQL Server
                var awardItemIdToNfaHeaderId = new Dictionary<int, int>();
                using (var cmd = new SqlCommand(@"
                    SELECT DISTINCT
                        sub.AwardItemId,
                        main.AWARDEVENTMAINID
                    FROM Tbl_AwardEventItemSub sub
                    INNER JOIN Tbl_AwardEventItem main ON main.AWARDEVENTITEM = sub.AwardItemId
                    WHERE sub.AwardItemId IS NOT NULL 
                      AND main.AWARDEVENTMAINID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var awardItemId = reader.GetInt32(0);
                        var nfaHeaderId = reader.GetInt32(1);
                        if (!awardItemIdToNfaHeaderId.ContainsKey(awardItemId))
                        {
                            awardItemIdToNfaHeaderId[awardItemId] = nfaHeaderId;
                        }
                    }
                }
                _logger.LogInformation($"Built AwardItemId -> nfa_header_id lookup with {awardItemIdToNfaHeaderId.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        SubItemID,
                        PRTransId,
                        ItemId,
                        AwardItemId,
                        Rate,
                        CreatedDate,
                        UpdatedDate
                    FROM Tbl_AwardEventItemSub
                    WHERE SubItemID IS NOT NULL
                    ORDER BY SubItemID", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            SubItemID = reader.GetInt32(0),
                            PRTransId = reader.GetInt32(1),
                            ItemId = reader.GetInt32(2),
                            AwardItemId = reader.GetInt32(3),
                            Rate = reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                            CreatedDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            UpdatedDate = reader.IsDBNull(6) ? null : reader.GetDateTime(6)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from Tbl_AwardEventItemSub");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate erp_pr_lines_id (REQUIRED - NOT NULL, FK)
                        if (!validErpPrLinesIds.Contains(record.PRTransId))
                        {
                            _logger.LogWarning($"Skipping SubItemID {record.SubItemID}: PRTransId={record.PRTransId} not found in erp_pr_lines");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.SubItemID.ToString(), $"PRTransId={record.PRTransId} not found in erp_pr_lines"));
                            continue;
                        }

                        // Lookup nfa_header_id via AwardItemId
                        if (!awardItemIdToNfaHeaderId.TryGetValue(record.AwardItemId, out var nfaHeaderId))
                        {
                            _logger.LogWarning($"Skipping SubItemID {record.SubItemID}: AwardItemId={record.AwardItemId} not found in lookup (Tbl_AwardEventItem)");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.SubItemID.ToString(), $"AwardItemId={record.AwardItemId} not found in lookup (Tbl_AwardEventItem)"));
                            continue;
                        }

                        // Validate nfa_line_id (REQUIRED - NOT NULL, FK)
                        if (!validNfaLineIds.Contains(record.AwardItemId))
                        {
                            _logger.LogWarning($"Skipping SubItemID {record.SubItemID}: AwardItemId={record.AwardItemId} not found in nfa_line");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.SubItemID.ToString(), $"AwardItemId={record.AwardItemId} not found in nfa_line"));
                            continue;
                        }

                        // Lookup boq_qty from tbl_PRBOQItems via ItemId
                        if (!itemIdToBoqQty.TryGetValue(record.ItemId, out var boqQty))
                        {
                            _logger.LogWarning($"Skipping SubItemID {record.SubItemID}: ItemId={record.ItemId} not found in tbl_PRBOQItems");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.SubItemID.ToString(), $"ItemId={record.ItemId} not found in tbl_PRBOQItems"));
                            continue;
                        }

                        // Validate Rate (REQUIRED - NOT NULL)
                        if (!record.Rate.HasValue)
                        {
                            _logger.LogWarning($"Skipping SubItemID {record.SubItemID}: Rate is null");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.SubItemID.ToString(), "Rate is null"));
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            NfaBoqItemsId = record.SubItemID,
                            ErpPrLinesId = record.PRTransId,
                            NfaHeaderId = nfaHeaderId,
                            PrBoqId = record.ItemId,
                            NfaLineId = record.AwardItemId,
                            BoqItemUnitPrice = record.Rate.Value,
                            BoqQty = boqQty,
                            CreatedDate = record.CreatedDate
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Error processing SubItemID {record.SubItemID}: {ex.Message}");
                        skippedRecordsCount++;
                        skippedRecords.Add((record.SubItemID.ToString(), $"Error: {ex.Message}"));
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecordsCount}");

                // Export migration stats and skipped records to Excel
                MigrationStatsExporter.ExportToExcel(
                    "NfaBoqItemsMigration_Stats.xlsx",
                    sourceData.Count,
                    migratedRecords,
                    skippedRecordsCount,
                    _logger,
                    skippedRecords
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            return migratedRecords;
        }

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO nfa_boq_items (");
            sql.AppendLine("    nfa_boq_items_id, erp_pr_lines_id, nfa_header_id, pr_boq_id,");
            sql.AppendLine("    nfa_line_id, boq_item_unit_price, boq_qty,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@NfaBoqItemsId{i}, @ErpPrLinesId{i}, @NfaHeaderId{i}, @PrBoqId{i}, @NfaLineId{i}, @BoqItemUnitPrice{i}, @BoqQty{i}, NULL, @CreatedDate{i}, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@NfaBoqItemsId{i}", row.NfaBoqItemsId);
                cmd.Parameters.AddWithValue($"@ErpPrLinesId{i}", row.ErpPrLinesId);
                cmd.Parameters.AddWithValue($"@NfaHeaderId{i}", row.NfaHeaderId);
                cmd.Parameters.AddWithValue($"@PrBoqId{i}", row.PrBoqId);
                cmd.Parameters.AddWithValue($"@NfaLineId{i}", row.NfaLineId);
                cmd.Parameters.AddWithValue($"@BoqItemUnitPrice{i}", row.BoqItemUnitPrice);
                cmd.Parameters.AddWithValue($"@BoqQty{i}", row.BoqQty);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", row.CreatedDate.HasValue ? (object)row.CreatedDate.Value : DBNull.Value);
            }

            sql.AppendLine(string.Join(",\n", values));
            cmd.CommandText = sql.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                _logger.LogDebug($"Batch inserted {rowsAffected} records");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Batch insert failed: {ex.Message}");
                throw;
            }
        }

        private class SourceRow
        {
            public int SubItemID { get; set; }
            public int PRTransId { get; set; }
            public int ItemId { get; set; }
            public int AwardItemId { get; set; }
            public decimal? Rate { get; set; }
            public DateTime? CreatedDate { get; set; }
            public DateTime? UpdatedDate { get; set; }
        }

        private class TargetRow
        {
            public int NfaBoqItemsId { get; set; }
            public int ErpPrLinesId { get; set; }
            public int NfaHeaderId { get; set; }
            public int PrBoqId { get; set; }
            public int NfaLineId { get; set; }
            public decimal BoqItemUnitPrice { get; set; }
            public decimal BoqQty { get; set; }
            public DateTime? CreatedDate { get; set; }
        }
    }
}
