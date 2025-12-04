using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SupplierPriceBidNonPricingMigration
    {
        private readonly ILogger<SupplierPriceBidNonPricingMigration> _logger;
        private readonly IConfiguration _configuration;

        public SupplierPriceBidNonPricingMigration(IConfiguration configuration, ILogger<SupplierPriceBidNonPricingMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "supplier_price_bid_non_pricing_id", type = "PostgreSQL auto-increment" },
                new { source = "PB_BuyerNonPricingId", target = "user_price_bid_non_pricing_id", type = "int -> integer" },
                new { source = "SUPPLIER_ID", target = "supplier_id", type = "int -> integer" },
                new { source = "EVENT_ID", target = "event_id", type = "int -> integer" },
                new { source = "PB_NonPricingRemark", target = "supplier_non_pricing_remark", type = "nvarchar -> varchar" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SupplierPriceBidNonPricing migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE supplier_price_bid_non_pricing RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset supplier_price_bid_non_pricing table and restarted identity sequence");
                }

                // Build lookup for valid event_ids from PostgreSQL
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_id 
                    FROM event_master 
                    WHERE event_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_id lookup with {validEventIds.Count} entries");

                // Build lookup for valid supplier_ids from PostgreSQL
                var validSupplierIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT supplier_id 
                    FROM supplier_master 
                    WHERE supplier_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validSupplierIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built supplier_id lookup with {validSupplierIds.Count} entries");

                // Build lookup for valid user_price_bid_non_pricing_ids from PostgreSQL
                var validUserPriceBidNonPricingIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT user_price_bid_non_pricing_id 
                    FROM user_price_bid_non_pricing 
                    WHERE user_price_bid_non_pricing_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validUserPriceBidNonPricingIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built user_price_bid_non_pricing_id lookup with {validUserPriceBidNonPricingIds.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PB_SupplerNonPricingId,
                        PB_BuyerNonPricingId,
                        EVENT_ID,
                        PB_NonPricingRemark,
                        SUPPLIER_ID
                    FROM TBL_PB_SUPPLIERNonPricing
                    WHERE PB_SupplerNonPricingId IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PB_SupplerNonPricingId = reader.GetInt32(0),
                            PB_BuyerNonPricingId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            EVENT_ID = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            PB_NonPricingRemark = reader.IsDBNull(3) ? null : reader.GetString(3),
                            SUPPLIER_ID = reader.IsDBNull(4) ? null : reader.GetInt32(4)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_PB_SUPPLIERNonPricing");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate user_price_bid_non_pricing_id exists (FK constraint)
                        if (record.PB_BuyerNonPricingId.HasValue)
                        {
                            if (!validUserPriceBidNonPricingIds.Contains(record.PB_BuyerNonPricingId.Value))
                            {
                                _logger.LogWarning($"Skipping PB_SupplerNonPricingId {record.PB_SupplerNonPricingId}: user_price_bid_non_pricing_id={record.PB_BuyerNonPricingId} not found in user_price_bid_non_pricing");
                                skippedRecords++;
                                continue;
                            }
                        }

                        // Validate event_id exists in event_master (FK constraint)
                        if (record.EVENT_ID.HasValue && !validEventIds.Contains(record.EVENT_ID.Value))
                        {
                            _logger.LogWarning($"Skipping PB_SupplerNonPricingId {record.PB_SupplerNonPricingId}: event_id={record.EVENT_ID} not found in event_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate supplier_id exists in supplier_master (FK constraint)
                        if (record.SUPPLIER_ID.HasValue && !validSupplierIds.Contains(record.SUPPLIER_ID.Value))
                        {
                            _logger.LogWarning($"Skipping PB_SupplerNonPricingId {record.PB_SupplerNonPricingId}: supplier_id={record.SUPPLIER_ID} not found in supplier_master");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            UserPriceBidNonPricingId = record.PB_BuyerNonPricingId,
                            SupplierId = record.SUPPLIER_ID,
                            EventId = record.EVENT_ID,
                            SupplierNonPricingRemark = record.PB_NonPricingRemark
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
                        _logger.LogError($"PB_SupplerNonPricingId {record.PB_SupplerNonPricingId}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
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
            sql.AppendLine("INSERT INTO supplier_price_bid_non_pricing (");
            sql.AppendLine("    user_price_bid_non_pricing_id, supplier_id, event_id,");
            sql.AppendLine("    supplier_non_pricing_remark,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@UserPriceBidNonPricingId{i}, @SupplierId{i}, @EventId{i}, @SupplierNonPricingRemark{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@UserPriceBidNonPricingId{i}", (object?)row.UserPriceBidNonPricingId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", (object?)row.SupplierId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@EventId{i}", (object?)row.EventId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierNonPricingRemark{i}", (object?)row.SupplierNonPricingRemark ?? DBNull.Value);
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
            public int PB_SupplerNonPricingId { get; set; }
            public int? PB_BuyerNonPricingId { get; set; }
            public int? EVENT_ID { get; set; }
            public string? PB_NonPricingRemark { get; set; }
            public int? SUPPLIER_ID { get; set; }
        }

        private class TargetRow
        {
            public int? UserPriceBidNonPricingId { get; set; }
            public int? SupplierId { get; set; }
            public int? EventId { get; set; }
            public string? SupplierNonPricingRemark { get; set; }
        }
    }
}
