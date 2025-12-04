using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class EventSupplierPriceBidMigration
    {
        private readonly ILogger<EventSupplierPriceBidMigration> _logger;
        private readonly IConfiguration _configuration;

        public EventSupplierPriceBidMigration(IConfiguration configuration, ILogger<EventSupplierPriceBidMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-increment", target = "event_supplier_price_bid_id", type = "SERIAL -> integer" },
                new { source = "SUPPLIER_ID", target = "supplier_id", type = "int -> integer" },
                new { source = "EVENTID", target = "event_id", type = "int -> integer (FK to event_master)" },
                new { source = "Calculated: SUM((UNIT_PRICE - (UNIT_PRICE * DiscountPer / 100)) * QTY)", target = "item_total", type = "Calculated -> numeric" },
                new { source = "TotalGSTAmount", target = "total_tax_amount", type = "decimal -> numeric" },
                new { source = "Calculated: item_total + lot_total + total_tax_amount", target = "total_after_tax", type = "Calculated -> numeric" },
                new { source = "SubTotal", target = "lot_total", type = "decimal -> numeric" },
                new { source = "Calculated: item_total + lot_total", target = "total_before_tax", type = "Calculated -> numeric" },
                new { source = "TBL_PB_SUPPLIER_ATTACHMENT.QUOTATIONVALIDITYDATE", target = "validity_days", type = "datetime -> timestamp with time zone" }
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
            var errors = new List<string>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting EventSupplierPriceBid migration...");

                // Get valid event_ids from PostgreSQL
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_id FROM event_master", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {validEventIds.Count} valid event_ids");

                // Get valid supplier_ids from PostgreSQL
                var validSupplierIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT supplier_id FROM supplier_master", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validSupplierIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {validSupplierIds.Count} valid supplier_ids");

                // Get validity dates from TBL_PB_SUPPLIER_ATTACHMENT
                var validityDatesMap = new Dictionary<(int supplierId, int eventId), DateTime?>();
                using (var cmd = new SqlCommand(@"
                    SELECT SUPPLIERID, EVENTID, QUOTATIONVALIDITYDATE
                    FROM TBL_PB_SUPPLIER_ATTACHMENT", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        try
                        {
                            // Handle potential decimal to int conversion
                            var supplierId = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetDecimal(0));
                            var eventId = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetDecimal(1));
                            var validityDate = reader.IsDBNull(2) ? null : (DateTime?)reader.GetDateTime(2);
                            
                            if (supplierId > 0 && eventId > 0)
                            {
                                validityDatesMap[(supplierId, eventId)] = validityDate;
                                if (validityDate.HasValue)
                                {
                                    _logger.LogDebug($"Validity date for Supplier {supplierId}, Event {eventId}: {validityDate.Value:yyyy-MM-dd}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Error reading validity date: {ex.Message}");
                        }
                    }
                }
                _logger.LogInformation($"Found {validityDatesMap.Count} validity date mappings");

                // Fetch aggregated data from SQL Server
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        SUPPLIER_ID,
                        EVENTID,
                        SUM((UNIT_PRICE - (UNIT_PRICE * ISNULL(DiscountPer, 0) / 100)) * ISNULL(QTY, 0)) AS ItemTotal,
                        MAX(ISNULL(TotalGSTAmount, 0)) AS TotalGSTAmount,
                        MAX(ISNULL(SubTotal, 0)) AS SubTotal
                    FROM TBL_PB_SUPPLIER
                    GROUP BY SUPPLIER_ID, EVENTID", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            SUPPLIER_ID = reader.IsDBNull(0) ? null : reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            ItemTotal = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                            TotalGSTAmount = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                            SubTotal = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} aggregated records from source table");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate required fields
                        if (!record.SUPPLIER_ID.HasValue || !record.EVENTID.HasValue)
                        {
                            _logger.LogDebug($"Record skipped: SUPPLIER_ID or EVENTID is NULL");
                            skippedRecords++;
                            continue;
                        }

                        // Validate event_id exists (FK constraint)
                        if (!validEventIds.Contains(record.EVENTID.Value))
                        {
                            _logger.LogDebug($"SUPPLIER_ID {record.SUPPLIER_ID}, EVENTID {record.EVENTID}: event_id not found in event_master (FK constraint violation)");
                            skippedRecords++;
                            continue;
                        }

                        // Validate supplier_id exists (FK constraint)
                        if (!validSupplierIds.Contains(record.SUPPLIER_ID.Value))
                        {
                            _logger.LogDebug($"SUPPLIER_ID {record.SUPPLIER_ID}, EVENTID {record.EVENTID}: supplier_id not found in supplier_master (FK constraint violation)");
                            skippedRecords++;
                            continue;
                        }

                        // Calculate totals
                        var itemTotal = record.ItemTotal;
                        var lotTotal = record.SubTotal;
                        var totalTaxAmount = record.TotalGSTAmount;
                        var totalBeforeTax = itemTotal + lotTotal;
                        var totalAfterTax = totalBeforeTax + totalTaxAmount;

                        // Get validity date (store as timestamp, not days)
                        DateTime? validityDays = null;
                        var key = (record.SUPPLIER_ID.Value, record.EVENTID.Value);
                        if (validityDatesMap.TryGetValue(key, out var validityDate))
                        {
                            validityDays = validityDate;
                            if (validityDate.HasValue)
                            {
                                _logger.LogDebug($"SUPPLIER_ID {record.SUPPLIER_ID}, EVENTID {record.EVENTID}: Found validity date {validityDate.Value:yyyy-MM-dd}");
                            }
                            else
                            {
                                _logger.LogDebug($"SUPPLIER_ID {record.SUPPLIER_ID}, EVENTID {record.EVENTID}: Validity date is NULL");
                            }
                        }
                        else
                        {
                            _logger.LogDebug($"SUPPLIER_ID {record.SUPPLIER_ID}, EVENTID {record.EVENTID}: No validity date mapping found");
                        }

                        var targetRow = new TargetRow
                        {
                            SupplierId = record.SUPPLIER_ID.Value,
                            EventId = record.EVENTID.Value,
                            ItemTotal = itemTotal,
                            TotalTaxAmount = totalTaxAmount,
                            TotalAfterTax = totalAfterTax,
                            LotTotal = lotTotal,
                            TotalBeforeTax = totalBeforeTax,
                            ValidityDays = validityDays
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"SUPPLIER_ID {record.SUPPLIER_ID}, EVENTID {record.EVENTID}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
                
                if (errors.Any())
                {
                    _logger.LogWarning($"Encountered {errors.Count} errors during migration");
                }
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
            sql.AppendLine("INSERT INTO event_supplier_price_bid (");
            sql.AppendLine("    supplier_id, event_id, item_total, total_tax_amount,");
            sql.AppendLine("    total_after_tax, lot_total, total_before_tax, validity_days,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@SupplierId{i}, @EventId{i}, @ItemTotal{i}, @TotalTaxAmount{i}, @TotalAfterTax{i}, @LotTotal{i}, @TotalBeforeTax{i}, @ValidityDays{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@SupplierId{i}", row.SupplierId);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@ItemTotal{i}", row.ItemTotal);
                cmd.Parameters.AddWithValue($"@TotalTaxAmount{i}", row.TotalTaxAmount);
                cmd.Parameters.AddWithValue($"@TotalAfterTax{i}", row.TotalAfterTax);
                cmd.Parameters.AddWithValue($"@LotTotal{i}", row.LotTotal);
                cmd.Parameters.AddWithValue($"@TotalBeforeTax{i}", row.TotalBeforeTax);
                cmd.Parameters.AddWithValue($"@ValidityDays{i}", (object?)row.ValidityDays ?? DBNull.Value);
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
            public int? SUPPLIER_ID { get; set; }
            public int? EVENTID { get; set; }
            public decimal ItemTotal { get; set; }
            public decimal TotalGSTAmount { get; set; }
            public decimal SubTotal { get; set; }
        }

        private class TargetRow
        {
            public int SupplierId { get; set; }
            public int EventId { get; set; }
            public decimal ItemTotal { get; set; }
            public decimal TotalTaxAmount { get; set; }
            public decimal TotalAfterTax { get; set; }
            public decimal LotTotal { get; set; }
            public decimal TotalBeforeTax { get; set; }
            public DateTime? ValidityDays { get; set; }
        }
    }
}
