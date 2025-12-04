using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SupplierPriceBidLotChargesMigration
    {
        private readonly ILogger<SupplierPriceBidLotChargesMigration> _logger;
        private readonly IConfiguration _configuration;

        public SupplierPriceBidLotChargesMigration(IConfiguration configuration, ILogger<SupplierPriceBidLotChargesMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "supplier_price_bid_lot_charges_id", type = "PostgreSQL auto-increment" },
                new { source = "EVENT_ID", target = "event_id", type = "int -> integer" },
                new { source = "PB_BuyerChargesId", target = "user_price_bid_lot_charges_id", type = "int -> integer" },
                new { source = "Lookup: TBL_PB_BUYEROTHERCHARGES.PB_ChargesId WHERE PB_BuyerChargesId", target = "price_bid_charges_id", type = "Lookup -> integer" },
                new { source = "SUPPLIER_ID", target = "supplier_id", type = "int -> integer" },
                new { source = "Percentage", target = "percentage", type = "numeric -> numeric" },
                new { source = "Amount", target = "basic_lot_charges_amount", type = "numeric -> numeric" },
                new { source = "Lookup: tax_master by GSTPer", target = "tax_master_id", type = "Lookup -> integer" },
                new { source = "GSTPer", target = "tax_percentage", type = "decimal -> numeric" },
                new { source = "GSTAmount", target = "tax_amount", type = "decimal -> numeric" },
                new { source = "Calculated: Amount + GSTAmount", target = "total_lot_charges_amount", type = "Calculated -> numeric" },
                new { source = "LotChargeFileName", target = "supplier_lot_charges_file_name", type = "nvarchar -> varchar" },
                new { source = "Default: ''", target = "supplier_lot_charges_file_path", type = "NOT NULL, default ''" },
                new { source = "Default: ''", target = "reason", type = "NOT NULL, default ''" }
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

                _logger.LogInformation("Starting SupplierPriceBidLotCharges migration...");

                // Restart the identity sequence for supplier_price_bid_lot_charges_id
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE supplier_price_bid_lot_charges RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset supplier_price_bid_lot_charges table and restarted identity sequence");
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

                // Build lookup for valid price_bid_charges_id from PostgreSQL
                var validPriceBidChargesIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT price_bid_charges_id 
                    FROM price_bid_charges_master 
                    WHERE price_bid_charges_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validPriceBidChargesIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built valid price_bid_charges_id lookup with {validPriceBidChargesIds.Count} entries from price_bid_charges_master");

                // Build lookup for price_bid_charges_id from SQL Server TBL_PB_BUYEROTHERCHARGES
                // Map PB_BuyerChargesId -> PB_ChargesId
                var priceBidChargesLookup = new Dictionary<int, int>();
                using (var cmd = new SqlCommand(@"
                    SELECT PB_BuyerChargesId, PB_ChargesId 
                    FROM TBL_PB_BUYEROTHERCHARGES 
                    WHERE PB_BuyerChargesId IS NOT NULL AND PB_ChargesId IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var buyerChargesId = reader.GetInt32(0);
                        var chargesId = reader.GetInt32(1);
                        priceBidChargesLookup[buyerChargesId] = chargesId;
                    }
                }
                _logger.LogInformation($"Built price_bid_charges_id lookup with {priceBidChargesLookup.Count} entries from TBL_PB_BUYEROTHERCHARGES");

                // Build lookup for tax_master_id by tax_percentage from PostgreSQL
                var taxMasterLookup = new Dictionary<decimal, int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT tax_master_id, tax_percentage 
                    FROM tax_master 
                    WHERE tax_percentage IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var taxMasterId = reader.GetInt32(0);
                        var taxPercentage = reader.GetDecimal(1);
                        if (!taxMasterLookup.ContainsKey(taxPercentage))
                        {
                            taxMasterLookup[taxPercentage] = taxMasterId;
                        }
                    }
                }
                _logger.LogInformation($"Built tax_master lookup with {taxMasterLookup.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PB_SupplerChargesId,
                        EVENT_ID,
                        PB_BuyerChargesId,
                        Amount,
                        SUPPLIER_ID,
                        Percentage,
                        LotChargeFileName,
                        GSTPer,
                        GSTAmount
                    FROM TBL_PB_SUPPLIEROTHERCHARGES
                    WHERE PB_SupplerChargesId IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PB_SupplerChargesId = reader.GetInt32(0),
                            EVENT_ID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            PB_BuyerChargesId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            Amount = reader.IsDBNull(3) ? null : reader.GetDecimal(3),
                            SUPPLIER_ID = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            Percentage = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                            LotChargeFileName = reader.IsDBNull(6) ? null : reader.GetString(6),
                            GSTPer = reader.IsDBNull(7) ? null : reader.GetDecimal(7),
                            GSTAmount = reader.IsDBNull(8) ? null : reader.GetDecimal(8)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_PB_SUPPLIEROTHERCHARGES");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id exists in event_master (FK constraint)
                        if (record.EVENT_ID.HasValue && !validEventIds.Contains(record.EVENT_ID.Value))
                        {
                            _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: event_id={record.EVENT_ID} not found in event_master");
                            skippedRecords++;
                            continue;
                        }

                        // Lookup price_bid_charges_id from TBL_PB_BUYEROTHERCHARGES and validate against PostgreSQL
                        int? priceBidChargesId = null;
                        if (record.PB_BuyerChargesId.HasValue && 
                            priceBidChargesLookup.TryGetValue(record.PB_BuyerChargesId.Value, out var chargesId))
                        {
                            // Validate that the looked-up PB_ChargesId exists in PostgreSQL price_bid_charges_master
                            if (validPriceBidChargesIds.Contains(chargesId))
                            {
                                priceBidChargesId = chargesId;
                            }
                            else
                            {
                                // Skip record if PB_ChargesId doesn't exist in price_bid_charges_master
                                _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: PB_ChargesId={chargesId} not found in price_bid_charges_master (looked up from PB_BuyerChargesId={record.PB_BuyerChargesId})");
                                skippedRecords++;
                                continue;
                            }
                        }
                        else if (record.PB_BuyerChargesId.HasValue)
                        {
                            // Skip record if PB_BuyerChargesId cannot be found in TBL_PB_BUYEROTHERCHARGES
                            _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: PB_BuyerChargesId={record.PB_BuyerChargesId} not found in TBL_PB_BUYEROTHERCHARGES");
                            skippedRecords++;
                            continue;
                        }
                        else
                        {
                            // Skip record if PB_BuyerChargesId is null
                            _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: PB_BuyerChargesId is null");
                            skippedRecords++;
                            continue;
                        }

                        // Lookup tax_master_id by GSTPer (REQUIRED - NOT NULL constraint)
                        int? taxMasterId = null;
                        if (record.GSTPer.HasValue && taxMasterLookup.TryGetValue(record.GSTPer.Value, out var taxId))
                        {
                            taxMasterId = taxId;
                        }
                        else if (record.GSTPer.HasValue)
                        {
                            // Skip record if tax_master_id cannot be found (NOT NULL constraint)
                            _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: tax_master_id not found for GSTPer={record.GSTPer}");
                            skippedRecords++;
                            continue;
                        }
                        // If GSTPer is null but we have tax amounts, skip the record
                        else if (record.GSTAmount.HasValue && record.GSTAmount.Value != 0)
                        {
                            _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: GSTPer is null but GSTAmount is {record.GSTAmount}");
                            skippedRecords++;
                            continue;
                        }

                        // Calculate total_lot_charges_amount
                        decimal? totalLotChargesAmount = null;
                        if (record.Amount.HasValue || record.GSTAmount.HasValue)
                        {
                            totalLotChargesAmount = (record.Amount ?? 0m) + (record.GSTAmount ?? 0m);
                        }

                        // Final safety check for NOT NULL constraints before inserting
                        if (!priceBidChargesId.HasValue)
                        {
                            _logger.LogWarning($"Skipping PB_SupplerChargesId {record.PB_SupplerChargesId}: price_bid_charges_id is null after lookup");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            EventId = record.EVENT_ID,
                            UserPriceBidLotChargesId = record.PB_BuyerChargesId,
                            PriceBidChargesId = priceBidChargesId.Value, // Use .Value since we validated it's not null
                            SupplierId = record.SUPPLIER_ID,
                            Percentage = record.Percentage,
                            BasicLotChargesAmount = record.Amount,
                            TaxMasterId = taxMasterId,
                            TaxPercentage = record.GSTPer,
                            TaxAmount = record.GSTAmount,
                            TotalLotChargesAmount = totalLotChargesAmount,
                            SupplierLotChargesFileName = record.LotChargeFileName,
                            SupplierLotChargesFilePath = "", // Default empty string
                            Reason = "" // Default empty string (NOT NULL)
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
                        _logger.LogError($"PB_SupplerChargesId {record.PB_SupplerChargesId}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
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
            sql.AppendLine("INSERT INTO supplier_price_bid_lot_charges (");
            sql.AppendLine("    event_id, user_price_bid_lot_charges_id, price_bid_charges_id,");
            sql.AppendLine("    supplier_id, percentage, basic_lot_charges_amount,");
            sql.AppendLine("    tax_master_id, tax_percentage, tax_amount,");
            sql.AppendLine("    total_lot_charges_amount, supplier_lot_charges_file_name,");
            sql.AppendLine("    supplier_lot_charges_file_path, reason,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventId{i}, @UserPriceBidLotChargesId{i}, @PriceBidChargesId{i}, @SupplierId{i}, @Percentage{i}, @BasicLotChargesAmount{i}, @TaxMasterId{i}, @TaxPercentage{i}, @TaxAmount{i}, @TotalLotChargesAmount{i}, @SupplierLotChargesFileName{i}, @SupplierLotChargesFilePath{i}, @Reason{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EventId{i}", (object?)row.EventId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@UserPriceBidLotChargesId{i}", (object?)row.UserPriceBidLotChargesId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@PriceBidChargesId{i}", (object?)row.PriceBidChargesId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", (object?)row.SupplierId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@Percentage{i}", (object?)row.Percentage ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@BasicLotChargesAmount{i}", (object?)row.BasicLotChargesAmount ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@TaxMasterId{i}", (object?)row.TaxMasterId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@TaxPercentage{i}", (object?)row.TaxPercentage ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@TaxAmount{i}", (object?)row.TaxAmount ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@TotalLotChargesAmount{i}", (object?)row.TotalLotChargesAmount ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierLotChargesFileName{i}", (object?)row.SupplierLotChargesFileName ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierLotChargesFilePath{i}", row.SupplierLotChargesFilePath);
                cmd.Parameters.AddWithValue($"@Reason{i}", row.Reason);
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
            public int PB_SupplerChargesId { get; set; }
            public int? EVENT_ID { get; set; }
            public int? PB_BuyerChargesId { get; set; }
            public decimal? Amount { get; set; }
            public int? SUPPLIER_ID { get; set; }
            public decimal? Percentage { get; set; }
            public string? LotChargeFileName { get; set; }
            public decimal? GSTPer { get; set; }
            public decimal? GSTAmount { get; set; }
        }

        private class TargetRow
        {
            public int? EventId { get; set; }
            public int? UserPriceBidLotChargesId { get; set; }
            public int PriceBidChargesId { get; set; } // NOT NULL - validated before insert
            public int? SupplierId { get; set; }
            public decimal? Percentage { get; set; }
            public decimal? BasicLotChargesAmount { get; set; }
            public int? TaxMasterId { get; set; }
            public decimal? TaxPercentage { get; set; }
            public decimal? TaxAmount { get; set; }
            public decimal? TotalLotChargesAmount { get; set; }
            public string? SupplierLotChargesFileName { get; set; }
            public string SupplierLotChargesFilePath { get; set; } = ""; // NOT NULL
            public string Reason { get; set; } = ""; // NOT NULL
        }
    }
}
