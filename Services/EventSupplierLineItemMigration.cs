using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class EventSupplierLineItemMigration
    {
        private readonly ILogger<EventSupplierLineItemMigration> _logger;
    private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public EventSupplierLineItemMigration(IConfiguration configuration, ILogger<EventSupplierLineItemMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

    public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "PBID", target = "event_supplier_line_item_id", type = "int -> integer" },
                new { source = "EVENTID", target = "event_id", type = "int -> integer (FK to event_master)" },
                new { source = "SUPPLIER_ID", target = "supplier_id", type = "int -> integer (FK to supplier_master)" },
                new { source = "Lookup: TBL_PB_BUYER.PBID by EVENTID, PRTRANSID", target = "event_item_id", type = "Lookup -> integer" },
                new { source = "Lookup: event_supplier_price_bid_id by SUPPLIER_ID, EVENTID", target = "event_supplier_price_bid_id", type = "Lookup -> integer" },
                new { source = "PRTRANSID", target = "erp_pr_lines_id", type = "int -> integer" },
                new { source = "HSNCode", target = "hsn_code", type = "nvarchar -> varchar" },
                new { source = "QTY", target = "qty", type = "decimal -> numeric" },
                new { source = "ProposedQty", target = "proposed_qty", type = "decimal -> numeric" },
                new { source = "ItemBidStatus (0:Bidding, 1:Included, 2:Regret)", target = "item_bid_status", type = "int -> varchar (CASE mapping)" },
                new { source = "UNIT_PRICE", target = "unit_price", type = "decimal -> numeric" },
                new { source = "DiscountPer", target = "discount_percentage", type = "decimal -> numeric" },
                new { source = "Calculated: UNIT_PRICE - (UNIT_PRICE * DiscountPer / 100)", target = "final_unit_price", type = "Calculated -> numeric" },
                new { source = "GSTID", target = "tax_master_id", type = "int -> integer" },
                new { source = "GSTPer", target = "tax_percentage", type = "decimal -> numeric" },
                new { source = "GSTAmount", target = "tax_amount", type = "decimal -> numeric" },
                new { source = "Calculated: (UNIT_PRICE - (UNIT_PRICE * DiscountPer / 100)) * QTY", target = "item_total", type = "Calculated -> numeric" },
                new { source = "AddtheReadyStock", target = "supplier_ready_stock", type = "decimal -> numeric" },
                new { source = "DeliveryDate", target = "item_delivery_date", type = "datetime -> timestamp with time zone" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            _migrationLogger = new MigrationLogger(_logger, "event_supplier_line_item");
            _migrationLogger.LogInfo("Starting migration");

            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;
            var errors = new List<string>();
            var skippedRecordDetails = new List<(string RecordId, string Reason)>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting EventSupplierLineItem migration...");

                // Build lookup maps for foreign keys
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

                // Get valid event_item_ids from PostgreSQL
                var validEventItemIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_item_id FROM event_items", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventItemIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {validEventItemIds.Count} valid event_item_ids");

                // Build lookup for event_item_id from TBL_PB_BUYER (PBID by EVENTID and PRTRANSID)
                var eventItemIdMap = new Dictionary<(int eventId, int prTransId), int>();
                using (var cmd = new SqlCommand(@"
                    SELECT EVENTID, PRTRANSID, PBID
                    FROM TBL_PB_BUYER
                    WHERE EVENTID IS NOT NULL AND PRTRANSID IS NOT NULL AND PBID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        try
                        {
                            var eventId = reader.GetInt32(0);
                            var prTransId = reader.GetInt32(1);
                            var pbId = reader.GetInt32(2);
                            eventItemIdMap[(eventId, prTransId)] = pbId;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Error reading event_item_id lookup: {ex.Message}");
                        }
                    }
                }
                _logger.LogInformation($"Found {eventItemIdMap.Count} event_item_id mappings from TBL_PB_BUYER");

                // Build lookup for event_supplier_price_bid_id from the just-created table
                var eventSupplierPriceBidIdMap = new Dictionary<(int supplierId, int eventId), int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_supplier_price_bid_id, supplier_id, event_id
                    FROM event_supplier_price_bid", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        try
                        {
                            var id = reader.GetInt32(0);
                            var supplierId = reader.GetInt32(1);
                            var eventId = reader.GetInt32(2);
                            eventSupplierPriceBidIdMap[(supplierId, eventId)] = id;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Error reading event_supplier_price_bid_id lookup: {ex.Message}");
                        }
                    }
                }
                _logger.LogInformation($"Found {eventSupplierPriceBidIdMap.Count} event_supplier_price_bid_id mappings");

                // Fetch source data from TBL_PB_SUPPLIER
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PBID,
                        EVENTID,
                        SUPPLIER_ID,
                        PRTRANSID,
                        HSNCode,
                        QTY,
                        ProposedQty,
                        ItemBidStatus,
                        UNIT_PRICE,
                        DiscountPer,
                        GSTID,
                        GSTPer,
                        GSTAmount,
                        AddtheReadyStock,
                        DeliveryDate
                    FROM TBL_PB_SUPPLIER
                    WHERE PBID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PBID = reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            SUPPLIER_ID = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            PRTRANSID = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            HSNCode = reader.IsDBNull(4) ? null : reader.GetString(4),
                            QTY = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                            ProposedQty = reader.IsDBNull(6) ? null : reader.GetDecimal(6),
                            ItemBidStatus = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            UNIT_PRICE = reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                            DiscountPer = reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                            GSTID = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                            GSTPer = reader.IsDBNull(11) ? null : reader.GetDecimal(11),
                            GSTAmount = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
                            AddtheReadyStock = reader.IsDBNull(13) ? null : reader.GetDecimal(13),
                            DeliveryDate = reader.IsDBNull(14) ? null : reader.GetDateTime(14)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records from TBL_PB_SUPPLIER");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate required fields
                        if (!record.EVENTID.HasValue || !record.SUPPLIER_ID.HasValue)
                        {
                            var reason = $"EVENTID or SUPPLIER_ID is NULL, skipping";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Validate event_id exists
                        if (!validEventIds.Contains(record.EVENTID.Value))
                        {
                            var reason = $"event_id {record.EVENTID.Value} not found in event_master";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Validate supplier_id exists
                        if (!validSupplierIds.Contains(record.SUPPLIER_ID.Value))
                        {
                            var reason = $"supplier_id {record.SUPPLIER_ID.Value} not found in supplier_master";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Lookup event_item_id (NOT NULL - skip record if not found)
                        int? eventItemId = null;
                        if (record.PRTRANSID.HasValue)
                        {
                            var key = (record.EVENTID.Value, record.PRTRANSID.Value);
                            if (eventItemIdMap.TryGetValue(key, out var itemId))
                            {
                                if (validEventItemIds.Contains(itemId))
                                {
                                    eventItemId = itemId;
                                }
                                else
                                {
                                    var reason = $"event_item_id {itemId} not found in event_items table (FK constraint), skipping record";
                                    _logger.LogDebug($"PBID {record.PBID}: {reason}");
                                    skippedRecords++;
                                    skippedRecordDetails.Add((record.PBID.ToString(), reason));
                                    continue;
                                }
                            }
                            else
                            {
                                var reason = $"No event_item_id mapping found for EVENTID {record.EVENTID.Value}, PRTRANSID {record.PRTRANSID.Value}, skipping record";
                                _logger.LogDebug($"PBID {record.PBID}: {reason}");
                                skippedRecords++;
                                skippedRecordDetails.Add((record.PBID.ToString(), reason));
                                continue;
                            }
                        }
                        else
                        {
                            var reason = $"PRTRANSID is NULL, cannot lookup event_item_id, skipping record";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Lookup event_supplier_price_bid_id (NOT NULL - skip record if not found)
                        int? eventSupplierPriceBidId = null;
                        var priceBidKey = (record.SUPPLIER_ID.Value, record.EVENTID.Value);
                        if (eventSupplierPriceBidIdMap.TryGetValue(priceBidKey, out var priceBidId))
                        {
                            eventSupplierPriceBidId = priceBidId;
                        }
                        else
                        {
                            var reason = $"No event_supplier_price_bid_id found for SUPPLIER_ID {record.SUPPLIER_ID.Value}, EVENTID {record.EVENTID.Value}, skipping record";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Validate erp_pr_lines_id (NOT NULL - skip if NULL)
                        if (!record.PRTRANSID.HasValue)
                        {
                            var reason = $"erp_pr_lines_id (PRTRANSID) is NULL (NOT NULL constraint), skipping record";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Validate tax_master_id (NOT NULL - skip if NULL)
                        if (!record.GSTID.HasValue)
                        {
                            var reason = $"tax_master_id (GSTID) is NULL (NOT NULL constraint), skipping record";
                            _logger.LogDebug($"PBID {record.PBID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.PBID.ToString(), reason));
                            continue;
                        }

                        // Map ItemBidStatus to string (nullable)
                        string? itemBidStatus = null;
                        if (record.ItemBidStatus.HasValue)
                        {
                            itemBidStatus = record.ItemBidStatus.Value switch
                            {
                                0 => "Bidding",
                                1 => "Included",
                                2 => "Regret",
                                _ => null
                            };
                        }

                        // Calculate final_unit_price (NOT NULL - default to 0)
                        var unitPrice = record.UNIT_PRICE ?? 0m;
                        var discountPer = record.DiscountPer ?? 0m;
                        var finalUnitPrice = unitPrice - (unitPrice * discountPer / 100);

                        // Calculate item_total (NOT NULL - default to 0)
                        var qty = record.QTY ?? 0m;
                        var itemTotal = finalUnitPrice * qty;

                        // Get other NOT NULL fields with defaults
                        var proposedQty = record.ProposedQty ?? 0m;
                        var taxPercentage = record.GSTPer ?? 0m;
                        var taxAmount = record.GSTAmount ?? 0m;

                        var targetRow = new TargetRow
                        {
                            EventSupplierLineItemId = record.PBID,
                            EventId = record.EVENTID.Value,
                            SupplierId = record.SUPPLIER_ID.Value,
                            EventItemId = eventItemId.Value, // NOT NULL - validated above
                            EventSupplierPriceBidId = eventSupplierPriceBidId.Value, // NOT NULL - validated above
                            ErpPrLinesId = record.PRTRANSID.Value, // NOT NULL - validated above
                            HsnCode = record.HSNCode ?? "", // NOT NULL - default to empty string
                            Qty = qty, // NOT NULL - defaults to 0
                            ProposedQty = proposedQty, // NOT NULL - defaults to 0
                            ItemBidStatus = itemBidStatus, // NULLABLE
                            UnitPrice = unitPrice, // NOT NULL - defaults to 0
                            DiscountPercentage = discountPer, // NOT NULL - defaults to 0
                            FinalUnitPrice = finalUnitPrice, // NOT NULL - calculated
                            TaxMasterId = record.GSTID.Value, // NOT NULL - validated above
                            TaxPercentage = taxPercentage, // NOT NULL - defaults to 0
                            TaxAmount = taxAmount, // NOT NULL - defaults to 0
                            ItemTotal = itemTotal, // NOT NULL - calculated
                            SupplierReadyStock = record.AddtheReadyStock ?? 0m, // NOT NULL - defaults to 0
                            ItemDeliveryDate = record.DeliveryDate
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
                        var errorMsg = $"PBID {record.PBID}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                        skippedRecordDetails.Add((record.PBID.ToString(), ex.Message));
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
                // Export migration stats to Excel
                MigrationStatsExporter.ExportToExcel(
                    "EventSupplierLineItemMigration_Stats.xlsx",
                    migratedRecords + skippedRecords,
                    migratedRecords,
                    skippedRecords,
                    _logger,
                    skippedRecordDetails
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
            sql.AppendLine("INSERT INTO event_supplier_line_item (");
            sql.AppendLine("    event_supplier_line_item_id, event_id, supplier_id, event_item_id,");
            sql.AppendLine("    event_supplier_price_bid_id, erp_pr_lines_id, hsn_code, qty,");
            sql.AppendLine("    proposed_qty, item_bid_status, unit_price, discount_percentage,");
            sql.AppendLine("    final_unit_price, tax_master_id, tax_percentage, tax_amount,");
            sql.AppendLine("    item_total, supplier_ready_stock, item_delivery_date,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventSupplierLineItemId{i}, @EventId{i}, @SupplierId{i}, @EventItemId{i}, @EventSupplierPriceBidId{i}, @ErpPrLinesId{i}, @HsnCode{i}, @Qty{i}, @ProposedQty{i}, @ItemBidStatus{i}, @UnitPrice{i}, @DiscountPercentage{i}, @FinalUnitPrice{i}, @TaxMasterId{i}, @TaxPercentage{i}, @TaxAmount{i}, @ItemTotal{i}, @SupplierReadyStock{i}, @ItemDeliveryDate{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EventSupplierLineItemId{i}", row.EventSupplierLineItemId);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", row.SupplierId);
                cmd.Parameters.AddWithValue($"@EventItemId{i}", row.EventItemId);
                cmd.Parameters.AddWithValue($"@EventSupplierPriceBidId{i}", row.EventSupplierPriceBidId);
                cmd.Parameters.AddWithValue($"@ErpPrLinesId{i}", row.ErpPrLinesId);
                cmd.Parameters.AddWithValue($"@HsnCode{i}", row.HsnCode);
                cmd.Parameters.AddWithValue($"@Qty{i}", row.Qty);
                cmd.Parameters.AddWithValue($"@ProposedQty{i}", row.ProposedQty);
                cmd.Parameters.AddWithValue($"@ItemBidStatus{i}", (object?)row.ItemBidStatus ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@UnitPrice{i}", row.UnitPrice);
                cmd.Parameters.AddWithValue($"@DiscountPercentage{i}", row.DiscountPercentage);
                cmd.Parameters.AddWithValue($"@FinalUnitPrice{i}", row.FinalUnitPrice);
                cmd.Parameters.AddWithValue($"@TaxMasterId{i}", row.TaxMasterId);
                cmd.Parameters.AddWithValue($"@TaxPercentage{i}", row.TaxPercentage);
                cmd.Parameters.AddWithValue($"@TaxAmount{i}", row.TaxAmount);
                cmd.Parameters.AddWithValue($"@ItemTotal{i}", row.ItemTotal);
                cmd.Parameters.AddWithValue($"@SupplierReadyStock{i}", row.SupplierReadyStock);
                cmd.Parameters.AddWithValue($"@ItemDeliveryDate{i}", (object?)row.ItemDeliveryDate ?? DBNull.Value);
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
            public int PBID { get; set; }
            public int? EVENTID { get; set; }
            public int? SUPPLIER_ID { get; set; }
            public int? PRTRANSID { get; set; }
            public string? HSNCode { get; set; }
            public decimal? QTY { get; set; }
            public decimal? ProposedQty { get; set; }
            public int? ItemBidStatus { get; set; }
            public decimal? UNIT_PRICE { get; set; }
            public decimal? DiscountPer { get; set; }
            public int? GSTID { get; set; }
            public decimal? GSTPer { get; set; }
            public decimal? GSTAmount { get; set; }
            public decimal? AddtheReadyStock { get; set; }
            public DateTime? DeliveryDate { get; set; }
        }

        private class TargetRow
        {
            public int EventSupplierLineItemId { get; set; }
            public int EventId { get; set; }
            public int SupplierId { get; set; }
            public int EventItemId { get; set; } // NOT NULL
            public int EventSupplierPriceBidId { get; set; } // NOT NULL
            public int ErpPrLinesId { get; set; } // NOT NULL
            public string HsnCode { get; set; } = ""; // NOT NULL
            public decimal Qty { get; set; } // NOT NULL
            public decimal ProposedQty { get; set; } // NOT NULL
            public string? ItemBidStatus { get; set; } // NULLABLE
            public decimal UnitPrice { get; set; } // NOT NULL
            public decimal DiscountPercentage { get; set; } // NOT NULL
            public decimal FinalUnitPrice { get; set; } // NOT NULL
            public int TaxMasterId { get; set; } // NOT NULL
            public decimal TaxPercentage { get; set; } // NOT NULL
            public decimal TaxAmount { get; set; } // NOT NULL
            public decimal ItemTotal { get; set; } // NOT NULL
            public decimal SupplierReadyStock { get; set; } // NOT NULL
            public DateTime? ItemDeliveryDate { get; set; } // NULLABLE
        }

        /// <summary>
        /// Row structure for TBL_AUC_SUPPLIER (Auction Supplier) records.
        /// These records are ordered by UPDATEID and upserted based on event_id + supplier_id + erp_pr_lines_id.
        /// </summary>
        private class AucSupplierRow
        {
            public int UPDATEID { get; set; }
            public int? EVENTID { get; set; }
            public int? SUPPLIER_ID { get; set; }
            public int? PRTRANSID { get; set; }
            public string? HSNCode { get; set; }
            public decimal? QTY { get; set; }
            public decimal? ProposedQty { get; set; }
            public int? ItemBidStatus { get; set; }
            public decimal? UNIT_PRICE { get; set; }
            public decimal? DiscountPer { get; set; }
            public int? GSTID { get; set; }
            public decimal? GSTPer { get; set; }
            public decimal? GSTAmount { get; set; }
            public decimal? AddtheReadyStock { get; set; }
            public DateTime? DeliveryDate { get; set; }
        }

        /// <summary>
        /// Upsert records from TBL_AUC_SUPPLIER (Auction Supplier).
        /// Records are ordered by UPDATEID and upserted based on the unique key: event_id + supplier_id + erp_pr_lines_id.
        /// Later records (higher UPDATEID) will overwrite earlier records with the same key.
        /// </summary>
        public async Task<int> UpsertAuctionSupplierAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var upsertedRecords = 0;
            var skippedRecords = 0;
            var errors = new List<string>();
            var skippedRecordDetails = new List<(string RecordId, string Reason)>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting EventSupplierLineItem UPSERT from TBL_AUC_SUPPLIER...");

                // Build lookup maps for foreign keys
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

                // Get valid event_item_ids from PostgreSQL
                var validEventItemIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_item_id FROM event_items", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventItemIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {validEventItemIds.Count} valid event_item_ids");

                // Build lookup for event_item_id from TBL_PB_BUYER (PBID by EVENTID and PRTRANSID)
                var eventItemIdMap = new Dictionary<(int eventId, int prTransId), int>();
                using (var cmd = new SqlCommand(@"
                    SELECT EVENTID, PRTRANSID, PBID
                    FROM TBL_PB_BUYER
                    WHERE EVENTID IS NOT NULL AND PRTRANSID IS NOT NULL AND PBID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        try
                        {
                            var eventId = reader.GetInt32(0);
                            var prTransId = reader.GetInt32(1);
                            var pbId = reader.GetInt32(2);
                            eventItemIdMap[(eventId, prTransId)] = pbId;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Error reading event_item_id lookup: {ex.Message}");
                        }
                    }
                }
                _logger.LogInformation($"Found {eventItemIdMap.Count} event_item_id mappings from TBL_PB_BUYER");

                // Build lookup for event_supplier_price_bid_id from the just-created table
                var eventSupplierPriceBidIdMap = new Dictionary<(int supplierId, int eventId), int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_supplier_price_bid_id, supplier_id, event_id
                    FROM event_supplier_price_bid", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        try
                        {
                            var id = reader.GetInt32(0);
                            var supplierId = reader.GetInt32(1);
                            var eventId = reader.GetInt32(2);
                            eventSupplierPriceBidIdMap[(supplierId, eventId)] = id;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Error reading event_supplier_price_bid_id lookup: {ex.Message}");
                        }
                    }
                }
                _logger.LogInformation($"Found {eventSupplierPriceBidIdMap.Count} event_supplier_price_bid_id mappings");

                // Fetch auction source data from TBL_AUC_SUPPLIER, ordered by UPDATEID
                var auctionData = new List<AucSupplierRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        UPDATEID,
                        EVENTID,
                        SUPPLIER_ID,
                        PRTRANSID,
                        HSNCode,
                        QTY,
                        ProposedQty,
                        ItemBidStatus,
                        UNIT_PRICE,
                        DiscountPer,
                        GSTID,
                        GSTPer,
                        GSTAmount,
                        AddtheReadyStock,
                        DeliveryDate
                    FROM TBL_AUC_SUPPLIER
                    WHERE UPDATEID IS NOT NULL
                    ORDER BY UPDATEID", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        auctionData.Add(new AucSupplierRow
                        {
                            UPDATEID = reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            SUPPLIER_ID = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            PRTRANSID = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            HSNCode = reader.IsDBNull(4) ? null : reader.GetString(4),
                            QTY = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                            ProposedQty = reader.IsDBNull(6) ? null : reader.GetDecimal(6),
                            ItemBidStatus = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            UNIT_PRICE = reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                            DiscountPer = reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                            GSTID = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                            GSTPer = reader.IsDBNull(11) ? null : reader.GetDecimal(11),
                            GSTAmount = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
                            AddtheReadyStock = reader.IsDBNull(13) ? null : reader.GetDecimal(13),
                            DeliveryDate = reader.IsDBNull(14) ? null : reader.GetDateTime(14)
                        });
                    }
                }

                _logger.LogInformation($"Found {auctionData.Count} records from TBL_AUC_SUPPLIER (ordered by UPDATEID)");

                // Process and upsert one by one
                foreach (var record in auctionData)
                {
                    try
                    {
                        // Validate required fields for upsert key
                        if (!record.EVENTID.HasValue || !record.SUPPLIER_ID.HasValue || !record.PRTRANSID.HasValue)
                        {
                            var reason = $"EVENTID, SUPPLIER_ID, or PRTRANSID is NULL, cannot form unique key, skipping";
                            _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                            continue;
                        }

                        // Validate event_id exists
                        if (!validEventIds.Contains(record.EVENTID.Value))
                        {
                            var reason = $"event_id {record.EVENTID.Value} not found in event_master";
                            _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                            continue;
                        }

                        // Validate supplier_id exists
                        if (!validSupplierIds.Contains(record.SUPPLIER_ID.Value))
                        {
                            var reason = $"supplier_id {record.SUPPLIER_ID.Value} not found in supplier_master";
                            _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                            continue;
                        }

                        // Lookup event_item_id (NOT NULL - skip record if not found)
                        int? eventItemId = null;
                        var key = (record.EVENTID.Value, record.PRTRANSID.Value);
                        if (eventItemIdMap.TryGetValue(key, out var itemId))
                        {
                            if (validEventItemIds.Contains(itemId))
                            {
                                eventItemId = itemId;
                            }
                            else
                            {
                                var reason = $"event_item_id {itemId} not found in event_items table (FK constraint), skipping record";
                                _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                                skippedRecords++;
                                skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                                continue;
                            }
                        }
                        else
                        {
                            var reason = $"No event_item_id mapping found for EVENTID {record.EVENTID.Value}, PRTRANSID {record.PRTRANSID.Value}, skipping record";
                            _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                            continue;
                        }

                        // Lookup event_supplier_price_bid_id (NOT NULL - skip record if not found)
                        int? eventSupplierPriceBidId = null;
                        var priceBidKey = (record.SUPPLIER_ID.Value, record.EVENTID.Value);
                        if (eventSupplierPriceBidIdMap.TryGetValue(priceBidKey, out var priceBidId))
                        {
                            eventSupplierPriceBidId = priceBidId;
                        }
                        else
                        {
                            var reason = $"No event_supplier_price_bid_id found for SUPPLIER_ID {record.SUPPLIER_ID.Value}, EVENTID {record.EVENTID.Value}, skipping record";
                            _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                            continue;
                        }

                        // Validate tax_master_id (NOT NULL - skip if NULL)
                        if (!record.GSTID.HasValue)
                        {
                            var reason = $"tax_master_id (GSTID) is NULL (NOT NULL constraint), skipping record";
                            _logger.LogDebug($"UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedRecordDetails.Add((record.UPDATEID.ToString(), reason));
                            continue;
                        }

                        // Map ItemBidStatus to string (nullable)
                        string? itemBidStatus = null;
                        if (record.ItemBidStatus.HasValue)
                        {
                            itemBidStatus = record.ItemBidStatus.Value switch
                            {
                                0 => "Bidding",
                                1 => "Included",
                                2 => "Regret",
                                _ => null
                            };
                        }

                        // Calculate final_unit_price (NOT NULL - default to 0)
                        var unitPrice = record.UNIT_PRICE ?? 0m;
                        var discountPer = record.DiscountPer ?? 0m;
                        var finalUnitPrice = unitPrice - (unitPrice * discountPer / 100);

                        // Calculate item_total (NOT NULL - default to 0)
                        var qty = record.QTY ?? 0m;
                        var itemTotal = finalUnitPrice * qty;

                        // Get other NOT NULL fields with defaults
                        var proposedQty = record.ProposedQty ?? 0m;
                        var taxPercentage = record.GSTPer ?? 0m;
                        var taxAmount = record.GSTAmount ?? 0m;

                        // UPSERT based on unique key: event_id + supplier_id + erp_pr_lines_id
                        using var upsertCmd = new NpgsqlCommand(@"
                            INSERT INTO event_supplier_line_item (
                                event_id, supplier_id, erp_pr_lines_id, event_item_id,
                                event_supplier_price_bid_id, hsn_code, qty,
                                proposed_qty, item_bid_status, unit_price, discount_percentage,
                                final_unit_price, tax_master_id, tax_percentage, tax_amount,
                                item_total, supplier_ready_stock, item_delivery_date,
                                created_by, created_date, modified_by, modified_date,
                                is_deleted, deleted_by, deleted_date
                            ) VALUES (
                                @EventId, @SupplierId, @ErpPrLinesId, @EventItemId,
                                @EventSupplierPriceBidId, @HsnCode, @Qty,
                                @ProposedQty, @ItemBidStatus, @UnitPrice, @DiscountPercentage,
                                @FinalUnitPrice, @TaxMasterId, @TaxPercentage, @TaxAmount,
                                @ItemTotal, @SupplierReadyStock, @ItemDeliveryDate,
                                NULL, CURRENT_TIMESTAMP, NULL, CURRENT_TIMESTAMP,
                                false, NULL, NULL
                            )
                            ON CONFLICT (event_id, supplier_id, erp_pr_lines_id) 
                            DO UPDATE SET
                                event_item_id = EXCLUDED.event_item_id,
                                event_supplier_price_bid_id = EXCLUDED.event_supplier_price_bid_id,
                                hsn_code = EXCLUDED.hsn_code,
                                qty = EXCLUDED.qty,
                                proposed_qty = EXCLUDED.proposed_qty,
                                item_bid_status = EXCLUDED.item_bid_status,
                                unit_price = EXCLUDED.unit_price,
                                discount_percentage = EXCLUDED.discount_percentage,
                                final_unit_price = EXCLUDED.final_unit_price,
                                tax_master_id = EXCLUDED.tax_master_id,
                                tax_percentage = EXCLUDED.tax_percentage,
                                tax_amount = EXCLUDED.tax_amount,
                                item_total = EXCLUDED.item_total,
                                supplier_ready_stock = EXCLUDED.supplier_ready_stock,
                                item_delivery_date = EXCLUDED.item_delivery_date,
                                modified_date = CURRENT_TIMESTAMP", pgConnection);

                        upsertCmd.Parameters.AddWithValue("@EventId", record.EVENTID.Value);
                        upsertCmd.Parameters.AddWithValue("@SupplierId", record.SUPPLIER_ID.Value);
                        upsertCmd.Parameters.AddWithValue("@ErpPrLinesId", record.PRTRANSID.Value);
                        upsertCmd.Parameters.AddWithValue("@EventItemId", eventItemId.Value);
                        upsertCmd.Parameters.AddWithValue("@EventSupplierPriceBidId", eventSupplierPriceBidId.Value);
                        upsertCmd.Parameters.AddWithValue("@HsnCode", record.HSNCode ?? "");
                        upsertCmd.Parameters.AddWithValue("@Qty", qty);
                        upsertCmd.Parameters.AddWithValue("@ProposedQty", proposedQty);
                        upsertCmd.Parameters.AddWithValue("@ItemBidStatus", (object?)itemBidStatus ?? DBNull.Value);
                        upsertCmd.Parameters.AddWithValue("@UnitPrice", unitPrice);
                        upsertCmd.Parameters.AddWithValue("@DiscountPercentage", discountPer);
                        upsertCmd.Parameters.AddWithValue("@FinalUnitPrice", finalUnitPrice);
                        upsertCmd.Parameters.AddWithValue("@TaxMasterId", record.GSTID.Value);
                        upsertCmd.Parameters.AddWithValue("@TaxPercentage", taxPercentage);
                        upsertCmd.Parameters.AddWithValue("@TaxAmount", taxAmount);
                        upsertCmd.Parameters.AddWithValue("@ItemTotal", itemTotal);
                        upsertCmd.Parameters.AddWithValue("@SupplierReadyStock", record.AddtheReadyStock ?? 0m);
                        upsertCmd.Parameters.AddWithValue("@ItemDeliveryDate", (object?)record.DeliveryDate ?? DBNull.Value);

                        await upsertCmd.ExecuteNonQueryAsync();
                        upsertedRecords++;
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"UPDATEID {record.UPDATEID}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                        skippedRecordDetails.Add((record.UPDATEID.ToString(), ex.Message));
                    }
                }

                _logger.LogInformation($"Auction UPSERT completed. Upserted: {upsertedRecords}, Skipped: {skippedRecords}");
                if (errors.Any())
                {
                    _logger.LogWarning($"Encountered {errors.Count} errors during auction upsert");
                }
                // Export upsert stats to Excel
                MigrationStatsExporter.ExportToExcel(
                    "EventSupplierLineItemAuctionUpsert_Stats.xlsx",
                    upsertedRecords + skippedRecords,
                    upsertedRecords,
                    skippedRecords,
                    _logger,
                    skippedRecordDetails
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Auction UPSERT failed");
                throw;
            }

            return upsertedRecords;
        }
    }
}
