using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SupplierEventPriceBidColumnsMigration
    {
        private readonly ILogger<SupplierEventPriceBidColumnsMigration> _logger;
        private readonly IConfiguration _configuration;

        public SupplierEventPriceBidColumnsMigration(IConfiguration configuration, ILogger<SupplierEventPriceBidColumnsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "supplier_event_price_bid_columns_id", type = "PostgreSQL auto-increment" },
                new { source = "EVENTID", target = "event_id", type = "int -> integer (FK to event_master, NOT NULL)" },
                new { source = "Lookup: TBL_PB_BUYER.PBID by EVENTID, PRTRANSID", target = "event_item_id", type = "Lookup -> integer (FK to event_items, NOT NULL)" },
                new { source = "SUPPLIER_ID", target = "supplier_id", type = "int -> integer (FK to supplier_master, NOT NULL)" },
                new { source = "Lookup: Match HEADER from SEQUENCEID=0 to event_price_bid_columns.column_name", target = "event_price_bid_columns_id", type = "Lookup -> integer (NOT NULL)" },
                new { source = "HEADER1-10 (where SEQUENCEID > 0)", target = "supplier_value_in_text", type = "nvarchar -> text (actual bid value)" },
                new { source = "HEADER1-10 parsed as numeric", target = "supplier_value_in_number", type = "parsed numeric -> numeric" }
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

                _logger.LogInformation("Starting SupplierEventPriceBidColumns migration...");

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

                // Build event_item_id lookup from SQL Server TBL_PB_BUYER
                var eventItemLookup = new Dictionary<(int EventId, int PrTransId), int>();
                using (var cmd = new SqlCommand(@"
                    SELECT PBID, EVENTID, PRTRANSID 
                    FROM TBL_PB_BUYER 
                    WHERE EVENTID IS NOT NULL AND PRTRANSID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var pbid = reader.GetInt32(0);
                        var eventId = reader.GetInt32(1);
                        var prTransId = reader.GetInt32(2);
                        var key = (eventId, prTransId);
                        if (!eventItemLookup.ContainsKey(key))
                        {
                            eventItemLookup[key] = pbid;
                        }
                    }
                }
                _logger.LogInformation($"Built event_item lookup with {eventItemLookup.Count} entries");

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

                // Build event_price_bid_columns_id lookup (by event_id and column_name)
                var priceBidColumnsLookup = new Dictionary<(int EventId, string ColumnName), int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_price_bid_columns_id, event_id, column_name 
                    FROM event_price_bid_columns", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var columnId = reader.GetInt32(0);
                        var eventId = reader.GetInt32(1);
                        var columnName = reader.GetString(2);
                        var key = (eventId, columnName);
                        if (!priceBidColumnsLookup.ContainsKey(key))
                        {
                            priceBidColumnsLookup[key] = columnId;
                        }
                    }
                }
                _logger.LogInformation($"Built price bid columns lookup with {priceBidColumnsLookup.Count} entries");

                // Build header names lookup from SEQUENCEID = 0 records
                // Key: (EVENTID, SUPPLIER_ID), Value: Dictionary of header position -> header name
                var headerNamesLookup = new Dictionary<(int EventId, int SupplierId), Dictionary<int, string>>();
                using (var cmd = new SqlCommand(@"
                    SELECT EVENTID, SUPPLIER_ID,
                           HEADER1, HEADER2, HEADER3, HEADER4, HEADER5,
                           HEADER6, HEADER7, HEADER8, HEADER9, HEADER10
                    FROM TBL_PB_SUPPLIER
                    WHERE SEQUENCEID = 0
                      AND EVENTID IS NOT NULL
                      AND SUPPLIER_ID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var eventId = reader.GetInt32(0);
                        var supplierId = reader.GetInt32(1);
                        var key = (eventId, supplierId);
                        
                        var headers = new Dictionary<int, string>();
                        for (int i = 1; i <= 10; i++)
                        {
                            var headerValue = reader.IsDBNull(1 + i) ? null : reader.GetString(1 + i);
                            if (!string.IsNullOrWhiteSpace(headerValue))
                            {
                                headers[i] = headerValue;
                            }
                        }
                        
                        if (headers.Any())
                        {
                            headerNamesLookup[key] = headers;
                        }
                    }
                }
                _logger.LogInformation($"Built header names lookup with {headerNamesLookup.Count} entries");

                // Fetch source data - only records where SEQUENCEID > 0
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PBID,
                        EVENTID,
                        SEQUENCEID,
                        PRTRANSID,
                        SUPPLIER_ID,
                        HEADER1, HEADER2, HEADER3, HEADER4, HEADER5,
                        HEADER6, HEADER7, HEADER8, HEADER9, HEADER10
                    FROM TBL_PB_SUPPLIER
                    WHERE SEQUENCEID > 0", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PBID = reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            SEQUENCEID = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            PRTRANSID = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            SUPPLIER_ID = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            HEADER1 = reader.IsDBNull(5) ? null : reader.GetString(5),
                            HEADER2 = reader.IsDBNull(6) ? null : reader.GetString(6),
                            HEADER3 = reader.IsDBNull(7) ? null : reader.GetString(7),
                            HEADER4 = reader.IsDBNull(8) ? null : reader.GetString(8),
                            HEADER5 = reader.IsDBNull(9) ? null : reader.GetString(9),
                            HEADER6 = reader.IsDBNull(10) ? null : reader.GetString(10),
                            HEADER7 = reader.IsDBNull(11) ? null : reader.GetString(11),
                            HEADER8 = reader.IsDBNull(12) ? null : reader.GetString(12),
                            HEADER9 = reader.IsDBNull(13) ? null : reader.GetString(13),
                            HEADER10 = reader.IsDBNull(14) ? null : reader.GetString(14)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_PB_SUPPLIER");

                // Process each row and create multiple records for each HEADER column
                var batchSize = 500;
                var processedCount = 0;
                var insertBatch = new List<TargetRow>();

                foreach (var row in sourceData)
                {
                    try
                    {
                        // Validate required fields
                        if (!row.EVENTID.HasValue || !validEventIds.Contains(row.EVENTID.Value))
                        {
                            _logger.LogDebug($"Skipping PBID {row.PBID}: Invalid or missing event_id {row.EVENTID}");
                            skippedRecords++;
                            continue;
                        }

                        if (!row.SUPPLIER_ID.HasValue || !validSupplierIds.Contains(row.SUPPLIER_ID.Value))
                        {
                            _logger.LogDebug($"Skipping PBID {row.PBID}: Invalid or missing supplier_id {row.SUPPLIER_ID}");
                            skippedRecords++;
                            continue;
                        }

                        // Lookup event_item_id
                        int? eventItemId = null;
                        if (row.EVENTID.HasValue && row.PRTRANSID.HasValue)
                        {
                            var key = (row.EVENTID.Value, row.PRTRANSID.Value);
                            if (eventItemLookup.TryGetValue(key, out var lookupId) && validEventItemIds.Contains(lookupId))
                            {
                                eventItemId = lookupId;
                            }
                        }

                        if (!eventItemId.HasValue)
                        {
                            _logger.LogDebug($"Skipping PBID {row.PBID}: Could not find valid event_item_id for EventID {row.EVENTID}, PRTRANSID {row.PRTRANSID}");
                            skippedRecords++;
                            continue;
                        }

                        // Get header names for this event and supplier (from SEQUENCEID=0 records)
                        var lookupKey = (row.EVENTID.Value, row.SUPPLIER_ID.Value);
                        if (!headerNamesLookup.TryGetValue(lookupKey, out var headerNames))
                        {
                            _logger.LogDebug($"Skipping PBID {row.PBID}: No header names found for EventID {row.EVENTID}, SupplierID {row.SUPPLIER_ID} (no SEQUENCEID=0 record)");
                            skippedRecords++;
                            continue;
                        }

                        // Process each HEADER column (1-10)
                        var headers = new[] { row.HEADER1, row.HEADER2, row.HEADER3, row.HEADER4, row.HEADER5,
                                              row.HEADER6, row.HEADER7, row.HEADER8, row.HEADER9, row.HEADER10 };

                        for (int i = 0; i < headers.Length; i++)
                        {
                            var headerValue = headers[i];
                            if (string.IsNullOrWhiteSpace(headerValue))
                                continue;

                            var headerPosition = i + 1; // 1-based position

                            // Get the column name from SEQUENCEID=0 record
                            if (!headerNames.TryGetValue(headerPosition, out var columnName))
                            {
                                _logger.LogDebug($"Skipping PBID {row.PBID}, HEADER{headerPosition}: No column name defined in SEQUENCEID=0 record");
                                continue;
                            }

                            // Lookup event_price_bid_columns_id by event_id and column_name
                            if (!priceBidColumnsLookup.TryGetValue((row.EVENTID.Value, columnName), out var columnId))
                            {
                                _logger.LogDebug($"Skipping PBID {row.PBID}, HEADER{headerPosition}: Could not find event_price_bid_columns_id for EventID {row.EVENTID}, ColumnName '{columnName}'");
                                continue;
                            }

                            // Try to parse as numeric
                            decimal? numericValue = null;
                            if (decimal.TryParse(headerValue, out var parsedValue))
                            {
                                numericValue = parsedValue;
                            }

                            // Create target row
                            var targetRow = new TargetRow
                            {
                                EventId = row.EVENTID.Value,
                                EventItemId = eventItemId.Value,
                                SupplierId = row.SUPPLIER_ID.Value,
                                EventPriceBidColumnsId = columnId,
                                SupplierValueInText = headerValue,
                                SupplierValueInNumber = numericValue
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

                        processedCount++;
                        if (processedCount % 100 == 0)
                        {
                            _logger.LogInformation($"Processed {processedCount}/{sourceData.Count} source records, created {migratedRecords} target records");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Error migrating PBID {row.PBID}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"SupplierEventPriceBidColumns migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error during SupplierEventPriceBidColumns migration: {ex.Message}");
                throw;
            }

            return migratedRecords;
        }

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO supplier_event_price_bid_columns (");
            sql.AppendLine("    event_id, event_item_id, supplier_id, event_price_bid_columns_id,");
            sql.AppendLine("    supplier_value_in_text, supplier_value_in_number,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventId{i}, @EventItemId{i}, @SupplierId{i}, @EventPriceBidColumnsId{i}, @SupplierValueInText{i}, @SupplierValueInNumber{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@EventItemId{i}", row.EventItemId);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", row.SupplierId);
                cmd.Parameters.AddWithValue($"@EventPriceBidColumnsId{i}", row.EventPriceBidColumnsId);
                cmd.Parameters.AddWithValue($"@SupplierValueInText{i}", (object?)row.SupplierValueInText ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierValueInNumber{i}", (object?)row.SupplierValueInNumber ?? DBNull.Value);
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

        private class TargetRow
        {
            public int EventId { get; set; }
            public int EventItemId { get; set; }
            public int SupplierId { get; set; }
            public int EventPriceBidColumnsId { get; set; }
            public string? SupplierValueInText { get; set; }
            public decimal? SupplierValueInNumber { get; set; }
        }

        private class SourceRow
        {
            public int PBID { get; set; }
            public int? EVENTID { get; set; }
            public int? SEQUENCEID { get; set; }
            public int? PRTRANSID { get; set; }
            public int? SUPPLIER_ID { get; set; }
            public string? HEADER1 { get; set; }
            public string? HEADER2 { get; set; }
            public string? HEADER3 { get; set; }
            public string? HEADER4 { get; set; }
            public string? HEADER5 { get; set; }
            public string? HEADER6 { get; set; }
            public string? HEADER7 { get; set; }
            public string? HEADER8 { get; set; }
            public string? HEADER9 { get; set; }
            public string? HEADER10 { get; set; }
        }
    }
}
