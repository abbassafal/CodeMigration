using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class EventPriceBidColumnsMigration
    {
        private readonly ILogger<EventPriceBidColumnsMigration> _logger;
        private readonly IConfiguration _configuration;

        public EventPriceBidColumnsMigration(IConfiguration configuration, ILogger<EventPriceBidColumnsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "EVENTID", target = "event_id", type = "int -> integer (FK to event_master)" },
                new { source = "HEADER1-10 (unpivoted)", target = "column_name", type = "nvarchar -> text (WHERE ISNULL(SEQUENCEID,0)=0)" },
                new { source = "ExtChargeHeader1-10 (unpivoted)", target = "column_name", type = "nvarchar -> text (WHERE ISNULL(SEQUENCEID,0)=0)" },
                new { source = "Auto-generated", target = "event_price_bid_columns_id", type = "PostgreSQL auto-increment" },
                new { source = "Header position (1,2,3...)", target = "sequence_number", type = "integer (HEADER1=1, HEADER2=2, etc.)" },
                new { source = "Default: 'Text'", target = "column_type", type = "text" },
                new { source = "Default: true", target = "mandatory", type = "boolean" }
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

                _logger.LogInformation("Starting EventPriceBidColumns migration...");

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

                // Fetch data from SQL Server - all header columns
                // Only process records where SEQUENCEID is 0 or NULL (unpivot logic)
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"SELECT 
                    PBID,
                    EVENTID,
                    PRTRANSID,
                    HEADER1, HEADER2, HEADER3, HEADER4, HEADER5,
                    HEADER6, HEADER7, HEADER8, HEADER9, HEADER10,
                    ExtChargeHeader1, ExtChargeHeader2, ExtChargeHeader3, ExtChargeHeader4, ExtChargeHeader5,
                    ExtChargeHeader6, ExtChargeHeader7, ExtChargeHeader8, ExtChargeHeader9, ExtChargeHeader10
                FROM TBL_PB_BUYER
                WHERE ISNULL(SEQUENCEID, 0) = 0", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var row = new SourceRow
                        {
                            PBID = reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            PRTRANSID = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            Headers = new Dictionary<string, string>()
                        };

                        // Read all header columns
                        for (int i = 1; i <= 10; i++)
                        {
                            var headerValue = reader.IsDBNull(2 + i) ? null : reader.GetString(2 + i);
                            if (!string.IsNullOrWhiteSpace(headerValue))
                            {
                                row.Headers[$"HEADER{i}"] = headerValue;
                            }
                        }

                        // Read all ExtChargeHeader columns
                        for (int i = 1; i <= 10; i++)
                        {
                            var headerValue = reader.IsDBNull(12 + i) ? null : reader.GetString(12 + i);
                            if (!string.IsNullOrWhiteSpace(headerValue))
                            {
                                row.Headers[$"ExtChargeHeader{i}"] = headerValue;
                            }
                        }

                        sourceData.Add(row);
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                // No need to check existing IDs - we'll use auto-increment
                // PostgreSQL will handle the ID generation automatically
                
                int totalRowsGenerated = 0;
                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id exists (FK constraint)
                        if (!record.EVENTID.HasValue || !validEventIds.Contains(record.EVENTID.Value))
                        {
                            _logger.LogDebug($"PBID {record.PBID}: event_id {record.EVENTID} not found in event_master (FK constraint violation)");
                            skippedRecords++;
                            continue;
                        }

                        // If no headers have data, skip this record
                        if (!record.Headers.Any())
                        {
                            _logger.LogDebug($"PBID {record.PBID}: No header data found, skipping");
                            skippedRecords++;
                            continue;
                        }

                        // Generate a row for each header that has data
                        // Sort headers numerically: HEADER1, HEADER2, ..., HEADER10, ExtChargeHeader1, ...
                        var sortedHeaders = record.Headers
                            .OrderBy(h => h.Key.StartsWith("ExtChargeHeader") ? 1 : 0) // HEADER first, then ExtChargeHeader
                            .ThenBy(h => {
                                // Extract numeric part for proper numeric sorting
                                var key = h.Key;
                                var numStr = new string(key.Where(char.IsDigit).ToArray());
                                return int.TryParse(numStr, out int num) ? num : 0;
                            });

                        int sequenceNumber = 1;
                        foreach (var header in sortedHeaders)
                        {
                            // Let PostgreSQL auto-generate the ID
                            var targetRow = new TargetRow
                            {
                                EventId = record.EVENTID.Value,
                                ColumnName = header.Value,
                                SequenceNumber = sequenceNumber
                            };

                            insertBatch.Add(targetRow);

                            totalRowsGenerated++;
                            sequenceNumber++;
                        }

                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch, _logger);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"PBID {record.PBID}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch, _logger);
                }

                _logger.LogInformation($"Migration completed. Source Records Processed: {migratedRecords}, Skipped: {skippedRecords}, Total Rows Generated: {totalRowsGenerated}");
                
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

        private static async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch, ILogger logger)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO event_price_bid_columns (");
            sql.AppendLine("    event_id, column_name, column_type,");
            sql.AppendLine("    mandatory, sequence_number, created_by, created_date,");
            sql.AppendLine("    modified_by, modified_date, is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventId{i}, @ColumnName{i}, 'Text', true, @SequenceNumber{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@ColumnName{i}", row.ColumnName);
                cmd.Parameters.AddWithValue($"@SequenceNumber{i}", row.SequenceNumber);
            }

            sql.AppendLine(string.Join(",\n", values));
            cmd.CommandText = sql.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                logger.LogDebug($"Batch inserted {rowsAffected} records");
            }
            catch (Exception ex)
            {
                logger.LogError($"Batch insert failed: {ex.Message}");
                throw;
            }
        }

        private class TargetRow
        {
            public int EventId { get; set; }
            public string ColumnName { get; set; } = string.Empty;
            public int SequenceNumber { get; set; }
        }

        private class SourceRow
        {
            public int PBID { get; set; }
            public int? EVENTID { get; set; }
            public int? PRTRANSID { get; set; }
            public Dictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
        }
    }
}
