using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class EventFreezeCurrencyMigration
    {
    private readonly ILogger<EventFreezeCurrencyMigration> _logger;
    private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public EventFreezeCurrencyMigration(IConfiguration configuration, ILogger<EventFreezeCurrencyMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

    public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "FreezeCurrencyId", target = "event_freeze_currency_id", type = "numeric -> integer" },
                new { source = "Eventid", target = "event_id", type = "numeric -> integer (FK to event_master)" },
                new { source = "FromCurrency", target = "from_currency", type = "varchar -> text" },
                new { source = "ToCurrency", target = "to_currency", type = "varchar -> text" },
                new { source = "ExchangeRate", target = "exchange_rate", type = "decimal -> numeric" },
                new { source = "FreezeCurrencyDate", target = "created_date", type = "datetime -> timestamp" }
            };
        }

        public async Task<int> MigrateAsync()
        {
        _migrationLogger = new MigrationLogger(_logger, "event_freeze_currency");
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

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting EventFreezeCurrency migration...");

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

                // Load existing IDs into memory for fast lookup
                var existingIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_freeze_currency_id FROM event_freeze_currency", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        existingIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {existingIds.Count} existing records in target table");

                // Fetch data from SQL Server
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        FreezeCurrencyId,
                        Eventid,
                        FromCurrency,
                        ToCurrency,
                        ExchangeRate,
                        FreezeCurrencyDate
                    FROM TBL_FreezeCurrency", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            FreezeCurrencyId = reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetDecimal(0)),
                            Eventid = reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetDecimal(1)),
                            FromCurrency = reader.IsDBNull(2) ? null : reader.GetString(2),
                            ToCurrency = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ExchangeRate = reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                            FreezeCurrencyDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();
                var updateBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate required fields
                        if (!record.FreezeCurrencyId.HasValue)
                        {
                            _migrationLogger?.LogSkipped("FreezeCurrencyId is NULL", "Unknown");
                            skippedRecords++;
                            continue;
                        }

                        // Validate event_id exists (FK constraint)
                        if (!record.Eventid.HasValue || !validEventIds.Contains(record.Eventid.Value))
                        {
                            _migrationLogger?.LogSkipped($"event_id {record.Eventid} not found in event_master (FK constraint violation)", $"FreezeCurrencyId={record.FreezeCurrencyId}");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            FreezeCurrencyId = record.FreezeCurrencyId.Value,
                            EventId = record.Eventid.Value,
                            FromCurrency = string.IsNullOrWhiteSpace(record.FromCurrency) ? "USD" : record.FromCurrency.Trim(),
                            ToCurrency = string.IsNullOrWhiteSpace(record.ToCurrency) ? "USD" : record.ToCurrency.Trim(),
                            ExchangeRate = record.ExchangeRate ?? 1.0m,
                            CreatedDate = record.FreezeCurrencyDate
                        };

                        if (existingIds.Contains(targetRow.FreezeCurrencyId))
                        {
                            updateBatch.Add(targetRow);
                        }
                        else
                        {
                            insertBatch.Add(targetRow);
                        }

                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }

                        if (updateBatch.Count >= batchSize)
                        {
                            await ExecuteUpdateBatch(pgConnection, updateBatch);
                            updateBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _migrationLogger?.LogError($"Exception during processing: {ex.Message}", $"FreezeCurrencyId={record.FreezeCurrencyId}", ex);
                        errors.Add($"FreezeCurrencyId {record.FreezeCurrencyId}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                // Execute remaining batches
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                if (updateBatch.Any())
                {
                    await ExecuteUpdateBatch(pgConnection, updateBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
                if (errors.Any())
                {
                    _logger.LogWarning($"Encountered {errors.Count} errors during migration");
                }

                // Export migration stats to Excel
                if (_migrationLogger != null)
                {
                    var skippedLogEntries = _migrationLogger.GetSkippedRecords();
                    var skippedRecordsList = skippedLogEntries.Select(e => (e.RecordIdentifier, e.Message)).ToList();
                    var excelPath = Path.Combine("migration_outputs", $"EventFreezeCurrencyMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
                    MigrationStatsExporter.ExportToExcel(
                        excelPath,
                        migratedRecords + skippedRecords,
                        migratedRecords,
                        skippedRecords,
                        _logger,
                        skippedRecordsList
                    );
                    _migrationLogger.LogInfo($"Migration stats exported to {excelPath}");
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
            sql.AppendLine("INSERT INTO event_freeze_currency (");
            sql.AppendLine("    event_freeze_currency_id, event_id, from_currency, to_currency,");
            sql.AppendLine("    exchange_rate, created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@Id{i}, @EventId{i}, @FromCurrency{i}, @ToCurrency{i}, @ExchangeRate{i}, NULL, @CreatedDate{i}, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@Id{i}", row.FreezeCurrencyId);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@FromCurrency{i}", row.FromCurrency);
                cmd.Parameters.AddWithValue($"@ToCurrency{i}", row.ToCurrency);
                cmd.Parameters.AddWithValue($"@ExchangeRate{i}", row.ExchangeRate);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", row.CreatedDate.HasValue ? row.CreatedDate.Value : DateTime.UtcNow);
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

        private async Task ExecuteUpdateBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            foreach (var row in batch)
            {
                using var cmd = new NpgsqlCommand(@"
                    UPDATE event_freeze_currency SET
                        event_id = @EventId,
                        from_currency = @FromCurrency,
                        to_currency = @ToCurrency,
                        exchange_rate = @ExchangeRate,
                        modified_date = CURRENT_TIMESTAMP
                    WHERE event_freeze_currency_id = @Id", connection);

                cmd.Parameters.AddWithValue("@Id", row.FreezeCurrencyId);
                cmd.Parameters.AddWithValue("@EventId", row.EventId);
                cmd.Parameters.AddWithValue("@FromCurrency", row.FromCurrency);
                cmd.Parameters.AddWithValue("@ToCurrency", row.ToCurrency);
                cmd.Parameters.AddWithValue("@ExchangeRate", row.ExchangeRate);

                await cmd.ExecuteNonQueryAsync();
            }

            _logger.LogDebug($"Batch updated {batch.Count} records");
        }

        private class SourceRow
        {
            public int? FreezeCurrencyId { get; set; }
            public int? Eventid { get; set; }
            public string? FromCurrency { get; set; }
            public string? ToCurrency { get; set; }
            public decimal? ExchangeRate { get; set; }
            public DateTime? FreezeCurrencyDate { get; set; }
        }

        private class TargetRow
        {
            public int FreezeCurrencyId { get; set; }
            public int EventId { get; set; }
            public string FromCurrency { get; set; } = string.Empty;
            public string ToCurrency { get; set; } = string.Empty;
            public decimal ExchangeRate { get; set; }
            public DateTime? CreatedDate { get; set; }
        }
    }
}
