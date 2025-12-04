using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class NfaPoConditionMigration
    {
        private readonly ILogger<NfaPoConditionMigration> _logger;
        private readonly IConfiguration _configuration;

        public NfaPoConditionMigration(IConfiguration configuration, ILogger<NfaPoConditionMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "AwardEventPoConditionId (Auto-increment)", target = "nfa_po_condition_id", type = "int -> integer (NOT NULL, Auto-increment)" },
                new { source = "AwardEventItemId", target = "nfa_line_id", type = "int -> integer (NOT NULL, FK)" },
                new { source = "PoConditionId", target = "po_condition_id", type = "int -> integer (NOT NULL, FK)" },
                new { source = "Percentage", target = "value", type = "nvarchar -> numeric (NOT NULL)" }
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

                _logger.LogInformation("Starting NfaPoCondition migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE nfa_po_condition RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset nfa_po_condition table and restarted identity sequence");
                }

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

                // Build lookup for valid po_condition_id from PostgreSQL
                var validPoConditionIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT po_condition_id 
                    FROM po_condition_master 
                    WHERE po_condition_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validPoConditionIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built po_condition_id lookup with {validPoConditionIds.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        AwardEventPoConditionId,
                        AwardEventItemId,
                        PoConditionId,
                        Percentage
                    FROM TBL_AwardEventPoCondition
                    WHERE AwardEventPoConditionId IS NOT NULL
                    ORDER BY AwardEventPoConditionId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            AwardEventPoConditionId = reader.GetInt32(0),
                            AwardEventItemId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            PoConditionId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            Percentage = reader.IsDBNull(3) ? null : reader.GetString(3)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_AwardEventPoCondition");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate nfa_line_id (REQUIRED - NOT NULL, FK)
                        if (!record.AwardEventItemId.HasValue)
                        {
                            _logger.LogWarning($"Skipping AwardEventPoConditionId {record.AwardEventPoConditionId}: AwardEventItemId is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validNfaLineIds.Contains(record.AwardEventItemId.Value))
                        {
                            _logger.LogWarning($"Skipping AwardEventPoConditionId {record.AwardEventPoConditionId}: AwardEventItemId={record.AwardEventItemId} not found in nfa_line");
                            skippedRecords++;
                            continue;
                        }

                        // Validate po_condition_id (REQUIRED - NOT NULL, FK)
                        if (!record.PoConditionId.HasValue)
                        {
                            _logger.LogWarning($"Skipping AwardEventPoConditionId {record.AwardEventPoConditionId}: PoConditionId is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validPoConditionIds.Contains(record.PoConditionId.Value))
                        {
                            _logger.LogWarning($"Skipping AwardEventPoConditionId {record.AwardEventPoConditionId}: PoConditionId={record.PoConditionId} not found in po_condition_master");
                            skippedRecords++;
                            continue;
                        }

                        // Parse and validate Percentage (REQUIRED - NOT NULL)
                        if (string.IsNullOrWhiteSpace(record.Percentage))
                        {
                            _logger.LogWarning($"Skipping AwardEventPoConditionId {record.AwardEventPoConditionId}: Percentage is null/empty");
                            skippedRecords++;
                            continue;
                        }

                        if (!decimal.TryParse(record.Percentage, out var value))
                        {
                            _logger.LogWarning($"Skipping AwardEventPoConditionId {record.AwardEventPoConditionId}: Percentage='{record.Percentage}' is not a valid number");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            NfaPoConditionId = record.AwardEventPoConditionId,
                            NfaLineId = record.AwardEventItemId.Value,
                            PoConditionId = record.PoConditionId.Value,
                            Value = value
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
                        _logger.LogError($"Error processing AwardEventPoConditionId {record.AwardEventPoConditionId}: {ex.Message}");
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
            sql.AppendLine("INSERT INTO nfa_po_condition (");
            sql.AppendLine("    nfa_po_condition_id, nfa_line_id, po_condition_id, value,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@NfaPoConditionId{i}, @NfaLineId{i}, @PoConditionId{i}, @Value{i}, NULL, NULL, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@NfaPoConditionId{i}", row.NfaPoConditionId);
                cmd.Parameters.AddWithValue($"@NfaLineId{i}", row.NfaLineId);
                cmd.Parameters.AddWithValue($"@PoConditionId{i}", row.PoConditionId);
                cmd.Parameters.AddWithValue($"@Value{i}", row.Value);
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
            public int AwardEventPoConditionId { get; set; }
            public int? AwardEventItemId { get; set; }
            public int? PoConditionId { get; set; }
            public string? Percentage { get; set; }
        }

        private class TargetRow
        {
            public int NfaPoConditionId { get; set; }
            public int NfaLineId { get; set; }
            public int PoConditionId { get; set; }
            public decimal Value { get; set; }
        }
    }
}
