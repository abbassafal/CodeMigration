using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class PriceBidChargesMasterMigration
    {
        private readonly ILogger<PriceBidChargesMasterMigration> _logger;
        private readonly IConfiguration _configuration;

        public PriceBidChargesMasterMigration(IConfiguration configuration, ILogger<PriceBidChargesMasterMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "PB_ChargesID", target = "price_bid_charges_id", type = "int -> integer" },
                new { source = "Fixed: 'Lot Charges'", target = "charges_type", type = "NOT NULL, default 'Lot Charges'" },
                new { source = "ChargesName", target = "charges_name", type = "nvarchar -> text (NOT NULL, default empty string)" },
                new { source = "ISACTIVE", target = "Note: Not mapped directly", type = "numeric (handled in status if needed)" },
                new { source = "GSTPercentage", target = "Note: Not mapped", type = "decimal (no corresponding column)" }
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

                _logger.LogInformation("Starting PriceBidChargesMaster migration...");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PB_ChargesID,
                        ChargesName,
                        ISACTIVE,
                        GSTPercentage
                    FROM TBL_PB_OTHERCHARGESMST", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PB_ChargesID = reader.GetInt32(0),
                            ChargesName = reader.IsDBNull(1) ? null : reader.GetString(1),
                            ISACTIVE = reader.IsDBNull(2) ? null : reader.GetDecimal(2),
                            GSTPercentage = reader.IsDBNull(3) ? null : reader.GetDecimal(3)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records from TBL_PB_OTHERCHARGESMST");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        var targetRow = new TargetRow
                        {
                            PriceBidChargesId = record.PB_ChargesID,
                            ChargesType = "Lot Charges", // NOT NULL - fixed value
                            ChargesName = record.ChargesName ?? "" // NOT NULL - default to empty string
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
                        var errorMsg = $"PB_ChargesID {record.PB_ChargesID}: {ex.Message}";
                        _logger.LogError(errorMsg);
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
            sql.AppendLine("INSERT INTO price_bid_charges_master (");
            sql.AppendLine("    price_bid_charges_id, charges_type, charges_name,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@PriceBidChargesId{i}, @ChargesType{i}, @ChargesName{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@PriceBidChargesId{i}", row.PriceBidChargesId);
                cmd.Parameters.AddWithValue($"@ChargesType{i}", row.ChargesType);
                cmd.Parameters.AddWithValue($"@ChargesName{i}", row.ChargesName);
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
            public int PB_ChargesID { get; set; }
            public string? ChargesName { get; set; }
            public decimal? ISACTIVE { get; set; }
            public decimal? GSTPercentage { get; set; }
        }

        private class TargetRow
        {
            public int PriceBidChargesId { get; set; }
            public string ChargesType { get; set; } = "Lot Charges"; // NOT NULL
            public string ChargesName { get; set; } = ""; // NOT NULL
        }
    }
}
