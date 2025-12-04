using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class ErpCurrencyExchangeRateMigration
    {
        private readonly ILogger<ErpCurrencyExchangeRateMigration> _logger;
        private readonly IConfiguration _configuration;

        public ErpCurrencyExchangeRateMigration(IConfiguration configuration, ILogger<ErpCurrencyExchangeRateMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "RecId", target = "erp_currency_exchange_rate_id", type = "int -> integer" },
                new { source = "FromCurrency", target = "from_currency", type = "varchar -> character varying(10)" },
                new { source = "ToCurrency", target = "to_currency", type = "varchar -> character varying(10)" },
                new { source = "ExchangeRate", target = "exchange_rate", type = "decimal -> numeric" },
                new { source = "FromDate", target = "valid_from", type = "datetime -> timestamp" },
                new { source = "N/A", target = "company_id", type = "FK -> integer (default first company_id)" }
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

                _logger.LogInformation("Starting ErpCurrencyExchangeRate migration...");

                // Get valid company_ids from PostgreSQL
                var validCompanyIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT company_id FROM company_master WHERE is_deleted = false", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validCompanyIds.Add(reader.GetInt32(0));
                    }
                }

                if (!validCompanyIds.Any())
                {
                    _logger.LogError("No valid company_ids found in company_master table");
                    return 0;
                }

                // Use the first company_id as default since MSSQL table doesn't have company_id
                var defaultCompanyId = validCompanyIds.First();
                _logger.LogInformation($"Using default company_id: {defaultCompanyId}");

                // Fetch data from SQL Server
                var sourceData = new List<(int RecId, string? FromCurrency, string? ToCurrency, decimal? ExchangeRate, DateTime? FromDate)>();
                
                using (var cmd = new SqlCommand(@"SELECT 
                    RecId,
                    FromCurrency,
                    ToCurrency,
                    ExchangeRate,
                    FromDate
                FROM TBL_CurrencyConversionMaster", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add((
                            reader.GetInt32(0),
                            reader.IsDBNull(1) ? null : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2),
                            reader.IsDBNull(3) ? null : reader.GetDecimal(3),
                            reader.IsDBNull(4) ? null : reader.GetDateTime(4)
                        ));
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Check if record already exists
                        int? existingRecordId = null;
                        using (var checkCmd = new NpgsqlCommand(
                            "SELECT erp_currency_exchange_rate_id FROM erp_currency_exchange_rate WHERE erp_currency_exchange_rate_id = @Id",
                            pgConnection))
                        {
                            checkCmd.Parameters.AddWithValue("@Id", record.RecId);
                            var result = await checkCmd.ExecuteScalarAsync();
                            if (result != null && result != DBNull.Value)
                            {
                                existingRecordId = Convert.ToInt32(result);
                            }
                        }

                        var fromCurrency = GetStringValue(record.FromCurrency, 10);
                        var toCurrency = GetStringValue(record.ToCurrency, 10);
                        var validFrom = GetDateTimeValue(record.FromDate) ?? DateTime.UtcNow;
                        var exchangeRate = record.ExchangeRate ?? 1.0m;

                        if (existingRecordId.HasValue)
                        {
                            // Update existing record
                            using var updateCmd = new NpgsqlCommand(
                                @"UPDATE erp_currency_exchange_rate SET
                                    from_currency = @FromCurrency,
                                    to_currency = @ToCurrency,
                                    valid_from = @ValidFrom,
                                    exchange_rate = @ExchangeRate,
                                    company_id = @CompanyId,
                                    modified_by = NULL,
                                    modified_date = CURRENT_TIMESTAMP
                                WHERE erp_currency_exchange_rate_id = @Id",
                                pgConnection);

                            updateCmd.Parameters.AddWithValue("@Id", record.RecId);
                            updateCmd.Parameters.AddWithValue("@FromCurrency", fromCurrency);
                            updateCmd.Parameters.AddWithValue("@ToCurrency", toCurrency);
                            updateCmd.Parameters.AddWithValue("@ValidFrom", validFrom);
                            updateCmd.Parameters.AddWithValue("@ExchangeRate", exchangeRate);
                            updateCmd.Parameters.AddWithValue("@CompanyId", defaultCompanyId);

                            await updateCmd.ExecuteNonQueryAsync();
                            _logger.LogDebug($"Updated record with RecId: {record.RecId}");
                        }
                        else
                        {
                            // Insert new record
                            using var insertCmd = new NpgsqlCommand(
                                @"INSERT INTO erp_currency_exchange_rate (
                                    erp_currency_exchange_rate_id,
                                    from_currency,
                                    to_currency,
                                    valid_from,
                                    exchange_rate,
                                    company_id,
                                    created_by,
                                    created_date,
                                    modified_by,
                                    modified_date,
                                    is_deleted,
                                    deleted_by,
                                    deleted_date
                                ) VALUES (
                                    @Id,
                                    @FromCurrency,
                                    @ToCurrency,
                                    @ValidFrom,
                                    @ExchangeRate,
                                    @CompanyId,
                                    NULL,
                                    CURRENT_TIMESTAMP,
                                    NULL,
                                    NULL,
                                    false,
                                    NULL,
                                    NULL
                                )",
                                pgConnection);

                            insertCmd.Parameters.AddWithValue("@Id", record.RecId);
                            insertCmd.Parameters.AddWithValue("@FromCurrency", fromCurrency);
                            insertCmd.Parameters.AddWithValue("@ToCurrency", toCurrency);
                            insertCmd.Parameters.AddWithValue("@ValidFrom", validFrom);
                            insertCmd.Parameters.AddWithValue("@ExchangeRate", exchangeRate);
                            insertCmd.Parameters.AddWithValue("@CompanyId", defaultCompanyId);

                            await insertCmd.ExecuteNonQueryAsync();
                            _logger.LogDebug($"Inserted new record with RecId: {record.RecId}");
                        }

                        migratedRecords++;
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"RecId {record.RecId}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                    }
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

        private string GetStringValue(string? value, int maxLength)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            return value.Length > maxLength ? value.Substring(0, maxLength) : value;
        }

        private DateTime? GetDateTimeValue(DateTime? value)
        {
            if (!value.HasValue)
                return null;

            return DateTime.SpecifyKind(value.Value, DateTimeKind.Utc);
        }
    }
}
