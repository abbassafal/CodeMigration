using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using System.Text;

namespace DataMigration.Services
{
    /// <summary>
    /// Optimized migration for erp_currency_exchange_rate:
    /// - No manual PK generation (let Postgres handle identity)
    /// - Bulk COPY via NpgsqlBinaryImporter into a temp table
    /// - Single MERGE using INSERT ... ON CONFLICT to perform upserts
    /// - Transactional, batched, and tolerant to null/default values
    /// - Normalizes timestamps to seconds to avoid microsecond equality problems
    /// </summary>
    public class ErpCurrencyExchangeRateMigration
    {
        private readonly ILogger<ErpCurrencyExchangeRateMigration> _logger;
        private readonly IConfiguration _configuration;

        public ErpCurrencyExchangeRateMigration(IConfiguration configuration, ILogger<ErpCurrencyExchangeRateMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings() => new List<object>
        {
            new { source = "RecId", target = "erp_currency_exchange_rate_id", logic = "IDENTITY handled by Postgres", type = "serial/identity" },
            new { source = "FromCurrency", target = "from_currency", logic = "default 'USD' if NULL", type = "varchar -> character varying(10)" },
            new { source = "ToCurrency", target = "to_currency", logic = "default 'INR' if NULL", type = "varchar -> character varying(10)" },
            new { source = "ExchangeRate", target = "exchange_rate", logic = "default 1.0 if NULL or 0", type = "decimal -> numeric" },
            new { source = "FromDate", target = "valid_from", logic = "default NOW() if NULL", type = "timestamp with time zone" },
            new { source = "N/A (Generated)", target = "company_id", logic = "each company_id from company_master", type = "FK -> integer" }
        };

        /// <summary>
        /// High-level migration flow:
        /// 1. Read source rows from SQL Server.
        /// 2. Expand rows per valid company into an in-memory list.
        /// 3. Use PostgreSQL temporary table + COPY (binary) to load all rows in bulk.
        /// 4. MERGE temp -> final table with INSERT ... ON CONFLICT (business key) DO UPDATE.
        /// 5. Commit transaction.
        /// </summary>
        public async Task<int> MigrateAsync(CancellationToken cancellationToken = default)
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
                throw new InvalidOperationException("Database connection strings are not configured properly.");

            _logger.LogInformation("Starting optimized ErpCurrencyExchangeRate migration...");

            // Read valid companies
            var validCompanyIds = new List<int>();

            await using (var pgForCompanies = new NpgsqlConnection(pgConnectionString))
            {
                await pgForCompanies.OpenAsync(cancellationToken);
                await using var cmd = new NpgsqlCommand("SELECT company_id FROM company_master WHERE is_deleted = false or is_deleted is null", pgForCompanies);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    validCompanyIds.Add(reader.GetInt32(0));
                }
            }

            if (!validCompanyIds.Any())
            {
                _logger.LogError("No valid companies found; aborting migration.");
                return 0;
            }

            // Read source data once
            var sourceData = new List<(int RecId, string? FromCurrency, string? ToCurrency, decimal? ExchangeRate, DateTime? FromDate)>();
            await using (var sqlConn = new SqlConnection(sqlConnectionString))
            {
                await sqlConn.OpenAsync(cancellationToken);
                var sql = @"SELECT RecId, FromCurrency, ToCurrency, FromDate, ExchangeRate FROM TBL_CurrencyConversionMaster";
                await using var cmd = new SqlCommand(sql, sqlConn);
                await using var rdr = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await rdr.ReadAsync(cancellationToken))
                {
                    sourceData.Add((
                        rdr.GetInt32(0),
                        rdr.IsDBNull(1) ? null : rdr.GetString(1),
                        rdr.IsDBNull(2) ? null : rdr.GetString(2),
                        rdr.IsDBNull(4) ? null : rdr.GetDecimal(4),
                        rdr.IsDBNull(3) ? null : rdr.GetDateTime(3)
                    ));
                }
            }

            if (!sourceData.Any())
            {
                _logger.LogWarning("No source rows found; nothing to migrate.");
                return 0;
            }

            // Prepare flattened rows to bulk-insert
            var flattened = new List<TempRateRow>(capacity: sourceData.Count * validCompanyIds.Count);

            foreach (var src in sourceData)
            {
                var fromCurrency = NormalizeCurrency(src.FromCurrency, "USD");
                var toCurrency = NormalizeCurrency(src.ToCurrency, "INR");
                var rate = src.ExchangeRate.HasValue && src.ExchangeRate.Value != 0m ? src.ExchangeRate.Value : 1.0m;
                var validFrom = NormalizeTimestamp(src.FromDate ?? DateTime.UtcNow);

                foreach (var companyId in validCompanyIds)
                {
                    flattened.Add(new TempRateRow
                    {
                        FromCurrency = fromCurrency,
                        ToCurrency = toCurrency,
                        ValidFrom = validFrom,
                        ExchangeRate = rate,
                        CompanyId = companyId
                    });
                }
            }

            _logger.LogInformation($"Prepared {flattened.Count} rows for bulk load ({sourceData.Count} source × {validCompanyIds.Count} companies)");

            // Bulk load into a temp table using COPY (binary) and then MERGE
            var migratedCount = 0;
            await using (var pgConn = new NpgsqlConnection(pgConnectionString))
            {
                await pgConn.OpenAsync(cancellationToken);

                // Use a transaction to ensure atomicity
                await using var tx = await pgConn.BeginTransactionAsync(cancellationToken);

                // Create temporary table (session-local) - exclude PK (let final table manage identity)
                var createTempSql = @"
                    CREATE TEMP TABLE tmp_erp_rates (
                        from_currency varchar(10) NOT NULL,
                        to_currency varchar(10) NOT NULL,
                        valid_from timestamptz NOT NULL,
                        exchange_rate numeric NOT NULL,
                        company_id integer NOT NULL
                    ) ON COMMIT DROP;";

                await using (var createCmd = new NpgsqlCommand(createTempSql, pgConn, tx))
                {
                    await createCmd.ExecuteNonQueryAsync(cancellationToken);
                }

                // Bulk COPY (binary) into temp table
                // Use NpgsqlBinaryImporter for speed
                await using (var importer = pgConn.BeginBinaryImport("COPY tmp_erp_rates (from_currency, to_currency, valid_from, exchange_rate, company_id) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var row in flattened)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        importer.StartRow();
                        importer.Write(row.FromCurrency, NpgsqlTypes.NpgsqlDbType.Varchar);
                        importer.Write(row.ToCurrency, NpgsqlTypes.NpgsqlDbType.Varchar);
                        importer.Write(row.ValidFrom, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                        importer.Write(row.ExchangeRate, NpgsqlTypes.NpgsqlDbType.Numeric);
                        importer.Write(row.CompanyId, NpgsqlTypes.NpgsqlDbType.Integer);
                    }

                    await importer.CompleteAsync(cancellationToken);
                }

                _logger.LogInformation("Bulk COPY into temporary table completed");

                // Check if unique constraint exists, if not create it
                var checkConstraintSql = @"
                    SELECT COUNT(*)
                    FROM pg_constraint
                    WHERE conname = 'uq_erp_currency_exchange_rate_business_key'
                      AND conrelid = 'erp_currency_exchange_rate'::regclass;";

                int constraintExists = 0;
                await using (var checkCmd = new NpgsqlCommand(checkConstraintSql, pgConn, tx))
                {
                    var result = await checkCmd.ExecuteScalarAsync(cancellationToken);
                    if (result != null && result != DBNull.Value)
                        constraintExists = Convert.ToInt32(result);
                }

                bool canUseBulkMerge = false;

                if (constraintExists == 0)
                {
                    _logger.LogInformation("Creating unique constraint on business key (from_currency, to_currency, valid_from, company_id)...");
                    
                    var createConstraintSql = @"
                        ALTER TABLE erp_currency_exchange_rate 
                        ADD CONSTRAINT uq_erp_currency_exchange_rate_business_key 
                        UNIQUE (from_currency, to_currency, valid_from, company_id);";
                    
                    try
                    {
                        await using var constraintCmd = new NpgsqlCommand(createConstraintSql, pgConn, tx);
                        await constraintCmd.ExecuteNonQueryAsync(cancellationToken);
                        _logger.LogInformation("✓ Unique constraint created successfully");
                        canUseBulkMerge = true;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"⚠️ Could not create constraint (may have duplicates): {ex.Message}");
                        _logger.LogWarning("Will use slower row-by-row upsert instead of bulk ON CONFLICT");
                        canUseBulkMerge = false;
                    }
                }
                else
                {
                    _logger.LogInformation("✓ Unique constraint already exists");
                    canUseBulkMerge = true;
                }

                // If we can't use bulk merge, fall back to row-by-row
                if (!canUseBulkMerge)
                {
                    await DoRowByRowUpsertAsync(pgConn, tx, flattened, cancellationToken);
                    await tx.CommitAsync(cancellationToken);
                    return flattened.Count;
                }

                // Merge temp data into final table with single statement
                var mergeSql = @"
                    INSERT INTO erp_currency_exchange_rate (
                        from_currency, to_currency, valid_from, exchange_rate, company_id, created_date, is_deleted
                    )
                    SELECT from_currency, to_currency, valid_from, exchange_rate, company_id, CURRENT_TIMESTAMP, false
                    FROM tmp_erp_rates
                    ON CONFLICT (from_currency, to_currency, valid_from, company_id)
                    DO UPDATE SET
                        exchange_rate = EXCLUDED.exchange_rate,
                        modified_date = CURRENT_TIMESTAMP;

                    -- return number of rows affected (inserted + updated)
                    SELECT (SELECT COUNT(*) FROM tmp_erp_rates) AS rows_in_tmp;";

                int rowsInTmp = 0;
                await using (var mergeCmd = new NpgsqlCommand(mergeSql, pgConn, tx))
                {
                    // ExecuteScalar will return the final SELECT value (rows_in_tmp)
                    var result = await mergeCmd.ExecuteScalarAsync(cancellationToken);
                    if (result != null && result != DBNull.Value)
                        rowsInTmp = Convert.ToInt32(result);
                }

                await tx.CommitAsync(cancellationToken);

                migratedCount = rowsInTmp;
                _logger.LogInformation($"Merge completed. Rows merged: {migratedCount}");
            }

            return migratedCount;
        }

        private static string NormalizeCurrency(string? input, string defaultVal)
        {
            if (string.IsNullOrWhiteSpace(input))
                return defaultVal;

            var trimmed = input.Trim();
            return trimmed.Length > 10 ? trimmed.Substring(0, 10) : trimmed;
        }

        private static DateTime NormalizeTimestamp(DateTime input)
        {
            // Force UTC and truncate to seconds to avoid microsecond precision mismatches
            var utc = DateTime.SpecifyKind(input, DateTimeKind.Utc);
            return new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, utc.Second, DateTimeKind.Utc);
        }

        /// <summary>
        /// Fallback method: row-by-row upsert when unique constraint doesn't exist
        /// Slower but more tolerant to duplicate data
        /// </summary>
        private async Task DoRowByRowUpsertAsync(
            NpgsqlConnection pgConn, 
            NpgsqlTransaction tx, 
            List<TempRateRow> rows, 
            CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Performing row-by-row upsert for {rows.Count} rows...");
            
            int inserted = 0;
            int updated = 0;
            int skipped = 0;
            
            var checkSql = @"
                SELECT erp_currency_exchange_rate_id 
                FROM erp_currency_exchange_rate 
                WHERE from_currency = @from_currency 
                  AND to_currency = @to_currency 
                  AND valid_from = @valid_from 
                  AND company_id = @company_id 
                LIMIT 1";
            
            var insertSql = @"
                INSERT INTO erp_currency_exchange_rate (
                    from_currency, to_currency, valid_from, exchange_rate, company_id, 
                    created_date, is_deleted
                ) VALUES (
                    @from_currency, @to_currency, @valid_from, @exchange_rate, @company_id,
                    CURRENT_TIMESTAMP, false
                )";
            
            var updateSql = @"
                UPDATE erp_currency_exchange_rate 
                SET exchange_rate = @exchange_rate, 
                    modified_date = CURRENT_TIMESTAMP 
                WHERE erp_currency_exchange_rate_id = @id";
            
            for (int i = 0; i < rows.Count; i++)
            {
                if (i % 100 == 0)
                {
                    _logger.LogInformation($"Progress: {i}/{rows.Count} rows processed (I:{inserted}, U:{updated}, S:{skipped})");
                }
                
                cancellationToken.ThrowIfCancellationRequested();
                
                var row = rows[i];
                
                try
                {
                    // Check if exists
                    int? existingId = null;
                    await using (var checkCmd = new NpgsqlCommand(checkSql, pgConn, tx))
                    {
                        checkCmd.Parameters.AddWithValue("@from_currency", row.FromCurrency);
                        checkCmd.Parameters.AddWithValue("@to_currency", row.ToCurrency);
                        checkCmd.Parameters.AddWithValue("@valid_from", row.ValidFrom);
                        checkCmd.Parameters.AddWithValue("@company_id", row.CompanyId);
                        
                        var result = await checkCmd.ExecuteScalarAsync(cancellationToken);
                        if (result != null && result != DBNull.Value)
                            existingId = Convert.ToInt32(result);
                    }
                    
                    if (existingId.HasValue)
                    {
                        // Update
                        await using var updateCmd = new NpgsqlCommand(updateSql, pgConn, tx);
                        updateCmd.Parameters.AddWithValue("@id", existingId.Value);
                        updateCmd.Parameters.AddWithValue("@exchange_rate", row.ExchangeRate);
                        await updateCmd.ExecuteNonQueryAsync(cancellationToken);
                        updated++;
                    }
                    else
                    {
                        // Insert
                        await using var insertCmd = new NpgsqlCommand(insertSql, pgConn, tx);
                        insertCmd.Parameters.AddWithValue("@from_currency", row.FromCurrency);
                        insertCmd.Parameters.AddWithValue("@to_currency", row.ToCurrency);
                        insertCmd.Parameters.AddWithValue("@valid_from", row.ValidFrom);
                        insertCmd.Parameters.AddWithValue("@exchange_rate", row.ExchangeRate);
                        insertCmd.Parameters.AddWithValue("@company_id", row.CompanyId);
                        await insertCmd.ExecuteNonQueryAsync(cancellationToken);
                        inserted++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Failed to upsert row {i}: {ex.Message}");
                    skipped++;
                }
            }
            
            _logger.LogInformation($"✓ Row-by-row upsert completed. Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}");
        }

        private class TempRateRow
        {
            public string FromCurrency { get; set; } = string.Empty;
            public string ToCurrency { get; set; } = string.Empty;
            public DateTime ValidFrom { get; set; }
            public decimal ExchangeRate { get; set; }
            public int CompanyId { get; set; }
        }
    }
}
