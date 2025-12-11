using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataMigration.Services
{
    public class DatabaseTruncateService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<DatabaseTruncateService> _logger;

        public DatabaseTruncateService(IConfiguration configuration, ILogger<DatabaseTruncateService> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task<(bool Success, string Message, int TablesTruncated, int TotalTables, List<string> Errors)> TruncateAllTablesAsync()
        {
            try
            {
                _logger.LogInformation("Starting database truncation (excluding migration tables)...");

                var pgConnString = _configuration.GetConnectionString("PostgreSql");
                if (string.IsNullOrEmpty(pgConnString))
                {
                    _logger.LogError("PostgreSQL connection string not found in configuration");
                    return (false, "PostgreSQL connection string not found in configuration", 0, 0, new List<string>());
                }

                using var pgConn = new NpgsqlConnection(pgConnString);
                await pgConn.OpenAsync();

                // Get all tables except migration history tables
                var getTablesQuery = @"
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    AND table_name NOT LIKE '__EFMigrationsHistory%'
                    AND table_name NOT LIKE '%migration%'
                    ORDER BY table_name";

                var tablesToTruncate = new List<string>();

                using (var cmd = new NpgsqlCommand(getTablesQuery, pgConn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tablesToTruncate.Add(reader.GetString(0));
                    }
                }

                _logger.LogInformation($"Found {tablesToTruncate.Count} tables to truncate");

                // Disable triggers and foreign key checks temporarily
                using (var disableCmd = new NpgsqlCommand("SET session_replication_role = 'replica';", pgConn))
                {
                    await disableCmd.ExecuteNonQueryAsync();
                }

                int tablesTruncated = 0;
                var errors = new List<string>();

                // Truncate each table
                foreach (var tableName in tablesToTruncate)
                {
                    try
                    {
                        var truncateQuery = $"TRUNCATE TABLE \"{tableName}\" CASCADE";
                        using var truncateCmd = new NpgsqlCommand(truncateQuery, pgConn);
                        await truncateCmd.ExecuteNonQueryAsync();
                        tablesTruncated++;
                        _logger.LogInformation($"Truncated table: {tableName}");
                    }
                    catch (Exception ex)
                    {
                        errors.Add($"{tableName}: {ex.Message}");
                        _logger.LogWarning($"Failed to truncate table {tableName}: {ex.Message}");
                    }
                }

                // Re-enable triggers and foreign key checks
                using (var enableCmd = new NpgsqlCommand("SET session_replication_role = 'origin';", pgConn))
                {
                    await enableCmd.ExecuteNonQueryAsync();
                }

                // Reset sequences
                var resetSequencesQuery = @"
                    SELECT 'SELECT SETVAL(' || quote_literal(quote_ident(sequence_schema) || '.' || quote_ident(sequence_name)) || 
                           ', 1, false);' as reset_query
                    FROM information_schema.sequences
                    WHERE sequence_schema = 'public'";

                using (var seqCmd = new NpgsqlCommand(resetSequencesQuery, pgConn))
                using (var reader = await seqCmd.ExecuteReaderAsync())
                {
                    var resetQueries = new List<string>();
                    while (await reader.ReadAsync())
                    {
                        resetQueries.Add(reader.GetString(0));
                    }

                    await reader.CloseAsync();

                    foreach (var resetQuery in resetQueries)
                    {
                        try
                        {
                            using var resetCmd = new NpgsqlCommand(resetQuery, pgConn);
                            await resetCmd.ExecuteNonQueryAsync();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Failed to reset sequence: {ex.Message}");
                        }
                    }
                }

                await pgConn.CloseAsync();

                var message = $"Database truncation completed. {tablesTruncated} out of {tablesToTruncate.Count} tables truncated successfully.";
                if (errors.Any())
                {
                    message += $" {errors.Count} errors occurred.";
                }

                _logger.LogInformation(message);

                // Export truncation stats to Excel
                var skippedRecords = errors.Select(e => {
                    var parts = e.Split(':', 2);
                    return (RecordId: parts[0], Reason: parts.Length > 1 ? parts[1].Trim() : "Unknown error");
                }).ToList();
                var excelPath = Path.Combine("migration_outputs", $"DatabaseTruncate_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
                MigrationStatsExporter.ExportToExcel(
                    excelPath,
                    tablesToTruncate.Count,
                    tablesTruncated,
                    errors.Count,
                    _logger,
                    skippedRecords
                );
                _logger.LogInformation($"Truncation stats exported to {excelPath}");

                return (true, message, tablesTruncated, tablesToTruncate.Count, errors);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred during database truncation.");
                return (false, ex.Message, 0, 0, new List<string> { ex.ToString() });
            }
        }
    }
}
