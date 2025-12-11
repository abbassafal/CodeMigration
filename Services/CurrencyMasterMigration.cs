using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class CurrencyMasterMigration : MigrationService
{
    private readonly ILogger<CurrencyMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT CurrencyMastID, Currency_Code, Currency_Name FROM TBL_CURRENCYMASTER";
    protected override string InsertQuery => @"INSERT INTO currency_master (company_id, currency_code, currency_name, currency_short_name, decimal_places, iso_code, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@company_id, @currency_code, @currency_name, @currency_short_name, @decimal_places, @iso_code, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public CurrencyMasterMigration(IConfiguration configuration, ILogger<CurrencyMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "currency_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Auto-generated",   // currency_id (SERIAL)
            "From company_master", // company_id
            "Direct",           // currency_code
            "Direct",           // currency_name
            "Default: null",    // currency_short_name
            "Default: 2",       // decimal_places
            "Default: null",    // iso_code
            "Default: 0",       // created_by
            "Default: Now",     // created_date
            "Default: null",    // modified_by
            "Default: null",    // modified_date
            "Default: false",   // is_deleted
            "Default: null",    // deleted_by
            "Default: null"     // deleted_date
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // Load all company IDs from company_master so we can insert the same currency rows for each company
        var companyIds = new List<int>();
        using (var compCmd = new NpgsqlCommand("SELECT company_id FROM company_master", pgConn, transaction))
        {
            using var compReader = await compCmd.ExecuteReaderAsync();
            while (await compReader.ReadAsync())
            {
                if (!compReader.IsDBNull(0)) companyIds.Add(compReader.GetInt32(0));
            }
        }

        if (companyIds.Count == 0)
        {
            migrationLogger.LogInfo("No companies found in company_master. Please migrate companies first!");
            return 0;
        }

        migrationLogger.LogInfo($"Found {companyIds.Count} companies. Will insert currencies for each company.");

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);

        int currencyCount = 0;
        int insertedCount = 0;
        int skippedCount = 0;
        var skippedRecords = new List<(string RecordId, string Reason)>();

        while (await reader.ReadAsync())
        {
            currencyCount++;
            var currencyCode = reader["Currency_Code"]?.ToString();
            var currencyName = reader["Currency_Name"]?.ToString();
            var currencyMastId = reader["CurrencyMastID"];

            // For each source currency row, insert one row per company
            foreach (var companyId in companyIds)
            {
                var recordId = $"Currency={currencyCode},Company={companyId}";
                
                pgCmd.Parameters.Clear();
                pgCmd.Parameters.AddWithValue("@company_id", companyId);
                pgCmd.Parameters.AddWithValue("@currency_code", currencyCode ?? (object)DBNull.Value);
                pgCmd.Parameters.AddWithValue("@currency_name", currencyName ?? (object)DBNull.Value);
                pgCmd.Parameters.AddWithValue("@currency_short_name", DBNull.Value); // Default: null
                pgCmd.Parameters.AddWithValue("@decimal_places", 2); // Default: 2
                pgCmd.Parameters.AddWithValue("@iso_code", DBNull.Value); // Default: null
                pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
                pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
                pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
                pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
                pgCmd.Parameters.AddWithValue("@is_deleted", false); // Default: false
                pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
                pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null

                try
                {
                    int result = await pgCmd.ExecuteNonQueryAsync();
                    if (result > 0)
                    {
                        migrationLogger.LogInserted(recordId);
                        insertedCount++;
                    }
                }
                catch (Exception ex)
                {
                    migrationLogger.LogError($"Failed to insert currency '{currencyCode}' for company {companyId}: {ex.Message}", recordId, ex);
                    skippedCount++;
                    skippedRecords.Add((recordId, ex.Message));
                    // If we're in a transaction and it's aborted, we need to stop
                    if (transaction != null && ex.Message.Contains("current transaction is aborted"))
                    {
                        migrationLogger.LogError("Transaction aborted. Rolling back all changes.", null, ex);
                        throw; // Re-throw to trigger rollback
                    }
                }
            }

            if (currencyCount % 10 == 0)
            {
                migrationLogger.LogInfo($"Processed {currencyCount} currencies... (Inserted: {insertedCount}, Skipped: {skippedCount})");
            }
        }

        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"Currency Migration Summary: Source currencies: {currencyCount}, Companies: {companyIds.Count}, Inserted: {insertedCount}, Skipped: {skippedCount}, Errors: {summary.TotalErrors}");

        // Export migration stats to Excel
        var excelPath = Path.Combine("migration_outputs", $"CurrencyMasterMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
        MigrationStatsExporter.ExportToExcel(
            excelPath,
            currencyCount * companyIds.Count,
            insertedCount,
            skippedCount,
            _logger,
            skippedRecords
        );
        _logger.LogInformation($"Migration stats exported to {excelPath}");

        return insertedCount;
    }
}