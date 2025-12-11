using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class ARCMainMigration : MigrationService
{
    private readonly ILogger<ARCMainMigration> _logger;
    private MigrationLogger? _migrationLogger;
    private HashSet<int> _validCurrencyIds = new HashSet<int>();
    private HashSet<int> _validUserIds = new HashSet<int>();
    private int _defaultCurrencyId = 1; // Will be loaded from DB

    // SQL Server: TBL_ARCMain -> PostgreSQL: arc_header
    // Using aliases that match PostgreSQL column names
    protected override string SelectQuery => @"
        SELECT 
            m.ARCMainId,
            m.ARCName,
            m.ARCNo,
            m.ARCDescription,
            m.SupplierName,
            m.VendorCode,
            m.VendorQuotationRef,
            m.FromDate,
            m.ToDate,
            m.Remarks,
            m.Status,
            m.VendorId,
            m.TotalARCValue,
            m.ValueTolerance,
            m.QuantityLimitation,
            m.ClientSAPId,
            m.EventId,
            m.UsedTotalValue,
            m.ARCTerms,
            m.CreatedBy,
            m.CreatedDate,
            m.UpdatedBy,
            m.UpdatedDate,
            (SELECT TOP 1 CurrencyId FROM TBL_ARCSub WHERE ARCMainId = m.ARCMainId) AS CurrencyId
        FROM TBL_ARCMain m";

    protected override string InsertQuery => @"
        INSERT INTO arc_header (
            arc_header_id,
            arc_name,
            arc_number,
            arc_description,
            supplier_name,
            supplier_code,
            supplier_quote_ref,
            arc_start_date,
            arc_end_date,
            remarks,
            approval_status,
            supplier_id,
            total_arc_value,
            arc_type,
            tolerance_percentage,
            company_id,
            event_id,
            used_total_value,
            arc_terms,
            currency_id,
            created_by, 
            created_date, 
            modified_by, 
            modified_date, 
            is_deleted, 
            deleted_by, 
            deleted_date
        ) VALUES (
            @arc_header_id,
            @arc_name,
            @arc_number,
            @arc_description,
            @supplier_name,
            @supplier_code,
            @supplier_quote_ref,
            @arc_start_date,
            @arc_end_date,
            @remarks,
            @approval_status,
            @supplier_id,
            @total_arc_value,
            @arc_type,
            @tolerance_percentage,
            @company_id,
            @event_id,
            @used_total_value,
            @arc_terms,
            @currency_id,
            @created_by, 
            @created_date, 
            @modified_by, 
            @modified_date, 
            @is_deleted, 
            @deleted_by, 
            @deleted_date
        )";

    public ARCMainMigration(IConfiguration configuration, ILogger<ARCMainMigration> logger) : base(configuration)
    {
        _logger = logger;
        // ...existing code...
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // arc_header_id
            "Direct", // arc_name
            "Direct", // arc_number
            "Direct", // arc_description
            "Direct", // supplier_name
            "Direct", // supplier_code
            "Direct", // supplier_quote_ref
            "Direct", // arc_start_date
            "Direct", // arc_end_date
            "Direct", // remarks
            "Direct", // approval_status
            "Direct", // supplier_id
            "Direct", // total_arc_value
            "Direct", // arc_type
            "Direct", // tolerance_percentage
            "Direct", // company_id
            "Direct", // event_id
            "Direct", // used_total_value
            "Direct", // arc_terms
            "Lookup: TBL_ARCSub"  // currency_id
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    // Load valid currency IDs at the start of migration
    private async Task LoadValidCurrencyIdsAsync(NpgsqlConnection pgConn)
    {
        _validCurrencyIds.Clear();
        using var cmd = new NpgsqlCommand("SELECT currency_id FROM currency_master WHERE currency_id IS NOT NULL ORDER BY currency_id", pgConn);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _validCurrencyIds.Add(reader.GetInt32(0));
        }
        
        // Set default to first available currency, or try to find INR/USD
        if (_validCurrencyIds.Count > 0)
        {
            // Prefer common currencies: 1 (often USD), 86 (INR if exists)
            if (_validCurrencyIds.Contains(1)) _defaultCurrencyId = 1;
            else if (_validCurrencyIds.Contains(86)) _defaultCurrencyId = 86;
            else _defaultCurrencyId = _validCurrencyIds.First();
        }
        
        Console.WriteLine($"Loaded {_validCurrencyIds.Count} valid currency IDs. Default currency_id: {_defaultCurrencyId}");
    }
    
    // Load valid user IDs at the start of migration
    private async Task LoadValidUserIdsAsync(NpgsqlConnection pgConn)
    {
        _validUserIds.Clear();
        using var cmd = new NpgsqlCommand("SELECT user_id FROM users WHERE user_id IS NOT NULL ORDER BY user_id", pgConn);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _validUserIds.Add(reader.GetInt32(0));
        }
        Console.WriteLine($"Loaded {_validUserIds.Count} valid user IDs from users table");
    }
    
    // Helper to validate user ID
    private object ValidateUserId(int? userId)
    {
        if (userId.HasValue && _validUserIds.Contains(userId.Value))
            return userId.Value;
        return DBNull.Value;
    }
    
    // Helper to validate and fix currency ID
    private int ValidateCurrencyId(int? currencyId)
    {
        if (currencyId.HasValue && _validCurrencyIds.Contains(currencyId.Value))
            return currencyId.Value;
        return _defaultCurrencyId;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "arc_header");
        _migrationLogger.LogInfo("Starting migration");

        Console.WriteLine("🚀 Starting ARCMain migration...");
        
        // Load valid currency IDs and user IDs first
        await LoadValidCurrencyIdsAsync(pgConn);
        await LoadValidUserIdsAsync(pgConn);
        
        Console.WriteLine($"📋 Executing query: {SelectQuery.Substring(0, Math.Min(100, SelectQuery.Length))}...");
        
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        Console.WriteLine($"✓ Query executed. Checking for records...");

        int insertedCount = 0;
        int skippedCount = 0;
        int totalReadCount = 0;
        var skippedRecords = new List<(string RecordId, string Reason)>();

        while (await reader.ReadAsync())
        {
            totalReadCount++;
            var arcMainId = reader["ARCMainId"]?.ToString() ?? "NULL";
            if (totalReadCount == 1)
            {
                Console.WriteLine($"✓ Found records! Processing...");
            }
            
            if (totalReadCount % 10 == 0)
            {
                Console.WriteLine($"📊 Processed {totalReadCount} records so far... (Inserted: {insertedCount}, Skipped: {skippedCount})");
            }
            
            // Use savepoint for each record to allow rollback without aborting entire transaction
            string savepointName = $"sp_{totalReadCount}";
            
            try
            {
                // Create savepoint if we have a parent transaction
                if (transaction != null)
                {
                    await using var savepointCmd = new NpgsqlCommand($"SAVEPOINT {savepointName}", pgConn, transaction);
                    await savepointCmd.ExecuteNonQueryAsync();
                }
                
                using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
                pgCmd.Transaction = transaction;

                var arcMainIdValue = reader["ARCMainId"];
                Console.WriteLine($"Processing ARCMainId: {arcMainIdValue}");

                pgCmd.Parameters.AddWithValue("@arc_header_id", arcMainIdValue ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_name", reader["ARCName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_number", reader["ARCNo"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_description", reader["ARCDescription"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_name", reader["SupplierName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_code", reader["VendorCode"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_quote_ref", reader["VendorQuotationRef"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_start_date", reader["FromDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_end_date", reader["ToDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@remarks", reader["Remarks"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@approval_status", reader["Status"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_id", reader["VendorId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@total_arc_value", reader["TotalARCValue"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_type", reader["QuantityLimitation"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@tolerance_percentage", reader["ValueTolerance"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@event_id", reader["EventId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@used_total_value", reader["UsedTotalValue"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_terms", reader["ARCTerms"] ?? DBNull.Value);
                
                // Validate currency_id - use ValidateCurrencyId to ensure it exists in currency_master
                var currencyId = reader["CurrencyId"];
                int validatedCurrencyId = ValidateCurrencyId(
                    currencyId != DBNull.Value && currencyId != null 
                        ? Convert.ToInt32(currencyId) 
                        : (int?)null
                );
                pgCmd.Parameters.AddWithValue("@currency_id", validatedCurrencyId);
                
                // Validate created_by and modified_by
                var createdBy = reader["CreatedBy"];
                var updatedBy = reader["UpdatedBy"];
                
                pgCmd.Parameters.AddWithValue("@created_by", ValidateUserId(
                    createdBy != DBNull.Value && createdBy != null 
                        ? Convert.ToInt32(createdBy) 
                        : (int?)null
                ));
                pgCmd.Parameters.AddWithValue("@created_date", reader["CreatedDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_by", ValidateUserId(
                    updatedBy != DBNull.Value && updatedBy != null 
                        ? Convert.ToInt32(updatedBy) 
                        : (int?)null
                ));
                pgCmd.Parameters.AddWithValue("@modified_date", reader["UpdatedDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@is_deleted", false);
                pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                Console.WriteLine($"Executing INSERT for ARCMainId: {arcMainIdValue}");
                int result = await pgCmd.ExecuteNonQueryAsync();
                Console.WriteLine($"Insert result for ARCMainId {arcMainIdValue}: {result} row(s) affected");

                // Insert into arc_header_history as well
                var historyInsert = @"
                    INSERT INTO arc_header_history (
                        arc_header_id,
                        arc_name,
                        arc_number,
                        arc_description,
                        supplier_name,
                        supplier_code,
                        supplier_quote_ref,
                        arc_start_date,
                        arc_end_date,
                        remarks,
                        approval_status,
                        supplier_id,
                        total_arc_value,
                        arc_type,
                        tolerance_percentage,
                        company_id,
                        event_id,
                        used_total_value,
                        arc_terms,
                        created_by,
                        created_date,
                        modified_by,
                        modified_date,
                        is_deleted,
                        deleted_by,
                        deleted_date,
                        currency_id
                    ) VALUES (
                        @arc_header_id,
                        @arc_name,
                        @arc_number,
                        @arc_description,
                        @supplier_name,
                        @supplier_code,
                        @supplier_quote_ref,
                        @arc_start_date,
                        @arc_end_date,
                        @remarks,
                        @approval_status,
                        @supplier_id,
                        @total_arc_value,
                        @arc_type,
                        @tolerance_percentage,
                        @company_id,
                        @event_id,
                        @used_total_value,
                        @arc_terms,
                        @created_by,
                        @created_date,
                        @modified_by,
                        @modified_date,
                        @is_deleted,
                        @deleted_by,
                        @deleted_date,
                        @currency_id
                    )";
                using (var historyCmd = new NpgsqlCommand(historyInsert, pgConn, transaction))
                {
                    foreach (NpgsqlParameter param in pgCmd.Parameters)
                    {
                        // arc_header_history does not have arc_header_history_id in insert (auto-increment)
                        if (param.ParameterName != "@arc_header_history_id")
                        {
                            historyCmd.Parameters.AddWithValue(param.ParameterName, param.Value ?? DBNull.Value);
                        }
                    }
                    int historyResult = await historyCmd.ExecuteNonQueryAsync();
                    Console.WriteLine($"Insert into arc_header_history for ARCMainId {arcMainIdValue}: {historyResult} row(s) affected");
                }

                if (result > 0) 
                {
                    insertedCount++;
                    Console.WriteLine($"✓ Successfully inserted ARCMainId: {arcMainIdValue}");
                    
                    // Release savepoint if successful
                    if (transaction != null)
                    {
                        await using var releaseCmd = new NpgsqlCommand($"RELEASE SAVEPOINT {savepointName}", pgConn, transaction);
                        await releaseCmd.ExecuteNonQueryAsync();
                    }
                }
                else
                {
                    Console.WriteLine($"⚠️  Warning: INSERT returned 0 rows for ARCMainId: {arcMainIdValue}");
                }
            }
            catch (Exception ex)
            {
                skippedCount++;
                Console.WriteLine($"❌ Error migrating ARCMainId {arcMainId}: {ex.Message}");
                skippedRecords.Add((arcMainId, ex.Message));
                
                // Rollback to savepoint if we have a transaction
                if (transaction != null)
                {
                    try
                    {
                        await using var rollbackCmd = new NpgsqlCommand($"ROLLBACK TO SAVEPOINT {savepointName}", pgConn, transaction);
                        await rollbackCmd.ExecuteNonQueryAsync();
                        Console.WriteLine($"   Rolled back to savepoint for ARCMainId {arcMainId}");
                    }
                    catch (Exception rollbackEx)
                    {
                        Console.WriteLine($"   Error rolling back savepoint: {rollbackEx.Message}");
                    }
                }
                
                // Continue with next record
            }
        }

        Console.WriteLine($"\n📊 Migration Summary:");
        Console.WriteLine($"   Total records read: {totalReadCount}");
        Console.WriteLine($"   ✓ Successfully inserted: {insertedCount}");
        Console.WriteLine($"   ❌ Skipped (errors): {skippedCount}");
        MigrationStatsExporter.ExportToExcel("arc_main_migration_stats.xlsx", totalReadCount, insertedCount, skippedCount, _logger, skippedRecords);
        
        if (totalReadCount == 0)
        {
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_ARCMain table!");
            Console.WriteLine($"   Please verify:");
            Console.WriteLine($"   1. Table exists in SQL Server database");
            Console.WriteLine($"   2. Table has data: SELECT COUNT(*) FROM TBL_ARCMain");
            Console.WriteLine($"   3. Connection string is correct");
        }
        
        if (skippedCount > 0)
        {
            Console.WriteLine($"\n⚠️  Warning: {skippedCount} records were skipped due to errors.");
        }

        return insertedCount;
    }
}
