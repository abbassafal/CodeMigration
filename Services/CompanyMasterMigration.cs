using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class CompanyMasterMigration : MigrationService
{
    // SQL Server: TBL_CLIENTSAPMASTER -> PostgreSQL: company_master
    private readonly ILogger<CompanyMasterMigration> _logger;
    private MigrationLogger? _migrationLogger;

    public CompanyMasterMigration(IConfiguration configuration, ILogger<CompanyMasterMigration> logger)
        : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override string SelectQuery => @"
        SELECT 
            ClientSAPId,
            ClientSAPCode,
            ClientSAPName,
            SAP,
            PRAllocationLogic,
            Address,
            UploadDocument,
            DocumentName
        FROM TBL_CLIENTSAPMASTER";

    protected override string InsertQuery => @"
        INSERT INTO company_master (
            company_id,
            company_code,
            company_name,
            sap_version,
            pr_allocation_logic,
            address,
            company_logo_url,
            company_logo_name,
            default_currency,
            qty_decimal_places,
            value_decimal_places,
            is_indian_currency,
            date_format,
            rfq_prefix,
            rfq_length,
            auction_prefix,
            auction_length,
            supplier_group_prefix,
            supplier_group_length,
            suppliercode_prefix,
            suppliercode_length,
            workflow_prefix,
            workflow_length,
            workflow_version_length,
            participate_terms,
            nfa_prefix,
            nfano_length,
            po_months_validity,
            time_format,
            site_url
        ) VALUES (
            @company_id,
            @company_code,
            @company_name,
            @sap_version,
            @pr_allocation_logic,
            @address,
            @company_logo_url,
            @company_logo_name,
            @default_currency,
            @qty_decimal_places,
            @value_decimal_places,
            @is_indian_currency,
            @date_format,
            @rfq_prefix,
            @rfq_length,
            @auction_prefix,
            @auction_length,
            @supplier_group_prefix,
            @supplier_group_length,
            @suppliercode_prefix,
            @suppliercode_length,
            @workflow_prefix,
            @workflow_length,
            @workflow_version_length,
            @participate_terms,
            @nfa_prefix,
            @nfano_length,
            @po_months_validity,
            @time_format,
            @site_url
        )";

    public CompanyMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "ClientSAPId -> company_id (Direct)",
            "ClientSAPCode -> company_code (Direct)",
            "ClientSAPName -> company_name (Direct)",
            "SAP -> sap_version (Direct)",
            "PRAllocationLogic -> pr_allocation_logic (Direct)",
            "Address -> address (Direct)",
            "UploadDocument -> company_logo_url (Direct)",
            "DocumentName -> company_logo_name (Direct)",
            "default_currency -> INR (Fixed Default)",
            "qty_decimal_places -> 3 (Fixed Default)",
            "value_decimal_places -> 2 (Fixed Default)",
            "is_indian_currency -> true (Fixed Default)",
            "date_format -> dd/MM/yyyy (Fixed Default)",
            "rfq_prefix -> R (Fixed Default)",
            "rfq_length -> 4 (Fixed Default)",
            "auction_prefix -> A (Fixed Default)",
            "auction_length -> 4 (Fixed Default)",
            "supplier_group_prefix -> WCL (Fixed Default)",
            "supplier_group_length -> 7 (Fixed Default)",
            "suppliercode_prefix -> T (Fixed Default)",
            "suppliercode_length -> 7 (Fixed Default)",
            "workflow_prefix -> WF/ (Fixed Default)",
            "workflow_length -> 5 (Fixed Default)",
            "workflow_version_length -> 3 (Fixed Default)",
            "participate_terms -> empty (Fixed Default)",
            "nfa_prefix -> empty (Fixed Default)",
            "nfano_length -> 6 (Fixed Default)",
            "po_months_validity -> 12 (Fixed Default)",
            "time_format -> hh:mm tt (Fixed Default)",
            "site_url -> empty (Fixed Default)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Direct)", target = "company_id" },
            new { source = "ClientSAPCode", logic = "ClientSAPCode -> company_code (Direct)", target = "company_code" },
            new { source = "ClientSAPName", logic = "ClientSAPName -> company_name (Direct)", target = "company_name" },
            new { source = "SAP", logic = "SAP -> sap_version (Direct)", target = "sap_version" },
            new { source = "PRAllocationLogic", logic = "PRAllocationLogic -> pr_allocation_logic (Direct)", target = "pr_allocation_logic" },
            new { source = "Address", logic = "Address -> address (Direct)", target = "address" },
            new { source = "UploadDocument", logic = "UploadDocument -> company_logo_url (Direct)", target = "company_logo_url" },
            new { source = "DocumentName", logic = "DocumentName -> company_logo_name (Direct)", target = "company_logo_name" },
            new { source = "-", logic = "default_currency -> INR (Fixed Default)", target = "default_currency" },
            new { source = "-", logic = "qty_decimal_places -> 3 (Fixed Default)", target = "qty_decimal_places" },
            new { source = "-", logic = "value_decimal_places -> 2 (Fixed Default)", target = "value_decimal_places" },
            new { source = "-", logic = "is_indian_currency -> true (Fixed Default)", target = "is_indian_currency" },
            new { source = "-", logic = "date_format -> dd/MM/yyyy (Fixed Default)", target = "date_format" },
            new { source = "-", logic = "rfq_prefix -> R (Fixed Default)", target = "rfq_prefix" },
            new { source = "-", logic = "rfq_length -> 4 (Fixed Default)", target = "rfq_length" },
            new { source = "-", logic = "auction_prefix -> A (Fixed Default)", target = "auction_prefix" },
            new { source = "-", logic = "auction_length -> 4 (Fixed Default)", target = "auction_length" },
            new { source = "-", logic = "supplier_group_prefix -> WCL (Fixed Default)", target = "supplier_group_prefix" },
            new { source = "-", logic = "supplier_group_length -> 7 (Fixed Default)", target = "supplier_group_length" },
            new { source = "-", logic = "suppliercode_prefix -> T (Fixed Default)", target = "suppliercode_prefix" },
            new { source = "-", logic = "suppliercode_length -> 7 (Fixed Default)", target = "suppliercode_length" },
            new { source = "-", logic = "workflow_prefix -> WF/ (Fixed Default)", target = "workflow_prefix" },
            new { source = "-", logic = "workflow_length -> 5 (Fixed Default)", target = "workflow_length" },
            new { source = "-", logic = "workflow_version_length -> 3 (Fixed Default)", target = "workflow_version_length" },
            new { source = "-", logic = "participate_terms -> empty (Fixed Default)", target = "participate_terms" },
            new { source = "-", logic = "nfa_prefix -> empty (Fixed Default)", target = "nfa_prefix" },
            new { source = "-", logic = "nfano_length -> 6 (Fixed Default)", target = "nfano_length" },
            new { source = "-", logic = "po_months_validity -> 12 (Fixed Default)", target = "po_months_validity" },
            new { source = "-", logic = "time_format -> hh:mm tt (Fixed Default)", target = "time_format" },
            new { source = "-", logic = "site_url -> empty (Fixed Default)", target = "site_url" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "company_master");
        _migrationLogger.LogInfo("Starting migration");
        _migrationLogger.LogSkipped("Starting migration");

        Console.WriteLine("🚀 Starting CompanyMaster migration...");
        Console.WriteLine($"📋 Executing query...");
        
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        Console.WriteLine($"✓ Query executed. Processing records...");
        
        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }

        int insertedCount = 0;
        int skippedCount = 0;
        int totalReadCount = 0;

        while (await reader.ReadAsync())
        {
            totalReadCount++;
            if (totalReadCount == 1)
            {
                Console.WriteLine($"✓ Found records! Processing...");
            }
            
            if (totalReadCount % 10 == 0)
            {
                Console.WriteLine($"📊 Processed {totalReadCount} records so far... (Inserted: {insertedCount}, Skipped: {skippedCount})");
            }
            
            try
            {
                pgCmd.Parameters.Clear();

                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_code", reader["ClientSAPCode"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_name", reader["ClientSAPName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@sap_version", reader["SAP"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@pr_allocation_logic", reader["PRAllocationLogic"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@address", reader["Address"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_logo_url", reader["UploadDocument"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_logo_name", reader["DocumentName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@default_currency", "INR");
                pgCmd.Parameters.AddWithValue("@qty_decimal_places", 3);
                pgCmd.Parameters.AddWithValue("@value_decimal_places", 2);
                pgCmd.Parameters.AddWithValue("@is_indian_currency", true);
                pgCmd.Parameters.AddWithValue("@date_format", "dd/MM/yyyy");
                pgCmd.Parameters.AddWithValue("@rfq_prefix", "R");
                pgCmd.Parameters.AddWithValue("@rfq_length", 4);
                pgCmd.Parameters.AddWithValue("@auction_prefix", "A");
                pgCmd.Parameters.AddWithValue("@auction_length", 4);
                pgCmd.Parameters.AddWithValue("@supplier_group_prefix", "WCL");
                pgCmd.Parameters.AddWithValue("@supplier_group_length", 7);
                pgCmd.Parameters.AddWithValue("@suppliercode_prefix", "T");
                pgCmd.Parameters.AddWithValue("@suppliercode_length", 7);
                pgCmd.Parameters.AddWithValue("@workflow_prefix", "WF/");
                pgCmd.Parameters.AddWithValue("@workflow_length", 5);
                pgCmd.Parameters.AddWithValue("@workflow_version_length", 3);
                pgCmd.Parameters.AddWithValue("@participate_terms", "");
                pgCmd.Parameters.AddWithValue("@nfa_prefix", "");
                pgCmd.Parameters.AddWithValue("@nfano_length", 6);
                pgCmd.Parameters.AddWithValue("@po_months_validity", 12);
                pgCmd.Parameters.AddWithValue("@time_format", "hh:mm tt");
                pgCmd.Parameters.AddWithValue("@site_url", "");

                int result = await pgCmd.ExecuteNonQueryAsync();
                if (result > 0) insertedCount++;
            }
            catch (Exception ex)
            {
                skippedCount++;
                Console.WriteLine($"❌ Error migrating ClientSAPId {reader["ClientSAPId"]}: {ex.Message}");
                Console.WriteLine($"   Stack Trace: {ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner Exception: {ex.InnerException.Message}");
                }
            }
        }

        Console.WriteLine($"\n📊 Migration Summary:");
        Console.WriteLine($"   Total records read: {totalReadCount}");
        Console.WriteLine($"   ✓ Successfully inserted: {insertedCount}");
        Console.WriteLine($"   ❌ Skipped (errors): {skippedCount}");
        
        if (totalReadCount == 0)
        {
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_CLIENTSAPMASTER table!");
        }

        return insertedCount;
    }
}
