using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class PurchaseOrganizationMasterMigration : MigrationService
{
    // SQL Server: TBL_PurchaseOrgMaster -> PostgreSQL: purchase_organization_master
    protected override string SelectQuery => @"
        SELECT 
            PurchaseOrgId,
            PurchaseOrgCode,
            PurchaseOrgDesc,
            ClientSAPId
        FROM TBL_PurchaseOrgMaster";

    protected override string InsertQuery => @"
        INSERT INTO purchase_organization_master (
            purchase_organization_id,
            purchase_organization_code,
            purchase_organization_currency_name,
            company_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @purchase_organization_id,
            @purchase_organization_code,
            @purchase_organization_currency_name,
            @company_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public PurchaseOrganizationMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // purchase_organization_id
            "Direct", // purchase_organization_code
            "Direct", // purchase_organization_name
            "Direct"  // company_id
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        Console.WriteLine("🚀 Starting PurchaseOrganizationMaster migration...");
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

                pgCmd.Parameters.AddWithValue("@purchase_organization_id", reader["PurchaseOrgId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@purchase_organization_code", reader["PurchaseOrgCode"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@purchase_organization_currency_name", reader["PurchaseOrgDesc"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_date", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@is_deleted", false);
                pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                int result = await pgCmd.ExecuteNonQueryAsync();
                if (result > 0) insertedCount++;
            }
            catch (Exception ex)
            {
                skippedCount++;
                Console.WriteLine($"❌ Error migrating PurchaseOrgId {reader["PurchaseOrgId"]}: {ex.Message}");
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
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_PurchaseOrgMaster table!");
        }

        return insertedCount;
    }
}
