using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class TypeOfCategoryMasterMigration : MigrationService
{
    // SQL Server: TBL_TypeOfCategory -> PostgreSQL: type_of_category_master
    protected override string SelectQuery => @"
        SELECT 
            id,
            CategoryType
        FROM TBL_TypeOfCategory";

    protected override string InsertQuery => @"
        INSERT INTO type_of_category_master (
            type_of_category_id,
            type_of_category_name,
            company_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @type_of_category_id,
            @type_of_category_name,
            @company_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public TypeOfCategoryMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // type_of_category_id
            "Direct", // type_of_category_name
            "Fixed: 1"  // company_id
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        Console.WriteLine("🚀 Starting TypeOfCategoryMaster migration...");
        Console.WriteLine($"📋 Executing query...");
        
        // Load all company IDs from company_master so we can insert each type_of_category row for every company
        var companyIds = new List<int>();
        using (var compCmd = new NpgsqlCommand("SELECT company_id FROM company_master", pgConn))
        {
            using var compReader = await compCmd.ExecuteReaderAsync();
            while (await compReader.ReadAsync())
            {
                if (!compReader.IsDBNull(0)) companyIds.Add(compReader.GetInt32(0));
            }
        }

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

            // For each source row, insert one row per company
            foreach (var companyId in companyIds)
            {
                try
                {
                    pgCmd.Parameters.Clear();

                    pgCmd.Parameters.AddWithValue("@type_of_category_id", reader["id"] ?? DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@type_of_category_name", reader["CategoryType"] ?? DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@company_id", companyId);
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
                catch (PostgresException pgEx)
                {
                    skippedCount++;
                    Console.WriteLine($"❌ Warning: failed to insert id {reader["id"]} for company {companyId}: {pgEx.Message}");
                    continue;
                }
                catch (Exception ex)
                {
                    skippedCount++;
                    Console.WriteLine($"❌ Error migrating id {reader["id"]} for company {companyId}: {ex.Message}");
                    continue;
                }
            }
        }

        Console.WriteLine($"\n📊 Migration Summary:");
        Console.WriteLine($"   Total source records read: {totalReadCount}");
        Console.WriteLine($"   ✓ Successfully inserted rows: {insertedCount}");
        Console.WriteLine($"   ❌ Skipped (errors): {skippedCount}");
        
        if (totalReadCount == 0)
        {
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_TypeOfCategory table!");
        }

        return insertedCount;
    }
}
