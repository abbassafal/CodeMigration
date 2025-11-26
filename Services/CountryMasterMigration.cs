using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class CountryMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT CountryMasterID, Country_NAME, Country_Shname, ISO, ISO3, NumberCode, PhoneCode FROM TBL_COUNTRYMASTER";
    protected override string InsertQuery => @"INSERT INTO country_master (countryid, countryname, countrycode, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@countryid, @countryname, @countrycode, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public CountryMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "CountryMasterID -> countryid (Direct)",
            "Country_NAME -> countryname (Direct)",
            "Country_Shname -> countrycode (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)",
            "Note: ISO, ISO3, NumberCode, PhoneCode fields from source are not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "CountryMasterID", logic = "CountryMasterID -> countryid (Direct)", target = "countryid" },
            new { source = "Country_NAME", logic = "Country_NAME -> countryname (Direct)", target = "countryname" },
            new { source = "Country_Shname", logic = "Country_Shname -> countrycode (Direct)", target = "countrycode" },
            new { source = "-", logic = "created_by -> 0 (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }

        int insertedCount = 0;
        int processedCount = 0;
        
        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                try
                {
                    // Validate field values before processing
                    var countryMasterId = reader.IsDBNull(reader.GetOrdinal("CountryMasterID")) ? 0 : Convert.ToInt32(reader["CountryMasterID"]);
                    var countryName = reader.IsDBNull(reader.GetOrdinal("Country_NAME")) ? "" : reader["Country_NAME"].ToString();
                    var countryCode = reader.IsDBNull(reader.GetOrdinal("Country_Shname")) ? "" : reader["Country_Shname"].ToString();

                    // Validate required fields
                    if (string.IsNullOrWhiteSpace(countryName))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (CountryMasterID: {countryMasterId}) - Country_NAME is null or empty");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(countryCode))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (CountryMasterID: {countryMasterId}) - Country_Shname is null or empty");
                        continue;
                    }

                    pgCmd.Parameters.Clear();
                    pgCmd.Parameters.AddWithValue("@countryid", countryMasterId);
                    pgCmd.Parameters.AddWithValue("@countryname", countryName ?? "");
                    pgCmd.Parameters.AddWithValue("@countrycode", countryCode ?? "");
                    pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
                    pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
                    pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
                    pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
                    pgCmd.Parameters.AddWithValue("@is_deleted", false); // Default: false
                    pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
                    pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null
                    
                    int result = await pgCmd.ExecuteNonQueryAsync();
                    if (result > 0) insertedCount++;
                }
                catch (Exception recordEx)
                {
                    throw new Exception($"Error processing record {processedCount} in Country Master Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception readerEx)
        {
            // Check if it's a connection or stream issue
            if (readerEx.Message.Contains("reading from stream") || readerEx.Message.Contains("connection") || readerEx.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during Country Master Migration after processing {processedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {readerEx.Message}", readerEx);
            }
            else if (readerEx.Message.Contains("constraint") || readerEx.Message.Contains("foreign key") || readerEx.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during Country Master Migration at record {processedCount}. " +
                                  $"This could be due to: 1) Duplicate primary keys, " +
                                  $"2) Invalid data values. Original error: {readerEx.Message}", readerEx);
            }
            else
            {
                throw new Exception($"Unexpected error during Country Master Migration at record {processedCount}: {readerEx.Message}", readerEx);
            }
        }
        
        return insertedCount;
    }
}
