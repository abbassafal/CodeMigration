using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class PODocTypeMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT PODocTypeId, PODocTypeCode, PODocTypeDesc, ClientSAPId FROM TBL_PO_DOC_TYPE";
    protected override string InsertQuery => @"INSERT INTO po_doc_type_master (po_doc_type_id, po_doc_type_code, po_doc_type_name, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@po_doc_type_id, @po_doc_type_code, @po_doc_type_name, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PODocTypeMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "PODocTypeId -> po_doc_type_id (Direct)",
            "PODocTypeCode -> po_doc_type_code (Direct)",
            "PODocTypeDesc -> po_doc_type_name (Direct)",
            "ClientSAPId -> company_id (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PODocTypeId", logic = "PODocTypeId -> po_doc_type_id (Direct)", target = "po_doc_type_id" },
            new { source = "PODocTypeCode", logic = "PODocTypeCode -> po_doc_type_code (Direct)", target = "po_doc_type_code" },
            new { source = "PODocTypeDesc", logic = "PODocTypeDesc -> po_doc_type_name (Direct)", target = "po_doc_type_name" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Direct)", target = "company_id" },
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
                    var poDocTypeId = reader.IsDBNull(reader.GetOrdinal("PODocTypeId")) ? 0 : Convert.ToInt32(reader["PODocTypeId"]);
                    var poDocTypeCode = reader.IsDBNull(reader.GetOrdinal("PODocTypeCode")) ? "" : reader["PODocTypeCode"].ToString();
                    var poDocTypeDesc = reader.IsDBNull(reader.GetOrdinal("PODocTypeDesc")) ? "" : reader["PODocTypeDesc"].ToString();
                    var clientSAPId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(reader["ClientSAPId"]);

                    // Validate required fields
                    if (string.IsNullOrWhiteSpace(poDocTypeCode))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (PODocTypeId: {poDocTypeId}) - PODocTypeCode is null or empty");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(poDocTypeDesc))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (PODocTypeId: {poDocTypeId}) - PODocTypeDesc is null or empty");
                        continue;
                    }

                    pgCmd.Parameters.Clear();
                    pgCmd.Parameters.AddWithValue("@po_doc_type_id", poDocTypeId);
                    pgCmd.Parameters.AddWithValue("@po_doc_type_code", poDocTypeCode ?? "");
                    pgCmd.Parameters.AddWithValue("@po_doc_type_name", poDocTypeDesc ?? "");
                    pgCmd.Parameters.AddWithValue("@company_id", clientSAPId);
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
                    throw new Exception($"Error processing record {processedCount} in PO Document Type Master Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception readerEx)
        {
            // Check if it's a connection or stream issue
            if (readerEx.Message.Contains("reading from stream") || readerEx.Message.Contains("connection") || readerEx.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during PO Document Type Master Migration after processing {processedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {readerEx.Message}", readerEx);
            }
            else if (readerEx.Message.Contains("constraint") || readerEx.Message.Contains("foreign key") || readerEx.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during PO Document Type Master Migration at record {processedCount}. " +
                                  $"This could be due to: 1) Duplicate primary keys, " +
                                  $"2) Invalid data values. Original error: {readerEx.Message}", readerEx);
            }
            else
            {
                throw new Exception($"Unexpected error during PO Document Type Master Migration at record {processedCount}: {readerEx.Message}", readerEx);
            }
        }
        
        return insertedCount;
    }
}
