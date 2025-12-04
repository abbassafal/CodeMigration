using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class POConditionMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT POConditionTypeId, POConditionTypeCode, POConditionTypeDesc, POType, StepNumber, ConditionCounter, ClientSAPId FROM TBL_POConditionTypeMaster";
    protected override string InsertQuery => @"INSERT INTO po_condition_master (po_condition_id, po_condition_code, po_condition_name, po_doc_type_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@po_condition_id, @po_condition_code, @po_condition_name, @po_doc_type_id, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public POConditionMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "POConditionTypeId -> po_condition_id (Direct)",
            "POConditionTypeCode -> po_condition_code (Direct)",
            "POConditionTypeDesc -> po_condition_name (Direct)",
            "POType -> po_doc_type_id (Foreign Key - requires lookup from po_doc_type_master)",
            "ClientSAPId -> company_id (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)",
            "Note: StepNumber, ConditionCounter fields from source are not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "POConditionTypeId", logic = "POConditionTypeId -> po_condition_id (Direct)", target = "po_condition_id" },
            new { source = "POConditionTypeCode", logic = "POConditionTypeCode -> po_condition_code (Direct)", target = "po_condition_code" },
            new { source = "POConditionTypeDesc", logic = "POConditionTypeDesc -> po_condition_name (Direct)", target = "po_condition_name" },
            new { source = "POType", logic = "POType -> po_doc_type_id (Foreign Key Lookup)", target = "po_doc_type_id" },
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
                    var poConditionTypeId = reader.IsDBNull(reader.GetOrdinal("POConditionTypeId")) ? 0 : Convert.ToInt32(reader["POConditionTypeId"]);
                    var poConditionTypeCode = reader.IsDBNull(reader.GetOrdinal("POConditionTypeCode")) ? "" : reader["POConditionTypeCode"].ToString();
                    var poConditionTypeDesc = reader.IsDBNull(reader.GetOrdinal("POConditionTypeDesc")) ? "" : reader["POConditionTypeDesc"].ToString();
                    var poType = reader.IsDBNull(reader.GetOrdinal("POType")) ? "" : reader["POType"].ToString();
                    var clientSAPId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(reader["ClientSAPId"]);

                    // Validate required fields
                    if (reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) || clientSAPId == 0)
                    {
                        Console.WriteLine($"Skipping record {processedCount} (POConditionTypeId: {poConditionTypeId}) - ClientSAPId is null or zero");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(poConditionTypeCode))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (POConditionTypeId: {poConditionTypeId}) - POConditionTypeCode is null or empty");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(poConditionTypeDesc))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (POConditionTypeId: {poConditionTypeId}) - POConditionTypeDesc is null or empty");
                        continue;
                    }


                    pgCmd.Parameters.Clear();
                    pgCmd.Parameters.AddWithValue("@po_condition_id", poConditionTypeId);
                    pgCmd.Parameters.AddWithValue("@po_condition_code", poConditionTypeCode ?? "");
                    pgCmd.Parameters.AddWithValue("@po_condition_name", poConditionTypeDesc ?? "");
                    pgCmd.Parameters.AddWithValue("@po_doc_type_id", poType ?? "");
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
                    throw new Exception($"Error processing record {processedCount} in PO Condition Master Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception readerEx)
        {
            // Check if it's a connection or stream issue
            if (readerEx.Message.Contains("reading from stream") || readerEx.Message.Contains("connection") || readerEx.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during PO Condition Master Migration after processing {processedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {readerEx.Message}", readerEx);
            }
            else if (readerEx.Message.Contains("constraint") || readerEx.Message.Contains("foreign key") || readerEx.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during PO Condition Master Migration at record {processedCount}. " +
                                  $"This could be due to: 1) Duplicate primary keys, " +
                                  $"2) Invalid foreign key references, 3) Invalid data values. Original error: {readerEx.Message}", readerEx);
            }
            else
            {
                throw new Exception($"Unexpected error during PO Condition Master Migration at record {processedCount}: {readerEx.Message}", readerEx);
            }
        }
        
        return insertedCount;
    }
}
