using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class WorkflowMasterHistoryMigration : MigrationService
{
    protected override string SelectQuery => "SELECT WorkFlowHistoryId, WorkFlowId, WorkFlowNo, VersionNo, Entry, Name, FromDate, IsDefault, RecStatus, CreatedBy, CreateDate, DepartmentId, PurchaseGroupId, ClientSAPId, PlantId FROM TBL_WorkFlowMain_History";
    protected override string InsertQuery => @"INSERT INTO workflow_master_history (workflow_history_id, workflow_id, workflow_number, version_number, workflow_for, workflow_name, status, is_default, purchase_group_id, company_id, plant_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@workflow_history_id, @workflow_id, @workflow_number, @version_number, @workflow_for, @workflow_name, @status, @is_default, @purchase_group_id, @company_id, @plant_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public WorkflowMasterHistoryMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "WorkFlowHistoryId -> workflow_history_id (Direct)",
            "WorkFlowId -> workflow_id (Direct)",
            "WorkFlowNo -> workflow_number (Direct)",
            "VersionNo -> version_number (Direct)",
            "Entry -> workflow_for (Direct)",
            "Name -> workflow_name (Direct)",
            "RecStatus -> status (Direct)",
            "IsDefault -> is_default (Convert char to boolean)",
            "PurchaseGroupId -> purchase_group_id (Convert to ARRAY - integer to integer[])",
            "ClientSAPId -> company_id (Direct)",
            "PlantId -> plant_id (Convert to ARRAY - integer to integer[])",
            "CreatedBy -> created_by (Direct)",
            "CreateDate -> created_date (Direct)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)",
            "Note: FromDate, DepartmentId fields from source are not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "WorkFlowHistoryId", logic = "WorkFlowHistoryId -> workflow_history_id (Direct)", target = "workflow_history_id" },
            new { source = "WorkFlowId", logic = "WorkFlowId -> workflow_id (Direct)", target = "workflow_id" },
            new { source = "WorkFlowNo", logic = "WorkFlowNo -> workflow_number (Direct)", target = "workflow_number" },
            new { source = "VersionNo", logic = "VersionNo -> version_number (Direct)", target = "version_number" },
            new { source = "Entry", logic = "Entry -> workflow_for (Direct)", target = "workflow_for" },
            new { source = "Name", logic = "Name -> workflow_name (Direct)", target = "workflow_name" },
            new { source = "RecStatus", logic = "RecStatus -> status (Direct)", target = "status" },
            new { source = "IsDefault", logic = "IsDefault -> is_default (Convert char to boolean)", target = "is_default" },
            new { source = "PurchaseGroupId", logic = "PurchaseGroupId -> purchase_group_id (Convert to integer[])", target = "purchase_group_id" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Direct)", target = "company_id" },
            new { source = "PlantId", logic = "PlantId -> plant_id (Convert to integer[])", target = "plant_id" },
            new { source = "CreatedBy", logic = "CreatedBy -> created_by (Direct)", target = "created_by" },
            new { source = "CreateDate", logic = "CreateDate -> created_date (Direct)", target = "created_date" },
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
                    var workFlowHistoryId = reader.IsDBNull(reader.GetOrdinal("WorkFlowHistoryId")) ? 0 : Convert.ToInt32(reader["WorkFlowHistoryId"]);
                    var workFlowId = reader.IsDBNull(reader.GetOrdinal("WorkFlowId")) ? 0 : Convert.ToInt32(reader["WorkFlowId"]);
                    var workFlowNo = reader.IsDBNull(reader.GetOrdinal("WorkFlowNo")) ? "" : reader["WorkFlowNo"].ToString();
                    var versionNo = reader.IsDBNull(reader.GetOrdinal("VersionNo")) ? "" : reader["VersionNo"].ToString();
                    var entry = reader.IsDBNull(reader.GetOrdinal("Entry")) ? "" : reader["Entry"].ToString();
                    var name = reader.IsDBNull(reader.GetOrdinal("Name")) ? "" : reader["Name"].ToString();
                    var recStatus = reader.IsDBNull(reader.GetOrdinal("RecStatus")) ? "" : reader["RecStatus"].ToString();
                    var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? 0 : Convert.ToInt32(reader["CreatedBy"]);
                    var createDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["CreateDate"]);
                    var isDefaultValue = reader.IsDBNull(reader.GetOrdinal("IsDefault")) ? "" : reader["IsDefault"].ToString();
                    var isDefault = !string.IsNullOrEmpty(isDefaultValue) && (isDefaultValue == "Y" || isDefaultValue == "1" || isDefaultValue.ToUpper() == "TRUE");
                    var purchaseGroupId = reader.IsDBNull(reader.GetOrdinal("PurchaseGroupId")) ? (int?)null : Convert.ToInt32(reader["PurchaseGroupId"]);
                    var clientSAPId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(reader["ClientSAPId"]);
                    var plantId = reader.IsDBNull(reader.GetOrdinal("PlantId")) ? (int?)null : Convert.ToInt32(reader["PlantId"]);

                    // Validate required fields
                    if (reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) || clientSAPId == 0)
                    {
                        Console.WriteLine($"Skipping record {processedCount} (WorkFlowHistoryId: {workFlowHistoryId}) - ClientSAPId is null or zero");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(workFlowNo))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (WorkFlowHistoryId: {workFlowHistoryId}) - WorkFlowNo is null or empty");
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(name))
                    {
                        Console.WriteLine($"Skipping record {processedCount} (WorkFlowHistoryId: {workFlowHistoryId}) - Name is null or empty");
                        continue;
                    }

                    // Convert single integer values to arrays for PostgreSQL ARRAY fields
                    var purchaseGroupIdArray = purchaseGroupId.HasValue ? new int[] { purchaseGroupId.Value } : new int[] { };
                    var plantIdArray = plantId.HasValue ? new int[] { plantId.Value } : new int[] { };

                    pgCmd.Parameters.Clear();
                    pgCmd.Parameters.AddWithValue("@workflow_history_id", workFlowHistoryId);
                    pgCmd.Parameters.AddWithValue("@workflow_id", workFlowId);
                    pgCmd.Parameters.AddWithValue("@workflow_number", workFlowNo ?? "");
                    pgCmd.Parameters.AddWithValue("@version_number", versionNo ?? "");
                    pgCmd.Parameters.AddWithValue("@workflow_for", entry ?? "");
                    pgCmd.Parameters.AddWithValue("@workflow_name", name ?? "");
                    pgCmd.Parameters.AddWithValue("@status", recStatus ?? "");
                    pgCmd.Parameters.AddWithValue("@is_default", isDefault);
                    pgCmd.Parameters.AddWithValue("@purchase_group_id", purchaseGroupIdArray);
                    pgCmd.Parameters.AddWithValue("@company_id", clientSAPId);
                    pgCmd.Parameters.AddWithValue("@plant_id", plantIdArray);
                    pgCmd.Parameters.AddWithValue("@created_by", createdBy);
                    pgCmd.Parameters.AddWithValue("@created_date", createDate);
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
                    throw new Exception($"Error processing record {processedCount} in Workflow Master History Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception readerEx)
        {
            // Check if it's a connection or stream issue
            if (readerEx.Message.Contains("reading from stream") || readerEx.Message.Contains("connection") || readerEx.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during Workflow Master History Migration after processing {processedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {readerEx.Message}", readerEx);
            }
            else if (readerEx.Message.Contains("constraint") || readerEx.Message.Contains("foreign key") || readerEx.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during Workflow Master History Migration at record {processedCount}. " +
                                  $"This could be due to: 1) Duplicate primary keys, " +
                                  $"2) Invalid foreign key references, 3) Invalid data values. Original error: {readerEx.Message}", readerEx);
            }
            else
            {
                throw new Exception($"Unexpected error during Workflow Master History Migration at record {processedCount}: {readerEx.Message}", readerEx);
            }
        }
        
        return insertedCount;
    }
}
