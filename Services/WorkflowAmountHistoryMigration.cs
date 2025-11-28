using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class WorkflowAmountHistoryMigration : MigrationService
{
    protected override string SelectQuery => "SELECT WorkFlowSubHistoryId, WorkFlowHistoryMainId, WorkFlowSubId, WorkFlowMainId, FromAmount, ToAmount, AssignBuyerID, CreatedBy, CreateDate FROM TBL_WorkFlowSub_History";
    
    protected override string InsertQuery => @"INSERT INTO workflow_amount_history (workflow_amount_history_id, workflow_amount_id, workflow_master_id, from_amount, to_amount, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@workflow_amount_history_id, @workflow_amount_id, @workflow_master_id, @from_amount, @to_amount, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public WorkflowAmountHistoryMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "WorkFlowSubHistoryId -> workflow_amount_history_id (Direct)",
            "WorkFlowSubId -> workflow_amount_id (Direct)",
            "WorkFlowMainId -> workflow_master_id (Direct)",
            "FromAmount -> from_amount (Direct)",
            "ToAmount -> to_amount (Direct)",
            "CreatedBy -> created_by (Direct)",
            "CreateDate -> created_date (Direct)",
            "modified_by -> NULL (Fixed Default)",
            "modified_date -> NULL (Fixed Default)",
            "is_deleted -> false (Fixed Default)",
            "deleted_by -> NULL (Fixed Default)",
            "deleted_date -> NULL (Fixed Default)",
            "Note: WorkFlowHistoryMainId, AssignBuyerID fields from source are not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "WorkFlowSubHistoryId", logic = "WorkFlowSubHistoryId -> workflow_amount_history_id (Direct)", target = "workflow_amount_history_id" },
            new { source = "WorkFlowSubId", logic = "WorkFlowSubId -> workflow_amount_id (Direct)", target = "workflow_amount_id" },
            new { source = "WorkFlowMainId", logic = "WorkFlowMainId -> workflow_master_id (Direct)", target = "workflow_master_id" },
            new { source = "FromAmount", logic = "FromAmount -> from_amount (Direct)", target = "from_amount" },
            new { source = "ToAmount", logic = "ToAmount -> to_amount (Direct)", target = "to_amount" },
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

        int insertedCount = 0;
        int processedCount = 0;
        
        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                try
                {
                    // Create a new command for each record to avoid transaction issues
                    using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
                    if (transaction != null)
                    {
                        pgCmd.Transaction = transaction;
                    }

                    // Map fields
                    var workFlowSubHistoryId = reader.IsDBNull(reader.GetOrdinal("WorkFlowSubHistoryId")) ? 0 : Convert.ToInt32(reader["WorkFlowSubHistoryId"]);
                    var workFlowSubId = reader.IsDBNull(reader.GetOrdinal("WorkFlowSubId")) ? 0 : Convert.ToInt32(reader["WorkFlowSubId"]);
                    var workFlowMainId = reader.IsDBNull(reader.GetOrdinal("WorkFlowMainId")) ? 0 : Convert.ToInt32(reader["WorkFlowMainId"]);
                    var fromAmount = reader.IsDBNull(reader.GetOrdinal("FromAmount")) ? 0 : Convert.ToDecimal(reader["FromAmount"]);
                    var toAmount = reader.IsDBNull(reader.GetOrdinal("ToAmount")) ? 0 : Convert.ToDecimal(reader["ToAmount"]);
                    var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? 0 : Convert.ToInt32(reader["CreatedBy"]);
                    var createDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["CreateDate"]);

                    // Add parameters
                    pgCmd.Parameters.AddWithValue("@workflow_amount_history_id", workFlowSubHistoryId);
                    pgCmd.Parameters.AddWithValue("@workflow_amount_id", workFlowSubId);
                    pgCmd.Parameters.AddWithValue("@workflow_master_id", workFlowMainId);
                    pgCmd.Parameters.AddWithValue("@from_amount", fromAmount);
                    pgCmd.Parameters.AddWithValue("@to_amount", toAmount);
                    pgCmd.Parameters.AddWithValue("@created_by", createdBy);
                    pgCmd.Parameters.AddWithValue("@created_date", createDate);
                    pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@is_deleted", false);
                    pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                    await pgCmd.ExecuteNonQueryAsync();
                    insertedCount++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing record {processedCount}: {ex.Message}");
                    
                    // If using transaction and it's aborted, we need to stop processing
                    if (transaction != null && ex.Message.Contains("current transaction is aborted"))
                    {
                        Console.WriteLine("Transaction is aborted. Rolling back and stopping migration.");
                        throw; // Re-throw to trigger rollback
                    }
                    // Continue with next record for non-transactional operations
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during migration: {ex.Message}");
            throw;
        }

        Console.WriteLine($"WorkflowAmountHistory Migration completed. Processed: {processedCount}, Inserted: {insertedCount}");
        return insertedCount;
    }
}
