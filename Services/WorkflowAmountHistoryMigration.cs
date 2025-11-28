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

    /// <summary>
    /// Migration without transaction for debugging purposes
    /// </summary>
    public async Task<int> MigrateWithoutTransactionAsync()
    {
        return await base.MigrateAsync(useTransaction: false);
    }

    /// <summary>
    /// Validates source data before migration
    /// </summary>
    public async Task<Dictionary<string, object>> ValidateSourceDataAsync()
    {
        var validation = new Dictionary<string, object>();
        
        using var sqlConn = GetSqlServerConnection();
        await sqlConn.OpenAsync();
        
        try
        {
            // Check total record count
            var countQuery = "SELECT COUNT(*) FROM TBL_WorkFlowSub_History";
            using var countCmd = new SqlCommand(countQuery, sqlConn);
            validation["TotalRecords"] = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
            
            // Check for duplicates
            var duplicateQuery = "SELECT COUNT(*) FROM (SELECT WorkFlowSubHistoryId FROM TBL_WorkFlowSub_History GROUP BY WorkFlowSubHistoryId HAVING COUNT(*) > 1) AS Duplicates";
            using var dupCmd = new SqlCommand(duplicateQuery, sqlConn);
            validation["DuplicateIds"] = Convert.ToInt32(await dupCmd.ExecuteScalarAsync());
            
            // Check for invalid IDs
            var invalidIdQuery = "SELECT COUNT(*) FROM TBL_WorkFlowSub_History WHERE WorkFlowSubHistoryId IS NULL OR WorkFlowSubHistoryId <= 0 OR WorkFlowSubId IS NULL OR WorkFlowSubId <= 0 OR WorkFlowMainId IS NULL OR WorkFlowMainId <= 0";
            using var invalidCmd = new SqlCommand(invalidIdQuery, sqlConn);
            validation["InvalidIds"] = Convert.ToInt32(await invalidCmd.ExecuteScalarAsync());
            
            // Check for numeric overflow issues
            var overflowQuery = "SELECT COUNT(*) FROM TBL_WorkFlowSub_History WHERE ABS(FromAmount) >= 1000000000000 OR ABS(ToAmount) >= 1000000000000";
            using var overflowCmd = new SqlCommand(overflowQuery, sqlConn);
            validation["NumericOverflowRecords"] = Convert.ToInt32(await overflowCmd.ExecuteScalarAsync());
            
            // Get sample of problematic records
            var problemQuery = @"SELECT TOP 5 WorkFlowSubHistoryId, WorkFlowSubId, WorkFlowMainId, FromAmount, ToAmount, CreateDate, CreatedBy 
                                FROM TBL_WorkFlowSub_History 
                                WHERE WorkFlowSubHistoryId IS NULL OR WorkFlowSubHistoryId <= 0 OR WorkFlowSubId IS NULL OR WorkFlowSubId <= 0 
                                   OR WorkFlowMainId IS NULL OR WorkFlowMainId <= 0 
                                   OR ABS(FromAmount) >= 1000000000000 OR ABS(ToAmount) >= 1000000000000";
            using var problemCmd = new SqlCommand(problemQuery, sqlConn);
            using var problemReader = await problemCmd.ExecuteReaderAsync();
            
            var problemRecords = new List<object>();
            while (await problemReader.ReadAsync())
            {
                problemRecords.Add(new
                {
                    WorkFlowSubHistoryId = problemReader["WorkFlowSubHistoryId"],
                    WorkFlowSubId = problemReader["WorkFlowSubId"],
                    WorkFlowMainId = problemReader["WorkFlowMainId"],
                    FromAmount = problemReader["FromAmount"],
                    ToAmount = problemReader["ToAmount"],
                    CreateDate = problemReader["CreateDate"],
                    CreatedBy = problemReader["CreatedBy"]
                });
            }
            validation["ProblematicRecords"] = problemRecords;
        }
        catch (Exception ex)
        {
            validation["ValidationError"] = ex.Message;
        }
        
        return validation;
    }

    /// <summary>
    /// Checks for missing foreign key references
    /// </summary>
    public async Task<Dictionary<string, object>> CheckForeignKeyReferencesAsync()
    {
        var result = new Dictionary<string, object>();
        
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        
        try
        {
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();
            
            // Check missing workflow_master references
            var missingMasterQuery = @"
                SELECT DISTINCT s.WorkFlowMainId 
                FROM TBL_WorkFlowSub_History s 
                WHERE s.WorkFlowMainId NOT IN (SELECT WorkFlowId FROM TBL_WorkFlowMain)";
            
            using var missingMasterCmd = new SqlCommand(missingMasterQuery, sqlConn);
            using var missingMasterReader = await missingMasterCmd.ExecuteReaderAsync();
            
            var missingMasterIds = new List<int>();
            while (await missingMasterReader.ReadAsync())
            {
                missingMasterIds.Add(Convert.ToInt32(missingMasterReader["WorkFlowMainId"]));
            }
            result["MissingWorkflowMasterIds"] = missingMasterIds;
            missingMasterReader.Close();
            
            // Check missing workflow_amount references
            var missingAmountQuery = @"
                SELECT DISTINCT s.WorkFlowSubId 
                FROM TBL_WorkFlowSub_History s 
                WHERE s.WorkFlowSubId NOT IN (SELECT WorkFlowSubId FROM TBL_WorkFlowSub)";
            
            using var missingAmountCmd = new SqlCommand(missingAmountQuery, sqlConn);
            using var missingAmountReader = await missingAmountCmd.ExecuteReaderAsync();
            
            var missingAmountIds = new List<int>();
            while (await missingAmountReader.ReadAsync())
            {
                missingAmountIds.Add(Convert.ToInt32(missingAmountReader["WorkFlowSubId"]));
            }
            result["MissingWorkflowAmountIds"] = missingAmountIds;
        }
        catch (Exception ex)
        {
            result["Error"] = ex.Message;
        }
        
        return result;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        int insertedCount = 0;
        int processedCount = 0;
        int skippedCount = 0;
        
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

                    // Map fields with validation
                    var workFlowSubHistoryId = reader.IsDBNull(reader.GetOrdinal("WorkFlowSubHistoryId")) ? 0 : Convert.ToInt32(reader["WorkFlowSubHistoryId"]);
                    var workFlowSubId = reader.IsDBNull(reader.GetOrdinal("WorkFlowSubId")) ? 0 : Convert.ToInt32(reader["WorkFlowSubId"]);
                    var workFlowMainId = reader.IsDBNull(reader.GetOrdinal("WorkFlowMainId")) ? 0 : Convert.ToInt32(reader["WorkFlowMainId"]);
                    var fromAmount = reader.IsDBNull(reader.GetOrdinal("FromAmount")) ? 0 : Convert.ToDecimal(reader["FromAmount"]);
                    var toAmount = reader.IsDBNull(reader.GetOrdinal("ToAmount")) ? 0 : Convert.ToDecimal(reader["ToAmount"]);
                    var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? 0 : Convert.ToInt32(reader["CreatedBy"]);
                    var createDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["CreateDate"]);

                    // Validate required fields
                    if (workFlowSubHistoryId <= 0)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: Invalid WorkFlowSubHistoryId ({workFlowSubHistoryId})");
                        skippedCount++;
                        continue;
                    }
                    
                    if (workFlowSubId <= 0)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: Invalid WorkFlowSubId ({workFlowSubId})");
                        skippedCount++;
                        continue;
                    }
                    
                    if (workFlowMainId <= 0)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: Invalid WorkFlowMainId ({workFlowMainId})");
                        skippedCount++;
                        continue;
                    }

                    // Validate numeric field constraints for PostgreSQL (precision 18, scale 6)
                    // Maximum value is 10^12 - 1 = 999,999,999,999.999999
                    const decimal maxValue = 999999999999.999999m;
                    const decimal minValue = -999999999999.999999m;
                    
                    if (fromAmount > maxValue || fromAmount < minValue)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: FromAmount ({fromAmount}) exceeds PostgreSQL numeric field limits");
                        skippedCount++;
                        continue;
                    }
                    
                    if (toAmount > maxValue || toAmount < minValue)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: ToAmount ({toAmount}) exceeds PostgreSQL numeric field limits");
                        skippedCount++;
                        continue;
                    }

                    // Check if workflow_master_id exists in workflow_master table
                    using var checkMasterCmd = new NpgsqlCommand("SELECT 1 FROM workflow_master WHERE workflow_id = @workflow_id LIMIT 1", pgConn);
                    if (transaction != null)
                    {
                        checkMasterCmd.Transaction = transaction;
                    }
                    checkMasterCmd.Parameters.AddWithValue("@workflow_id", workFlowMainId);
                    
                    var masterExists = await checkMasterCmd.ExecuteScalarAsync();
                    if (masterExists == null)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: WorkFlowMainId ({workFlowMainId}) not found in workflow_master table");
                        skippedCount++;
                        continue;
                    }

                    // Check if workflow_amount_id exists in workflow_amount table
                    using var checkAmountCmd = new NpgsqlCommand("SELECT 1 FROM workflow_amount WHERE workflow_amount_id = @workflow_amount_id LIMIT 1", pgConn);
                    if (transaction != null)
                    {
                        checkAmountCmd.Transaction = transaction;
                    }
                    checkAmountCmd.Parameters.AddWithValue("@workflow_amount_id", workFlowSubId);
                    
                    var amountExists = await checkAmountCmd.ExecuteScalarAsync();
                    if (amountExists == null)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: WorkFlowSubId ({workFlowSubId}) not found in workflow_amount table");
                        skippedCount++;
                        continue;
                    }

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
                    Console.WriteLine($"Record details - WorkFlowSubHistoryId: {(reader.IsDBNull(reader.GetOrdinal("WorkFlowSubHistoryId")) ? "NULL" : reader["WorkFlowSubHistoryId"].ToString())}");
                    
                    // If using transaction, any error aborts the transaction
                    if (transaction != null)
                    {
                        Console.WriteLine("Error occurred in transaction context. Rolling back and stopping migration.");
                        throw; // Re-throw to trigger rollback in the base class
                    }
                    // For non-transactional operations, continue with next record
                    skippedCount++;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during migration: {ex.Message}");
            throw;
        }

        Console.WriteLine($"WorkflowAmountHistory Migration completed. Processed: {processedCount}, Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }
}
