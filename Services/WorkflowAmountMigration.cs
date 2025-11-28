using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class WorkflowAmountMigration : MigrationService
{
    protected override string SelectQuery => "SELECT WorkFlowSubId, WorkFlowMainId, FromAmount, ToAmount, CreateDate, AssignBuyerID, CreatedBy FROM TBL_WorkFlowSub";
    
    protected override string InsertQuery => @"INSERT INTO workflow_amount (workflow_amount_id, workflow_master_id, from_amount, to_amount, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@workflow_amount_id, @workflow_master_id, @from_amount, @to_amount, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public WorkflowAmountMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
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
            "Note: AssignBuyerID field from source is not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
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
            var countQuery = "SELECT COUNT(*) FROM TBL_WorkFlowSub";
            using var countCmd = new SqlCommand(countQuery, sqlConn);
            validation["TotalRecords"] = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
            
            // Check for duplicates
            var duplicateQuery = "SELECT COUNT(*) FROM (SELECT WorkFlowSubId FROM TBL_WorkFlowSub GROUP BY WorkFlowSubId HAVING COUNT(*) > 1) AS Duplicates";
            using var dupCmd = new SqlCommand(duplicateQuery, sqlConn);
            validation["DuplicateIds"] = Convert.ToInt32(await dupCmd.ExecuteScalarAsync());
            
            // Check for invalid IDs
            var invalidIdQuery = "SELECT COUNT(*) FROM TBL_WorkFlowSub WHERE WorkFlowSubId IS NULL OR WorkFlowSubId <= 0 OR WorkFlowMainId IS NULL OR WorkFlowMainId <= 0";
            using var invalidCmd = new SqlCommand(invalidIdQuery, sqlConn);
            validation["InvalidIds"] = Convert.ToInt32(await invalidCmd.ExecuteScalarAsync());
            
            // Check for orphaned WorkFlowMainId references
            var orphanQuery = @"SELECT COUNT(*) FROM TBL_WorkFlowSub s 
                               LEFT JOIN TBL_WorkFlowMain m ON s.WorkFlowMainId = m.WorkFlowId 
                               WHERE m.WorkFlowId IS NULL";
            using var orphanCmd = new SqlCommand(orphanQuery, sqlConn);
            validation["OrphanedReferences"] = Convert.ToInt32(await orphanCmd.ExecuteScalarAsync());
            
            // Check for numeric overflow issues (PostgreSQL precision 18, scale 6 limits)
            var overflowQuery = @"SELECT COUNT(*) FROM TBL_WorkFlowSub 
                                 WHERE ABS(FromAmount) >= 1000000000000 OR ABS(ToAmount) >= 1000000000000";
            using var overflowCmd = new SqlCommand(overflowQuery, sqlConn);
            validation["NumericOverflowRecords"] = Convert.ToInt32(await overflowCmd.ExecuteScalarAsync());
            
            // Get sample of problematic records
            var problemQuery = @"SELECT TOP 5 WorkFlowSubId, WorkFlowMainId, FromAmount, ToAmount, CreateDate, CreatedBy 
                                FROM TBL_WorkFlowSub 
                                WHERE WorkFlowSubId IS NULL OR WorkFlowSubId <= 0 OR WorkFlowMainId IS NULL OR WorkFlowMainId <= 0
                                   OR ABS(FromAmount) >= 1000000000000 OR ABS(ToAmount) >= 1000000000000";
            using var problemCmd = new SqlCommand(problemQuery, sqlConn);
            using var problemReader = await problemCmd.ExecuteReaderAsync();
            
            var problemRecords = new List<object>();
            while (await problemReader.ReadAsync())
            {
                problemRecords.Add(new
                {
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
                    // Map fields with validation
                    var workFlowSubId = reader.IsDBNull(reader.GetOrdinal("WorkFlowSubId")) ? 0 : Convert.ToInt32(reader["WorkFlowSubId"]);
                    var workFlowMainId = reader.IsDBNull(reader.GetOrdinal("WorkFlowMainId")) ? 0 : Convert.ToInt32(reader["WorkFlowMainId"]);
                    var fromAmount = reader.IsDBNull(reader.GetOrdinal("FromAmount")) ? 0 : Convert.ToDecimal(reader["FromAmount"]);
                    var toAmount = reader.IsDBNull(reader.GetOrdinal("ToAmount")) ? 0 : Convert.ToDecimal(reader["ToAmount"]);
                    var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? 0 : Convert.ToInt32(reader["CreatedBy"]);
                    var createDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["CreateDate"]);

                    // Validate required fields
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
                    using var checkCmd = new NpgsqlCommand("SELECT 1 FROM workflow_master WHERE workflow_id = @workflow_id LIMIT 1", pgConn);
                    if (transaction != null)
                    {
                        checkCmd.Transaction = transaction;
                    }
                    checkCmd.Parameters.AddWithValue("@workflow_id", workFlowMainId);
                    
                    var exists = await checkCmd.ExecuteScalarAsync();
                    if (exists == null)
                    {
                        Console.WriteLine($"Skipping record {processedCount}: workflow_master_id ({workFlowMainId}) does not exist in workflow_master table");
                        skippedCount++;
                        continue;
                    }

                    // Create a new command for each record to avoid transaction issues
                    using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
                    if (transaction != null)
                    {
                        pgCmd.Transaction = transaction;
                    }

                    // Add parameters
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
                    Console.WriteLine($"Record details - WorkFlowSubId: {(reader.IsDBNull(reader.GetOrdinal("WorkFlowSubId")) ? "NULL" : reader["WorkFlowSubId"].ToString())}, WorkFlowMainId: {(reader.IsDBNull(reader.GetOrdinal("WorkFlowMainId")) ? "NULL" : reader["WorkFlowMainId"].ToString())}");
                    
                    skippedCount++;
                    
                    // If using transaction, any error aborts the transaction
                    if (transaction != null)
                    {
                        Console.WriteLine("Error occurred in transaction context. Rolling back and stopping migration.");
                        throw; // Re-throw to trigger rollback in the base class
                    }
                    // For non-transactional operations, continue with next record
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during migration: {ex.Message}");
            throw;
        }

        Console.WriteLine($"WorkflowAmount Migration completed. Processed: {processedCount}, Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    /// <summary>
    /// Gets the actual PostgreSQL table schema for debugging
    /// </summary>
    public async Task<List<object>> GetTargetTableSchemaAsync()
    {
        var schema = new List<object>();
        
        using var pgConn = GetPostgreSqlConnection();
        await pgConn.OpenAsync();
        
        try
        {
            var schemaQuery = @"
                SELECT column_name, data_type, is_nullable, column_default 
                FROM information_schema.columns 
                WHERE table_name = 'workflow_amount' 
                ORDER BY ordinal_position";
            
            using var cmd = new NpgsqlCommand(schemaQuery, pgConn);
            using var reader = await cmd.ExecuteReaderAsync();
            
            while (await reader.ReadAsync())
            {
                schema.Add(new
                {
                    column_name = reader["column_name"].ToString(),
                    data_type = reader["data_type"].ToString(),
                    is_nullable = reader["is_nullable"].ToString(),
                    column_default = reader["column_default"]?.ToString()
                });
            }
        }
        catch (Exception ex)
        {
            schema.Add(new { error = ex.Message });
        }
        
        return schema;
    }

    /// <summary>
    /// Identifies records that will fail foreign key constraints
    /// </summary>
    public async Task<Dictionary<string, object>> ValidateForeignKeyConstraintsAsync()
    {
        var validation = new Dictionary<string, object>();
        
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();
        
        try
        {
            // Get all unique WorkFlowMainId values from source
            var sourceQuery = "SELECT DISTINCT WorkFlowMainId FROM TBL_WorkFlowSub WHERE WorkFlowMainId IS NOT NULL AND WorkFlowMainId > 0";
            using var sourceCmd = new SqlCommand(sourceQuery, sqlConn);
            using var sourceReader = await sourceCmd.ExecuteReaderAsync();
            
            var sourceWorkflowIds = new List<int>();
            while (await sourceReader.ReadAsync())
            {
                sourceWorkflowIds.Add(Convert.ToInt32(sourceReader["WorkFlowMainId"]));
            }
            sourceReader.Close();
            
            validation["SourceWorkflowMasterIds"] = sourceWorkflowIds.Count;
            
            // Check which ones exist in the target workflow_master table
            var existingIds = new List<int>();
            var missingIds = new List<int>();
            
            foreach (var workflowId in sourceWorkflowIds)
            {
                using var checkCmd = new NpgsqlCommand("SELECT 1 FROM workflow_master WHERE workflow_id = @workflow_id LIMIT 1", pgConn);
                checkCmd.Parameters.AddWithValue("@workflow_id", workflowId);
                
                var exists = await checkCmd.ExecuteScalarAsync();
                if (exists != null)
                {
                    existingIds.Add(workflowId);
                }
                else
                {
                    missingIds.Add(workflowId);
                }
            }
            
            validation["ExistingWorkflowMasterIds"] = existingIds;
            validation["MissingWorkflowMasterIds"] = missingIds;
            
            // Get count of records that will be skipped due to missing foreign keys
            var skippedCountQuery = $"SELECT COUNT(*) FROM TBL_WorkFlowSub WHERE WorkFlowMainId IN ({string.Join(",", missingIds.Select(x => x.ToString()))})";
            if (missingIds.Count > 0)
            {
                using var skippedCmd = new SqlCommand(skippedCountQuery, sqlConn);
                validation["RecordsToBeSkipped"] = Convert.ToInt32(await skippedCmd.ExecuteScalarAsync());
            }
            else
            {
                validation["RecordsToBeSkipped"] = 0;
            }
            
            // Get sample of problematic records
            if (missingIds.Count > 0)
            {
                var sampleQuery = $@"SELECT TOP 5 WorkFlowSubId, WorkFlowMainId, FromAmount, ToAmount 
                                    FROM TBL_WorkFlowSub 
                                    WHERE WorkFlowMainId IN ({string.Join(",", missingIds.Take(10).Select(x => x.ToString()))})";
                using var sampleCmd = new SqlCommand(sampleQuery, sqlConn);
                using var sampleReader = await sampleCmd.ExecuteReaderAsync();
                
                var sampleRecords = new List<object>();
                while (await sampleReader.ReadAsync())
                {
                    sampleRecords.Add(new
                    {
                        WorkFlowSubId = sampleReader["WorkFlowSubId"],
                        WorkFlowMainId = sampleReader["WorkFlowMainId"],
                        FromAmount = sampleReader["FromAmount"],
                        ToAmount = sampleReader["ToAmount"]
                    });
                }
                validation["SampleProblematicRecords"] = sampleRecords;
            }
        }
        catch (Exception ex)
        {
            validation["ValidationError"] = ex.Message;
        }
        
        return validation;
    }

    /// <summary>
    /// Analyzes numeric field overflow issues in source data
    /// </summary>
    public async Task<Dictionary<string, object>> AnalyzeNumericFieldIssuesAsync()
    {
        var analysis = new Dictionary<string, object>();
        
        using var sqlConn = GetSqlServerConnection();
        await sqlConn.OpenAsync();
        
        try
        {
            // Check for records with amounts that exceed PostgreSQL limits
            var overflowQuery = @"
                SELECT 
                    COUNT(*) as TotalOverflowRecords,
                    SUM(CASE WHEN ABS(FromAmount) >= 1000000000000 THEN 1 ELSE 0 END) as FromAmountOverflow,
                    SUM(CASE WHEN ABS(ToAmount) >= 1000000000000 THEN 1 ELSE 0 END) as ToAmountOverflow,
                    MAX(FromAmount) as MaxFromAmount,
                    MIN(FromAmount) as MinFromAmount,
                    MAX(ToAmount) as MaxToAmount,
                    MIN(ToAmount) as MinToAmount
                FROM TBL_WorkFlowSub
                WHERE ABS(FromAmount) >= 1000000000000 OR ABS(ToAmount) >= 1000000000000";
            
            using var overflowCmd = new SqlCommand(overflowQuery, sqlConn);
            using var reader = await overflowCmd.ExecuteReaderAsync();
            
            if (await reader.ReadAsync())
            {
                analysis["TotalOverflowRecords"] = reader["TotalOverflowRecords"];
                analysis["FromAmountOverflow"] = reader["FromAmountOverflow"];
                analysis["ToAmountOverflow"] = reader["ToAmountOverflow"];
                analysis["MaxFromAmount"] = reader["MaxFromAmount"];
                analysis["MinFromAmount"] = reader["MinFromAmount"];
                analysis["MaxToAmount"] = reader["MaxToAmount"];
                analysis["MinToAmount"] = reader["MinToAmount"];
            }
            reader.Close();
            
            // Get sample of overflow records
            var sampleQuery = @"SELECT TOP 10 WorkFlowSubId, WorkFlowMainId, FromAmount, ToAmount 
                               FROM TBL_WorkFlowSub 
                               WHERE ABS(FromAmount) >= 1000000000000 OR ABS(ToAmount) >= 1000000000000
                               ORDER BY FromAmount DESC";
            
            using var sampleCmd = new SqlCommand(sampleQuery, sqlConn);
            using var sampleReader = await sampleCmd.ExecuteReaderAsync();
            
            var sampleRecords = new List<object>();
            while (await sampleReader.ReadAsync())
            {
                sampleRecords.Add(new
                {
                    WorkFlowSubId = sampleReader["WorkFlowSubId"],
                    WorkFlowMainId = sampleReader["WorkFlowMainId"],
                    FromAmount = sampleReader["FromAmount"],
                    ToAmount = sampleReader["ToAmount"],
                    FromAmountExceedsLimit = Math.Abs(Convert.ToDecimal(sampleReader["FromAmount"])) >= 1000000000000,
                    ToAmountExceedsLimit = Math.Abs(Convert.ToDecimal(sampleReader["ToAmount"])) >= 1000000000000
                });
            }
            analysis["SampleOverflowRecords"] = sampleRecords;
        }
        catch (Exception ex)
        {
            analysis["AnalysisError"] = ex.Message;
        }
        
        return analysis;
    }
}
