using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Threading;

public class WorkflowHistoryMigration : MigrationService
{
    private const int BATCH_SIZE = 5000; // Increased batch size for better performance
    private const int PROGRESS_UPDATE_INTERVAL = 1000; // Less frequent progress updates
    
    protected override string SelectQuery => @"SELECT 
        WFHistory_ID, Entry, Entry_ID, WorkFlowId, Approvedby, AlternateApprovedBy, 
        ResponseDate, ResponseUser, ResponseRemark, ResponseStatus, EntryStatus, Level, 
        WorkflowSubId, WorkflowSubSubId, EntryLineItem_ID, AssignDate, AssignRemarks, 
        AssignUser, RecallStatus, RecallDate, RecallRemarks, WorkflowRound 
        FROM TBL_WORKFLOW_HISTORY WITH (NOLOCK) 
        ORDER BY WFHistory_ID";
    
    protected override string InsertQuery => @"INSERT INTO workflow_history (workflow_history_id, workflow_for, workflow_for_id, approved_by, action_taken_date, action_taken_user, action_taken_remarks, action_status, workflow_for_status, workflow_level, workflow_master_id, workflow_round, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date, workflow_amount_id, workflow_approval_user_id, plant_id) 
                                             VALUES (@workflow_history_id, @workflow_for, @workflow_for_id, @approved_by, @action_taken_date, @action_taken_user, @action_taken_remarks, @action_status, @workflow_for_status, @workflow_level, @workflow_master_id, @workflow_round, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date, @workflow_amount_id, @workflow_approval_user_id, @plant_id)";

    public WorkflowHistoryMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "WFHistory_ID -> workflow_history_id (Direct)",
            "Entry -> workflow_for (Direct)",
            "Entry_ID -> workflow_for_id (Direct)",
            "Approvedby -> approved_by (Convert to ARRAY with AlternateApprovedBy)",
            "AlternateApprovedBy -> approved_by (Combined with Approvedby in ARRAY)",
            "ResponseDate -> action_taken_date (Direct)",
            "ResponseUser -> action_taken_user (Direct)",
            "ResponseRemark -> action_taken_remarks (Direct)",
            "ResponseStatus -> action_status (Direct)",
            "EntryStatus -> workflow_for_status (Direct)",
            "Level -> workflow_level (Direct)",
            "WorkFlowId -> workflow_master_id (Direct)",
            "WorkflowRound -> workflow_round (Direct)",
            "AssignUser -> created_by (Direct)",
            "AssignDate -> created_date (Direct)",
            "modified_by -> NULL (Fixed Default)",
            "modified_date -> NULL (Fixed Default)",
            "is_deleted -> false (Fixed Default)",
            "deleted_by -> NULL (Fixed Default)",
            "deleted_date -> NULL (Fixed Default)",
            "WorkflowSubId -> workflow_amount_id (Direct, set to 0 if null/invalid)",
            "WorkflowSubSubId -> workflow_approval_user_id (Direct, set to 0 if null/invalid)",
            "plant_id -> NULL (Fixed Default)",
            "Note: EntryLineItem_ID, AssignRemarks, RecallStatus, RecallDate, RecallRemarks fields from source are not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "WFHistory_ID", logic = "WFHistory_ID -> workflow_history_id (Direct)", target = "workflow_history_id" },
            new { source = "Entry", logic = "Entry -> workflow_for (Direct)", target = "workflow_for" },
            new { source = "Entry_ID", logic = "Entry_ID -> workflow_for_id (Direct)", target = "workflow_for_id" },
            new { source = "Approvedby", logic = "Approvedby -> approved_by (Convert to ARRAY with AlternateApprovedBy)", target = "approved_by" },
            new { source = "AlternateApprovedBy", logic = "AlternateApprovedBy -> approved_by (Combined with Approvedby)", target = "approved_by" },
            new { source = "ResponseDate", logic = "ResponseDate -> action_taken_date (Direct)", target = "action_taken_date" },
            new { source = "ResponseUser", logic = "ResponseUser -> action_taken_user (Direct)", target = "action_taken_user" },
            new { source = "ResponseRemark", logic = "ResponseRemark -> action_taken_remarks (Direct)", target = "action_taken_remarks" },
            new { source = "ResponseStatus", logic = "ResponseStatus -> action_status (Direct)", target = "action_status" },
            new { source = "EntryStatus", logic = "EntryStatus -> workflow_for_status (Direct)", target = "workflow_for_status" },
            new { source = "Level", logic = "Level -> workflow_level (Direct)", target = "workflow_level" },
            new { source = "WorkFlowId", logic = "WorkFlowId -> workflow_master_id (Direct)", target = "workflow_master_id" },
            new { source = "WorkflowRound", logic = "WorkflowRound -> workflow_round (Direct)", target = "workflow_round" },
            new { source = "AssignUser", logic = "AssignUser -> created_by (Direct)", target = "created_by" },
            new { source = "AssignDate", logic = "AssignDate -> created_date (Direct)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" },
            new { source = "WorkflowSubId", logic = "WorkflowSubId -> workflow_amount_id (Direct)", target = "workflow_amount_id" },
            new { source = "WorkflowSubSubId", logic = "WorkflowSubSubId -> workflow_approval_user_id (Direct)", target = "workflow_approval_user_id" },
            new { source = "-", logic = "plant_id -> NULL (Fixed Default)", target = "plant_id" }
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
            var countQuery = "SELECT COUNT(*) FROM TBL_WORKFLOW_HISTORY";
            using var countCmd = new SqlCommand(countQuery, sqlConn);
            validation["TotalRecords"] = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
            
            // Check for duplicates
            var duplicateQuery = "SELECT COUNT(*) FROM (SELECT WFHistory_ID FROM TBL_WORKFLOW_HISTORY GROUP BY WFHistory_ID HAVING COUNT(*) > 1) AS Duplicates";
            using var dupCmd = new SqlCommand(duplicateQuery, sqlConn);
            validation["DuplicateIds"] = Convert.ToInt32(await dupCmd.ExecuteScalarAsync());
            
            // Check for invalid IDs
            var invalidIdQuery = "SELECT COUNT(*) FROM TBL_WORKFLOW_HISTORY WHERE WFHistory_ID IS NULL OR WFHistory_ID <= 0";
            using var invalidCmd = new SqlCommand(invalidIdQuery, sqlConn);
            validation["InvalidIds"] = Convert.ToInt32(await invalidCmd.ExecuteScalarAsync());
            
            // Check for orphaned WorkFlowId references
            var orphanQuery = @"SELECT COUNT(*) FROM TBL_WORKFLOW_HISTORY h 
                               LEFT JOIN TBL_WorkFlowMain m ON h.WorkFlowId = m.WorkFlowId 
                               WHERE h.WorkFlowId IS NOT NULL AND h.WorkFlowId > 0 AND m.WorkFlowId IS NULL";
            using var orphanCmd = new SqlCommand(orphanQuery, sqlConn);
            validation["OrphanedWorkflowReferences"] = Convert.ToInt32(await orphanCmd.ExecuteScalarAsync());

            // Check for null WorkflowSubId (workflow_amount_id) values
            var nullAmountQuery = "SELECT COUNT(*) FROM TBL_WORKFLOW_HISTORY WHERE WorkflowSubId IS NULL OR WorkflowSubId <= 0";
            using var nullAmountCmd = new SqlCommand(nullAmountQuery, sqlConn);
            validation["NullWorkflowAmountId"] = Convert.ToInt32(await nullAmountCmd.ExecuteScalarAsync());

            // Check for null WorkflowSubSubId (workflow_approval_user_id) values
            var nullApprovalQuery = "SELECT COUNT(*) FROM TBL_WORKFLOW_HISTORY WHERE WorkflowSubSubId IS NULL OR WorkflowSubSubId <= 0";
            using var nullApprovalCmd = new SqlCommand(nullApprovalQuery, sqlConn);
            validation["NullWorkflowApprovalUserId"] = Convert.ToInt32(await nullApprovalCmd.ExecuteScalarAsync());
            
            // Get sample of problematic records
            var problemQuery = @"SELECT TOP 5 WFHistory_ID, Entry, Entry_ID, WorkFlowId, Approvedby, AlternateApprovedBy, Level, WorkflowSubId, WorkflowSubSubId 
                                FROM TBL_WORKFLOW_HISTORY 
                                WHERE WFHistory_ID IS NULL OR WFHistory_ID <= 0 
                                   OR WorkflowSubId IS NULL OR WorkflowSubId <= 0 
                                   OR WorkflowSubSubId IS NULL OR WorkflowSubSubId <= 0";
            using var problemCmd = new SqlCommand(problemQuery, sqlConn);
            using var problemReader = await problemCmd.ExecuteReaderAsync();
            
            var problemRecords = new List<object>();
            while (await problemReader.ReadAsync())
            {
                problemRecords.Add(new
                {
                    WFHistory_ID = problemReader["WFHistory_ID"],
                    Entry = problemReader["Entry"],
                    Entry_ID = problemReader["Entry_ID"],
                    WorkFlowId = problemReader["WorkFlowId"],
                    Approvedby = problemReader["Approvedby"],
                    AlternateApprovedBy = problemReader["AlternateApprovedBy"],
                    Level = problemReader["Level"],
                    WorkflowSubId = problemReader["WorkflowSubId"],
                    WorkflowSubSubId = problemReader["WorkflowSubSubId"]
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
        var stopwatch = Stopwatch.StartNew();
        var processedCount = 0;
        var insertedCount = 0;
        var skippedCount = 0;
        
        var cancellationToken = new CancellationTokenSource(TimeSpan.FromMinutes(180)).Token; // Increased to 3 hours
        
        try
        {
            using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
            sqlCmd.CommandTimeout = 600; // Increased timeout to 10 minutes
            using var reader = await sqlCmd.ExecuteReaderAsync(cancellationToken);
            
            var batch = new List<WorkflowHistoryRecord>();
            
            while (await reader.ReadAsync(cancellationToken))
            {
                processedCount++;
                
                var record = ProcessRecordFast(reader, processedCount);
                if (record != null)
                {
                    batch.Add(record);
                }
                else
                {
                    skippedCount++;
                }
                
                // Process batch when it reaches BATCH_SIZE
                if (batch.Count >= BATCH_SIZE)
                {
                    var batchInserted = await ProcessBatch(batch, pgConn, transaction);
                    insertedCount += batchInserted;
                    batch.Clear();
                    
                    // Update progress less frequently
                    if (processedCount % (PROGRESS_UPDATE_INTERVAL * 5) == 0)
                    {
                        var elapsed = stopwatch.Elapsed;
                        var rate = processedCount / elapsed.TotalSeconds;
                        Console.WriteLine($"Processed: {processedCount:N0}, Inserted: {insertedCount:N0}, Skipped: {skippedCount:N0}, Rate: {rate:F1}/sec, Elapsed: {elapsed:mm\\:ss}");
                    }
                }
            }
            
            // Process remaining records in batch
            if (batch.Count > 0)
            {
                var batchInserted = await ProcessBatch(batch, pgConn, transaction);
                insertedCount += batchInserted;
            }
            
            stopwatch.Stop();
            var totalRate = insertedCount / stopwatch.Elapsed.TotalSeconds;
            Console.WriteLine($"WorkflowHistory Migration completed. Total time: {stopwatch.Elapsed:mm\\:ss}, Rate: {totalRate:F1}/sec");
            Console.WriteLine($"Final counts - Processed: {processedCount:N0}, Inserted: {insertedCount:N0}, Skipped: {skippedCount:N0}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during migration: {ex.Message}");
            throw;
        }
        
        return insertedCount;
    }

    private WorkflowHistoryRecord? ProcessRecordFast(SqlDataReader reader, int recordNumber)
    {
        // Optimized version - reduced try-catch overhead and faster field access
        var wfHistoryId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0); // WFHistory_ID
        if (wfHistoryId <= 0) return null;

        var entry = reader.IsDBNull(1) ? "" : reader.GetString(1); // Entry
        var entryId = reader.IsDBNull(2) ? 0 : reader.GetInt32(2); // Entry_ID
        var workFlowId = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3); // WorkFlowId
        var approvedBy = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4); // Approvedby
        var alternateApprovedBy = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5); // AlternateApprovedBy
        var responseDate = reader.IsDBNull(6) ? (DateTime?)null : reader.GetDateTime(6); // ResponseDate
        var responseUser = reader.IsDBNull(7) ? (int?)null : reader.GetInt32(7); // ResponseUser
        var responseRemark = reader.IsDBNull(8) ? "" : reader.GetString(8); // ResponseRemark
        var responseStatus = reader.IsDBNull(9) ? "" : reader.GetString(9); // ResponseStatus
        var entryStatus = reader.IsDBNull(10) ? "" : reader.GetString(10); // EntryStatus
        var level = reader.IsDBNull(11) ? (int?)null : reader.GetInt32(11); // Level
        var workflowSubId = reader.IsDBNull(12) ? 0 : reader.GetInt32(12); // WorkflowSubId
        var workflowSubSubId = reader.IsDBNull(13) ? 0 : reader.GetInt32(13); // WorkflowSubSubId
        var assignDate = reader.IsDBNull(15) ? DateTime.UtcNow : reader.GetDateTime(15); // AssignDate
        var assignUser = reader.IsDBNull(17) ? (int?)null : reader.GetInt32(17); // AssignUser
        var workflowRound = reader.IsDBNull(21) ? (int?)null : reader.GetInt32(21); // WorkflowRound

        // Ensure non-null values for required fields
        if (workflowSubId <= 0) workflowSubId = 0;
        if (workflowSubSubId <= 0) workflowSubSubId = 0;

        // Optimized array creation
        int[]? approvedByArray = null;
        if (approvedBy.HasValue && approvedBy.Value > 0)
        {
            if (alternateApprovedBy.HasValue && alternateApprovedBy.Value > 0)
            {
                approvedByArray = new int[] { approvedBy.Value, alternateApprovedBy.Value };
            }
            else
            {
                approvedByArray = new int[] { approvedBy.Value };
            }
        }
        else if (alternateApprovedBy.HasValue && alternateApprovedBy.Value > 0)
        {
            approvedByArray = new int[] { alternateApprovedBy.Value };
        }

        return new WorkflowHistoryRecord
        {
            WorkflowHistoryId = wfHistoryId,
            WorkflowFor = entry,
            WorkflowForId = entryId,
            ApprovedBy = approvedByArray,
            ActionTakenDate = responseDate,
            ActionTakenUser = responseUser,
            ActionTakenRemarks = responseRemark,
            ActionStatus = responseStatus,
            WorkflowForStatus = entryStatus,
            WorkflowLevel = level,
            WorkflowMasterId = workFlowId,
            WorkflowRound = workflowRound,
            CreatedBy = assignUser,
            CreatedDate = assignDate,
            ModifiedBy = null,
            ModifiedDate = null,
            IsDeleted = false,
            DeletedBy = null,
            DeletedDate = null,
            WorkflowAmountId = workflowSubId,
            WorkflowApprovalUserId = workflowSubSubId,
            PlantId = null
        };
    }

    private WorkflowHistoryRecord? ProcessRecord(SqlDataReader reader, int recordNumber)
    {
        try
        {
            // Map fields with validation
            var wfHistoryId = reader.IsDBNull(reader.GetOrdinal("WFHistory_ID")) ? 0 : Convert.ToInt32(reader["WFHistory_ID"]);
            var entry = reader.IsDBNull(reader.GetOrdinal("Entry")) ? "" : reader["Entry"].ToString();
            var entryId = reader.IsDBNull(reader.GetOrdinal("Entry_ID")) ? 0 : Convert.ToInt32(reader["Entry_ID"]);
            var workFlowId = reader.IsDBNull(reader.GetOrdinal("WorkFlowId")) ? (int?)null : Convert.ToInt32(reader["WorkFlowId"]);
            var approvedBy = reader.IsDBNull(reader.GetOrdinal("Approvedby")) ? (int?)null : Convert.ToInt32(reader["Approvedby"]);
            var alternateApprovedBy = reader.IsDBNull(reader.GetOrdinal("AlternateApprovedBy")) ? (int?)null : Convert.ToInt32(reader["AlternateApprovedBy"]);
            var responseDate = reader.IsDBNull(reader.GetOrdinal("ResponseDate")) ? (DateTime?)null : Convert.ToDateTime(reader["ResponseDate"]);
            var responseUser = reader.IsDBNull(reader.GetOrdinal("ResponseUser")) ? (int?)null : Convert.ToInt32(reader["ResponseUser"]);
            var responseRemark = reader.IsDBNull(reader.GetOrdinal("ResponseRemark")) ? "" : reader["ResponseRemark"].ToString();
            var responseStatus = reader.IsDBNull(reader.GetOrdinal("ResponseStatus")) ? "" : reader["ResponseStatus"].ToString();
            var entryStatus = reader.IsDBNull(reader.GetOrdinal("EntryStatus")) ? "" : reader["EntryStatus"].ToString();
            var level = reader.IsDBNull(reader.GetOrdinal("Level")) ? (int?)null : Convert.ToInt32(reader["Level"]);
            var workflowSubId = reader.IsDBNull(reader.GetOrdinal("WorkflowSubId")) ? (int?)null : Convert.ToInt32(reader["WorkflowSubId"]);
            var workflowSubSubId = reader.IsDBNull(reader.GetOrdinal("WorkflowSubSubId")) ? (int?)null : Convert.ToInt32(reader["WorkflowSubSubId"]);
            var assignDate = reader.IsDBNull(reader.GetOrdinal("AssignDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["AssignDate"]);
            var assignUser = reader.IsDBNull(reader.GetOrdinal("AssignUser")) ? (int?)null : Convert.ToInt32(reader["AssignUser"]);
            var workflowRound = reader.IsDBNull(reader.GetOrdinal("WorkflowRound")) ? (int?)null : Convert.ToInt32(reader["WorkflowRound"]);

            // Validate required fields
            if (wfHistoryId <= 0)
            {
                Console.WriteLine($"Skipping record {recordNumber}: Invalid WFHistory_ID ({wfHistoryId})");
                return null;
            }

            // Handle null or zero workflow_amount_id by setting it to 0
            if (!workflowSubId.HasValue || workflowSubId.Value <= 0)
            {
                workflowSubId = 0; // Set to 0 instead of skipping
            }

            // Handle null or zero workflow_approval_user_id by setting it to 0
            if (!workflowSubSubId.HasValue || workflowSubSubId.Value <= 0)
            {
                workflowSubSubId = 0; // Set to 0 instead of null
            }

            // Convert ApprovedBy and AlternateApprovedBy to array format
            var approvedByList = new List<int>();
            if (approvedBy.HasValue && approvedBy.Value > 0)
            {
                approvedByList.Add(approvedBy.Value);
            }
            if (alternateApprovedBy.HasValue && alternateApprovedBy.Value > 0)
            {
                approvedByList.Add(alternateApprovedBy.Value);
            }
            int[]? approvedByArray = approvedByList.Count > 0 ? approvedByList.ToArray() : null;

            return new WorkflowHistoryRecord
            {
                WorkflowHistoryId = wfHistoryId,
                WorkflowFor = entry ?? "",
                WorkflowForId = entryId,
                ApprovedBy = approvedByArray,
                ActionTakenDate = responseDate,
                ActionTakenUser = responseUser,
                ActionTakenRemarks = responseRemark ?? "",
                ActionStatus = responseStatus ?? "",
                WorkflowForStatus = entryStatus ?? "",
                WorkflowLevel = level,
                WorkflowMasterId = workFlowId,
                WorkflowRound = workflowRound,
                CreatedBy = assignUser,
                CreatedDate = assignDate,
                ModifiedBy = null,
                ModifiedDate = null,
                IsDeleted = false,
                DeletedBy = null,
                DeletedDate = null,
                WorkflowAmountId = workflowSubId.Value, // Now guaranteed to be a valid value (0 or positive)
                WorkflowApprovalUserId = workflowSubSubId.Value, // Now guaranteed to be a valid value (0 or positive)
                PlantId = null
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing record {recordNumber}: {ex.Message}");
            return null;
        }
    }

    private async Task<int> ProcessBatch(List<WorkflowHistoryRecord> batch, NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        if (batch.Count == 0) return 0;
        
        try
        {
            // Always use COPY for best performance
            return await ProcessBatchWithCopy(batch, pgConn, transaction);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing batch of {batch.Count} records: {ex.Message}");
            throw;
        }
    }

    private async Task<int> ProcessBatchWithCopy(List<WorkflowHistoryRecord> batch, NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        const string copyCommand = @"COPY workflow_history (
            workflow_history_id, workflow_for, workflow_for_id, approved_by, action_taken_date, action_taken_user, 
            action_taken_remarks, action_status, workflow_for_status, workflow_level, workflow_master_id, 
            workflow_round, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, 
            deleted_date, workflow_amount_id, workflow_approval_user_id, plant_id
        ) FROM STDIN (FORMAT BINARY)";

        using var writer = await pgConn.BeginBinaryImportAsync(copyCommand, cancellationToken: default);

        foreach (var record in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(record.WorkflowHistoryId, NpgsqlDbType.Integer);
            await writer.WriteAsync(record.WorkflowFor, NpgsqlDbType.Text);
            await writer.WriteAsync(record.WorkflowForId, NpgsqlDbType.Integer);
            
            // Optimized array handling
            if (record.ApprovedBy != null)
                await writer.WriteAsync(record.ApprovedBy, NpgsqlDbType.Array | NpgsqlDbType.Integer);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Array | NpgsqlDbType.Integer);
            
            // Optimized null handling
            if (record.ActionTakenDate.HasValue)
                await writer.WriteAsync(record.ActionTakenDate.Value, NpgsqlDbType.Timestamp);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Timestamp);
                
            if (record.ActionTakenUser.HasValue)
                await writer.WriteAsync(record.ActionTakenUser.Value, NpgsqlDbType.Integer);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer);
            
            await writer.WriteAsync(record.ActionTakenRemarks, NpgsqlDbType.Text);
            await writer.WriteAsync(record.ActionStatus, NpgsqlDbType.Text);
            await writer.WriteAsync(record.WorkflowForStatus, NpgsqlDbType.Text);
            
            if (record.WorkflowLevel.HasValue)
                await writer.WriteAsync(record.WorkflowLevel.Value, NpgsqlDbType.Integer);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer);
                
            if (record.WorkflowMasterId.HasValue)
                await writer.WriteAsync(record.WorkflowMasterId.Value, NpgsqlDbType.Integer);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer);
                
            if (record.WorkflowRound.HasValue)
                await writer.WriteAsync(record.WorkflowRound.Value, NpgsqlDbType.Integer);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer);
                
            if (record.CreatedBy.HasValue)
                await writer.WriteAsync(record.CreatedBy.Value, NpgsqlDbType.Integer);
            else
                await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer);
                
            await writer.WriteAsync(record.CreatedDate, NpgsqlDbType.Timestamp);
            await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer); // modified_by
            await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Timestamp); // modified_date
            await writer.WriteAsync(record.IsDeleted, NpgsqlDbType.Boolean);
            await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer); // deleted_by
            await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Timestamp); // deleted_date
            await writer.WriteAsync(record.WorkflowAmountId, NpgsqlDbType.Integer);
            await writer.WriteAsync(record.WorkflowApprovalUserId, NpgsqlDbType.Integer);
            await writer.WriteAsync(DBNull.Value, NpgsqlDbType.Integer); // plant_id
        }

        await writer.CompleteAsync();
        return batch.Count;
    }

    // Data class for batch processing
    private class WorkflowHistoryRecord
    {
        public int WorkflowHistoryId { get; set; }
        public string WorkflowFor { get; set; } = "";
        public int WorkflowForId { get; set; }
        public int[]? ApprovedBy { get; set; }
        public DateTime? ActionTakenDate { get; set; }
        public int? ActionTakenUser { get; set; }
        public string ActionTakenRemarks { get; set; } = "";
        public string ActionStatus { get; set; } = "";
        public string WorkflowForStatus { get; set; } = "";
        public int? WorkflowLevel { get; set; }
        public int? WorkflowMasterId { get; set; }
        public int? WorkflowRound { get; set; }
        public int? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public int? ModifiedBy { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public bool IsDeleted { get; set; }
        public int? DeletedBy { get; set; }
        public DateTime? DeletedDate { get; set; }
        public int WorkflowAmountId { get; set; }
        public int WorkflowApprovalUserId { get; set; }
        public int? PlantId { get; set; }
    }
}