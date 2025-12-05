using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class TechnicalApprovalWorkflowMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<TechnicalApprovalWorkflowMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    TechApprovalHistory_Id,
    EVENT_ID,
    ApprovalUserId,
    LevelId AS ApprovalLevel,
    CreatedBy,
    CreatedDate AS AssignDate,
    CreatedDate,
    0 AS PlantID
FROM TBL_TechnicalApproval_History
";

    protected override string InsertQuery => @"
INSERT INTO technical_approval_workflow (
    technical_approval_workflow_id, event_id, user_id, assign_date, approval_level, 
    created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, 
    deleted_date, plant_id
) VALUES (
    @technical_approval_workflow_id, @event_id, @user_id, @assign_date, @approval_level, 
    @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, 
    @deleted_date, @plant_id
)
ON CONFLICT (technical_approval_workflow_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    user_id = EXCLUDED.user_id,
    assign_date = EXCLUDED.assign_date,
    approval_level = EXCLUDED.approval_level,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date,
    plant_id = EXCLUDED.plant_id";

    public TechnicalApprovalWorkflowMigration(IConfiguration configuration, ILogger<TechnicalApprovalWorkflowMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // technical_approval_workflow_id
        "Direct", // event_id
        "Direct", // user_id
        "Direct", // assign_date (from CreatedDate)
        "Direct", // approval_level
        "Direct", // created_by
        "Direct", // created_date
        "Fixed",  // modified_by
        "Fixed",  // modified_date
        "Fixed",  // is_deleted
        "Fixed",  // deleted_by
        "Fixed",  // deleted_date
        "Direct"  // plant_id (selected as 0)
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "TechApprovalHistory_Id", logic = "TechApprovalHistory_Id -> technical_approval_workflow_id (Primary key, autoincrement)", target = "technical_approval_workflow_id" },
            new { source = "EVENT_ID", logic = "EVENT_ID -> event_id (Ref from Event Master table)", target = "event_id" },
            new { source = "ApprovalUserId", logic = "ApprovalUserId -> user_id (Ref from User Master table)", target = "user_id" },
            new { source = "CreatedDate", logic = "CreatedDate -> assign_date (Direct)", target = "assign_date" },
            new { source = "LevelId", logic = "LevelId -> approval_level (Direct)", target = "approval_level" },
            new { source = "CreatedBy", logic = "CreatedBy -> created_by (Direct)", target = "created_by" },
            new { source = "CreatedDate", logic = "CreatedDate -> created_date (Direct)", target = "created_date" },
            new { source = "PlantID", logic = "PlantID -> plant_id (Direct, selected as 0)", target = "plant_id" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" },
            new { source = "-", logic = "plant_id -> 0 (Fixed Default)", target = "plant_id" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting TechnicalApprovalWorkflow migration...");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        // Load valid event IDs and user IDs
        var validEventIds = await LoadValidEventIdsAsync(pgConn, transaction);
        var validUserIds = await LoadValidUserIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validEventIds.Count} valid event IDs and {validUserIds.Count} valid user IDs.");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var techApprovalHistoryId = reader["TechApprovalHistory_Id"] ?? DBNull.Value;
            var eventId = reader["EVENT_ID"] ?? DBNull.Value;
            var approvalUserId = reader["ApprovalUserId"] ?? DBNull.Value;
            var levelId = reader["ApprovalLevel"] ?? reader["LevelId"] ?? DBNull.Value;
            var createdBy = reader["CreatedBy"] ?? DBNull.Value;
            var assignDate = reader["AssignDate"] ?? reader["CreatedDate"] ?? DBNull.Value;
            var createdDate = reader["CreatedDate"] ?? DBNull.Value;
            var plantId = reader["PlantID"] ?? DBNull.Value;

            // Validate required keys
            if (techApprovalHistoryId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping row: TechApprovalHistory_Id is NULL.");
                skippedCount++;
                continue;
            }

            // Validate event_id exists in event_master
            if (eventId != DBNull.Value)
            {
                int eventIdValue = Convert.ToInt32(eventId);
                if (!validEventIds.Contains(eventIdValue))
                {
                    _logger.LogWarning($"Skipping TechApprovalHistory_Id {techApprovalHistoryId}: event_id {eventIdValue} not found in event_master.");
                    skippedCount++;
                    continue;
                }
            }

            // Validate user_id is not NULL (required field)
            if (approvalUserId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping TechApprovalHistory_Id {techApprovalHistoryId}: user_id (ApprovalUserId) is NULL.");
                skippedCount++;
                continue;
            }

            // Validate user_id exists in users table
            int userIdValue = Convert.ToInt32(approvalUserId);
            if (!validUserIds.Contains(userIdValue))
            {
                _logger.LogWarning($"Skipping TechApprovalHistory_Id {techApprovalHistoryId}: user_id (ApprovalUserId) {userIdValue} not found in users.");
                skippedCount++;
                continue;
            }

            var record = new Dictionary<string, object>
            {
                ["technical_approval_workflow_id"] = techApprovalHistoryId,
                ["event_id"] = eventId,
                ["user_id"] = approvalUserId,
                ["assign_date"] = assignDate,
                ["approval_level"] = levelId,
                ["created_by"] = createdBy,
                ["created_date"] = createdDate,
                ["modified_by"] = DBNull.Value,
                ["modified_date"] = DBNull.Value,
                ["is_deleted"] = false,
                ["deleted_by"] = DBNull.Value,
                ["deleted_date"] = DBNull.Value,
                ["plant_id"] = plantId
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Inserting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Inserting final batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
        }

        _logger.LogInformation($"TechnicalApprovalWorkflow migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<HashSet<int>> LoadValidEventIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT event_id FROM event_master";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        
        return validIds;
    }

    private async Task<HashSet<int>> LoadValidUserIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT user_id FROM users";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        
        return validIds;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        // Deduplicate by technical_approval_workflow_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["technical_approval_workflow_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate technical_approval_workflow_id records.");
        }

        var columns = new List<string> {
            "technical_approval_workflow_id", "event_id", "user_id", "assign_date", "approval_level", 
            "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", 
            "deleted_date", "plant_id"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in deduplicatedBatch)
        {
            var valuePlaceholders = new List<string>();
            foreach (var col in columns)
            {
                var paramName = $"@p{paramIndex}";
                valuePlaceholders.Add(paramName);
                parameters.Add(new NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }

        var updateColumns = columns.Where(c => c != "technical_approval_workflow_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO technical_approval_workflow ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (technical_approval_workflow_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
