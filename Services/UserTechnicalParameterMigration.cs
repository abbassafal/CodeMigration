using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class UserTechnicalParameterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<UserTechnicalParameterMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    TechItemTermId,
    PBID,
    Terms,
    Required,
    EventId
FROM TBL_TechItemTerms
";

    protected override string InsertQuery => @"
INSERT INTO user_technical_parameter (
    user_technical_parameter_id, event_item_id, technical_parameter, 
    technical_parameter_mandatory, event_id, created_by, created_date, 
    modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @user_technical_parameter_id, @event_item_id, @technical_parameter, 
    @technical_parameter_mandatory, @event_id, @created_by, @created_date, 
    @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)
ON CONFLICT (user_technical_parameter_id) DO UPDATE SET
    event_item_id = EXCLUDED.event_item_id,
    technical_parameter = EXCLUDED.technical_parameter,
    technical_parameter_mandatory = EXCLUDED.technical_parameter_mandatory,
    event_id = EXCLUDED.event_id,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    public UserTechnicalParameterMigration(IConfiguration configuration, ILogger<UserTechnicalParameterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // user_technical_parameter_id
        "Direct", // event_item_id
        "Direct", // technical_parameter
        "Direct", // technical_parameter_mandatory
        "Direct", // event_id
        "Fixed",  // created_by
        "Fixed",  // created_date
        "Fixed",  // modified_by
        "Fixed",  // modified_date
        "Fixed",  // is_deleted
        "Fixed",  // deleted_by
        "Fixed"   // deleted_date
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "TechItemTermId", logic = "TechItemTermId -> user_technical_parameter_id (Primary key, autoincrement)", target = "user_technical_parameter_id" },
            new { source = "PBID", logic = "PBID -> event_item_id (Ref from table UserPriceBid)", target = "event_item_id" },
            new { source = "Terms", logic = "Terms -> technical_parameter (Direct)", target = "technical_parameter" },
            new { source = "Required", logic = "Required -> technical_parameter_mandatory (Direct)", target = "technical_parameter_mandatory" },
            new { source = "EventId", logic = "EventId -> event_id (Ref from table event_master)", target = "event_id" },
            new { source = "-", logic = "created_by -> NULL (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
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
        _logger.LogInformation("Starting UserTechnicalParameter migration...");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        // Load valid event_item IDs and event IDs
        var validEventItemIds = await LoadValidEventItemIdsAsync(pgConn, transaction);
        var validEventIds = await LoadValidEventIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validEventItemIds.Count} valid event_item IDs and {validEventIds.Count} valid event IDs.");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var techItemTermId = reader["TechItemTermId"] ?? DBNull.Value;
            var pbId = reader["PBID"] ?? DBNull.Value;
            var terms = reader["Terms"] ?? DBNull.Value;
            var required = reader["Required"] ?? DBNull.Value;
            var eventId = reader["EventId"] ?? DBNull.Value;

            // Validate required keys
            if (techItemTermId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping row: TechItemTermId is NULL.");
                skippedCount++;
                continue;
            }

            // Validate event_item_id (PBID) exists in event_items
            if (pbId != DBNull.Value)
            {
                int eventItemIdValue = Convert.ToInt32(pbId);
                if (!validEventItemIds.Contains(eventItemIdValue))
                {
                    _logger.LogWarning($"Skipping TechItemTermId {techItemTermId}: event_item_id (PBID) {eventItemIdValue} not found in event_items.");
                    skippedCount++;
                    continue;
                }
            }

            // Validate event_id exists in event_master
            if (eventId != DBNull.Value)
            {
                int eventIdValue = Convert.ToInt32(eventId);
                if (!validEventIds.Contains(eventIdValue))
                {
                    _logger.LogWarning($"Skipping TechItemTermId {techItemTermId}: event_id {eventIdValue} not found in event_master.");
                    skippedCount++;
                    continue;
                }
            }

            // Convert Required integer to boolean: 1 = true, 2 = false
            object technicalParameterMandatory = DBNull.Value;
            if (required != DBNull.Value)
            {
                int requiredValue = Convert.ToInt32(required);
                technicalParameterMandatory = requiredValue == 1;
            }

            var record = new Dictionary<string, object>
            {
                ["user_technical_parameter_id"] = techItemTermId,
                ["event_item_id"] = pbId,
                ["technical_parameter"] = terms,
                ["technical_parameter_mandatory"] = technicalParameterMandatory,
                ["event_id"] = eventId,
                ["created_by"] = DBNull.Value,
                ["created_date"] = DBNull.Value,
                ["modified_by"] = DBNull.Value,
                ["modified_date"] = DBNull.Value,
                ["is_deleted"] = false,
                ["deleted_by"] = DBNull.Value,
                ["deleted_date"] = DBNull.Value
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

        _logger.LogInformation($"UserTechnicalParameter migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<HashSet<int>> LoadValidEventItemIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT event_item_id FROM event_items";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        
        return validIds;
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

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        // Deduplicate by user_technical_parameter_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["user_technical_parameter_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate user_technical_parameter_id records.");
        }

        var columns = new List<string> {
            "user_technical_parameter_id", "event_item_id", "technical_parameter", 
            "technical_parameter_mandatory", "event_id", "created_by", "created_date", 
            "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
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

        var updateColumns = columns.Where(c => c != "user_technical_parameter_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO user_technical_parameter ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (user_technical_parameter_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
