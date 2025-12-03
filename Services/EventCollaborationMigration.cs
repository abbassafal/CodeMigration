using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class EventCollaborationMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<EventCollaborationMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    EVENTSELUSERID,
    EVENTID,
    USERID,
    USERTYPE,
    ENTBY,
    ENTDATETIME,
    PARTICIPATESTATUS,
    REGRATECOMMENT,
    TECHNICALSTATUS,
    TCSTATUS,
    PRICEBIDSTATUS,
    RegrateTypeId,
    ISTECHAPPROVE,
    ISSAROGATATEAPPROVE,
    REQUESTBYSAROGATE,
    VendorCurrencyId,
    TechRemarks,
    Score,
    ScoreDocument,
    IS_SCORE_ASSIGN,
    IS_COMMERCIAL_ACCESS,
    IS_TECHNICAL_ACCESS,
    IS_TECHNICAL_APPROVE,
    AssignType,
    AlternativeApproval,
    SarogateApprovedBy,
    SarogateApproveDate,
    SendEmail,
    LastAutoSendMail,
    IsSourceListUser
FROM TBL_EVENTSELECTEDUSER
WHERE USERTYPE != 'Vendor'
";

    protected override string InsertQuery => @"
INSERT INTO event_collaboration (
    event_collaboration_id, event_id, user_id, technical_access, tnc_access, 
    price_bid_access, add_vendor, event_schedule, view_nfa, nfa_approval, 
    event_communication, event_extended, created_by, created_date, modified_by, 
    modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @event_collaboration_id, @event_id, @user_id, @technical_access, @tnc_access, 
    @price_bid_access, @add_vendor, @event_schedule, @view_nfa, @nfa_approval, 
    @event_communication, @event_extended, @created_by, @created_date, @modified_by, 
    @modified_date, @is_deleted, @deleted_by, @deleted_date
)
ON CONFLICT (event_collaboration_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    user_id = EXCLUDED.user_id,
    technical_access = EXCLUDED.technical_access,
    tnc_access = EXCLUDED.tnc_access,
    price_bid_access = EXCLUDED.price_bid_access,
    add_vendor = EXCLUDED.add_vendor,
    event_schedule = EXCLUDED.event_schedule,
    view_nfa = EXCLUDED.view_nfa,
    nfa_approval = EXCLUDED.nfa_approval,
    event_communication = EXCLUDED.event_communication,
    event_extended = EXCLUDED.event_extended,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    public EventCollaborationMigration(IConfiguration configuration, ILogger<EventCollaborationMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // event_collaboration_id
        "Direct", // event_id
        "Direct", // user_id
        "Conditional", // technical_access (Buyer/HOD: true, Technical: true, others: false)
        "Conditional", // tnc_access (Buyer/HOD: true, Technical: true, others: false)
        "Conditional", // price_bid_access (Buyer/HOD: true, Technical: false, others: false)
        "Conditional", // add_vendor (Buyer/HOD: true, Technical: false, others: false)
        "Conditional", // event_schedule (Buyer/HOD: true, Technical: false, others: false)
        "Conditional", // view_nfa (Buyer/HOD: true, Technical: false, others: false)
        "Conditional", // nfa_approval (Buyer/HOD: true, Technical: false, others: false)
        "Conditional", // event_communication (Buyer/HOD: true, Technical: false, others: false)
        "Conditional", // event_extended (Buyer/HOD: true, Technical: false, others: false)
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
            new { source = "EVENTSELUSERID", logic = "EVENTSELUSERID -> event_collaboration_id (Direct)", target = "event_collaboration_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Direct from Event Master Table)", target = "event_id" },
            new { source = "USERID", logic = "USERID -> user_id (Direct from users table)", target = "user_id" },
            new { source = "USERTYPE", logic = "USERTYPE -> technical_access (Conditional: Buyer/HOD=true, Technical=true, others=false)", target = "technical_access" },
            new { source = "USERTYPE", logic = "USERTYPE -> tnc_access (Conditional: Buyer/HOD=true, Technical=true, others=false)", target = "tnc_access" },
            new { source = "USERTYPE", logic = "USERTYPE -> price_bid_access (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "price_bid_access" },
            new { source = "USERTYPE", logic = "USERTYPE -> add_vendor (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "add_vendor" },
            new { source = "USERTYPE", logic = "USERTYPE -> event_schedule (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "event_schedule" },
            new { source = "USERTYPE", logic = "USERTYPE -> view_nfa (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "view_nfa" },
            new { source = "USERTYPE", logic = "USERTYPE -> nfa_approval (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "nfa_approval" },
            new { source = "USERTYPE", logic = "USERTYPE -> event_communication (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "event_communication" },
            new { source = "USERTYPE", logic = "USERTYPE -> event_extended (Conditional: Buyer/HOD=true, Technical=false, others=false)", target = "event_extended" },
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
        _logger.LogInformation("Starting EventCollaboration migration (USERTYPE != 'Vendor')...");
        
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
            var eventSelUserId = reader["EVENTSELUSERID"] ?? DBNull.Value;
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var userId = reader["USERID"] ?? DBNull.Value;
            var userType = reader["USERTYPE"]?.ToString()?.Trim() ?? "";
            
            // Validate required foreign keys
            if (eventId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping EVENTSELUSERID {eventSelUserId}: EVENTID is NULL.");
                skippedCount++;
                continue;
            }

            int eventIdValue = Convert.ToInt32(eventId);
            if (!validEventIds.Contains(eventIdValue))
            {
                _logger.LogWarning($"Skipping EVENTSELUSERID {eventSelUserId}: EVENTID {eventIdValue} not found in event_master.");
                skippedCount++;
                continue;
            }

            if (userId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping EVENTSELUSERID {eventSelUserId}: USERID is NULL.");
                skippedCount++;
                continue;
            }

            int userIdValue = Convert.ToInt32(userId);
            if (!validUserIds.Contains(userIdValue))
            {
                _logger.LogWarning($"Skipping EVENTSELUSERID {eventSelUserId}: USERID {userIdValue} not found in users table.");
                skippedCount++;
                continue;
            }

            // Determine access based on USERTYPE
            bool technicalAccess = false;
            bool tncAccess = false;
            bool priceBidAccess = false;
            bool addVendor = false;
            bool eventSchedule = false;
            bool viewNfa = false;
            bool nfaApproval = false;
            bool eventCommunication = false;
            bool eventExtended = false;

            if (userType.Equals("Buyer", StringComparison.OrdinalIgnoreCase) || 
                userType.Equals("HOD", StringComparison.OrdinalIgnoreCase))
            {
                // Buyer or HOD: All access = true
                technicalAccess = true;
                tncAccess = true;
                priceBidAccess = true;
                addVendor = true;
                eventSchedule = true;
                viewNfa = true;
                nfaApproval = true;
                eventCommunication = true;
                eventExtended = true;
            }
            else if (userType.Equals("Technical", StringComparison.OrdinalIgnoreCase))
            {
                // Technical: Only technical_access and tnc_access = true
                technicalAccess = true;
                tncAccess = true;
                // All others remain false
            }
            // For any other USERTYPE, all remain false

            var record = new Dictionary<string, object>
            {
                ["event_collaboration_id"] = eventSelUserId,
                ["event_id"] = eventId,
                ["user_id"] = userId,
                ["technical_access"] = technicalAccess,
                ["tnc_access"] = tncAccess,
                ["price_bid_access"] = priceBidAccess,
                ["add_vendor"] = addVendor,
                ["event_schedule"] = eventSchedule,
                ["view_nfa"] = viewNfa,
                ["nfa_approval"] = nfaApproval,
                ["event_communication"] = eventCommunication,
                ["event_extended"] = eventExtended,
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

        _logger.LogInformation($"EventCollaboration migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
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

        // Deduplicate by event_collaboration_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["event_collaboration_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate event_collaboration_id records.");
        }

        var columns = new List<string> {
            "event_collaboration_id", "event_id", "user_id", "technical_access", "tnc_access",
            "price_bid_access", "add_vendor", "event_schedule", "view_nfa", "nfa_approval",
            "event_communication", "event_extended", "created_by", "created_date", "modified_by",
            "modified_date", "is_deleted", "deleted_by", "deleted_date"
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

        var updateColumns = columns.Where(c => c != "event_collaboration_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO event_collaboration ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (event_collaboration_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
