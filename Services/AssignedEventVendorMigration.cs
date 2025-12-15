using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;


public class AssignedEventVendorMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<AssignedEventVendorMigration> _logger;
    private MigrationLogger? _migrationLogger;

    public AssignedEventVendorMigration(IConfiguration configuration, ILogger<AssignedEventVendorMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override string SelectQuery => @"
SELECT DISTINCT
    TBL_EVENTSELECTEDUSER.EVENTSELUSERID,
    TBL_EVENTSELECTEDUSER.EVENTID,
    TBL_EVENTSELECTEDUSER.USERID,
    TBL_EVENTSELECTEDUSER.USERTYPE,
    TBL_EVENTSELECTEDUSER.ENTBY,
    TBL_EVENTSELECTEDUSER.ENTDATETIME,
    TBL_EVENTSELECTEDUSER.PARTICIPATESTATUS,
    TBL_EVENTSELECTEDUSER.REGRATECOMMENT,
    TBL_EVENTSELECTEDUSER.TECHNICALSTATUS,
    TBL_EVENTSELECTEDUSER.TCSTATUS,
    TBL_EVENTSELECTEDUSER.PRICEBIDSTATUS,
    TBL_EVENTSELECTEDUSER.RegrateTypeId,
    TBL_EVENTSELECTEDUSER.ISTECHAPPROVE,
    TBL_EVENTSELECTEDUSER.ISSAROGATEAPPROVE,
    TBL_EVENTSELECTEDUSER.REQUESTBYSAROGATE,
    TBL_EVENTSELECTEDUSER.VendorCurrencyId,
    TBL_EVENTSELECTEDUSER.TechRemarks,
    TBL_EVENTSELECTEDUSER.Score,
    TBL_EVENTSELECTEDUSER.ScoreDocument,
    TBL_EVENTSELECTEDUSER.IS_SCORE_ASSIGN,
    TBL_EVENTSELECTEDUSER.IS_COMMERCIAL_ACCESS,
    TBL_EVENTSELECTEDUSER.IS_TECHNICAL_ACCESS,
    TBL_EVENTSELECTEDUSER.IS_TECHNICAL_APPROVE,
    TBL_EVENTSELECTEDUSER.AssignType,
    TBL_EVENTSELECTEDUSER.AlternativeApproval,
    TBL_EVENTSELECTEDUSER.SarogateApprovedBy,
    TBL_EVENTSELECTEDUSER.SarogateApproveDate,
    TBL_EVENTSELECTEDUSER.SendEmail,
    TBL_EVENTSELECTEDUSER.LastAutoSendMail,
    TBL_EVENTSELECTEDUSER.IsSourceListUser,
    TBL_PB_SUPPLIER.BifurcationDone,
    TBL_PB_SUPPLIER.VendorName
FROM TBL_EVENTSELECTEDUSER
LEFT JOIN TBL_PB_SUPPLIER 
    ON TBL_PB_SUPPLIER.EVENTID = TBL_EVENTSELECTEDUSER.EVENTID 
    AND TBL_EVENTSELECTEDUSER.USERID = TBL_PB_SUPPLIER.SUPPLIER_ID 
    AND TBL_EVENTSELECTEDUSER.USERTYPE = 'VENDOR'
    AND TBL_PB_SUPPLIER.SEQUENCEID > 0
WHERE TBL_EVENTSELECTEDUSER.USERTYPE = 'Vendor'
";

    protected override string InsertQuery => @"
INSERT INTO assigned_event_vendor (
    assigned_event_vendor_id, event_id, supplier_id, supplier_participation_status, 
    supplier_event_regrate_remark, supplier_price_bid_status, supplier_price_bid_currency_id, 
    supplier_email_address, supplier_source_status, lot_auctionbifurcation_flag, 
    partner_vendor_name, created_by, created_date, modified_by, modified_date, 
    is_deleted, deleted_by, deleted_date
) VALUES (
    @assigned_event_vendor_id, @event_id, @supplier_id, @supplier_participation_status, 
    @supplier_event_regrate_remark, @supplier_price_bid_status, @supplier_price_bid_currency_id, 
    @supplier_email_address, @supplier_source_status, @lot_auctionbifurcation_flag, 
    @partner_vendor_name, @created_by, @created_date, @modified_by, @modified_date, 
    @is_deleted, @deleted_by, @deleted_date
)
ON CONFLICT (assigned_event_vendor_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    supplier_id = EXCLUDED.supplier_id,
    supplier_participation_status = EXCLUDED.supplier_participation_status,
    supplier_event_regrate_remark = EXCLUDED.supplier_event_regrate_remark,
    supplier_price_bid_status = EXCLUDED.supplier_price_bid_status,
    supplier_price_bid_currency_id = EXCLUDED.supplier_price_bid_currency_id,
    supplier_email_address = EXCLUDED.supplier_email_address,
    supplier_source_status = EXCLUDED.supplier_source_status,
    lot_auctionbifurcation_flag = EXCLUDED.lot_auctionbifurcation_flag,
    partner_vendor_name = EXCLUDED.partner_vendor_name,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct",      // assigned_event_vendor_id
        "Direct",      // event_id
        "Direct",      // supplier_id
        "Direct",      // supplier_participation_status
        "Direct",      // supplier_event_regrate_remark
        "Direct",      // supplier_price_bid_status
        "Direct",      // supplier_price_bid_currency_id
        "Direct",      // supplier_email_address
        "Direct",      // supplier_source_status
        "Conditional", // lot_auctionbifurcation_flag (from BifurcationDone: 1 -> true, else false)
        "Direct",      // partner_vendor_name (from VendorName)
        "Fixed",       // created_by
        "Fixed",       // created_date
        "Fixed",       // modified_by
        "Fixed",       // modified_date
        "Fixed",       // is_deleted
        "Fixed",       // deleted_by
        "Fixed"        // deleted_date
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "EVENTSELUSERID", logic = "EVENTSELUSERID -> assigned_event_vendor_id (Direct)", target = "assigned_event_vendor_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Direct from Event Master Table)", target = "event_id" },
            new { source = "USERID", logic = "USERID -> supplier_id (Direct from Supplier Master table)", target = "supplier_id" },
            new { source = "PARTICIPATESTATUS", logic = "PARTICIPATESTATUS -> supplier_participation_status (Direct)", target = "supplier_participation_status" },
            new { source = "REGRATECOMMENT", logic = "REGRATECOMMENT -> supplier_event_regrate_remark (Direct)", target = "supplier_event_regrate_remark" },
            new { source = "PRICEBIDSTATUS", logic = "PRICEBIDSTATUS -> supplier_price_bid_status (Direct)", target = "supplier_price_bid_status" },
            new { source = "VendorCurrencyId", logic = "VendorCurrencyId -> supplier_price_bid_currency_id (Ref from currency Master)", target = "supplier_price_bid_currency_id" },
            new { source = "SendEmail", logic = "SendEmail -> supplier_email_address (Direct)", target = "supplier_email_address" },
            new { source = "IsSourceListUser", logic = "IsSourceListUser -> supplier_source_status (Direct)", target = "supplier_source_status" },
            new { source = "BifurcationDone", logic = "BifurcationDone -> lot_auctionbifurcation_flag (Conditional: 1 -> true, else false) from TBL_PB_SUPPLIER via LEFT JOIN", target = "lot_auctionbifurcation_flag" },
            new { source = "VendorName", logic = "VendorName -> partner_vendor_name (Direct) from TBL_PB_SUPPLIER via LEFT JOIN", target = "partner_vendor_name" },
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
        _migrationLogger = new MigrationLogger(_logger, "assigned_event_vendor");
        _migrationLogger.LogInfo("Starting AssignedEventVendor migration (USERTYPE = 'Vendor' only)");
        int result = await base.MigrateAsync(useTransaction: true);
        _migrationLogger.LogInfo($"Completed: {_migrationLogger.InsertedCount} inserted, {_migrationLogger.SkippedCount} skipped");
        return result;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "assigned_event_vendor");
        _migrationLogger.LogInfo("Starting AssignedEventVendor migration (USERTYPE = 'Vendor' only)");
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        var validEventIds = await LoadValidEventIdsAsync(pgConn, transaction);
        var validSupplierIds = await LoadValidSupplierIdsAsync(pgConn, transaction);
        _migrationLogger.LogInfo($"Loaded {validEventIds.Count} valid event IDs and {validSupplierIds.Count} valid supplier IDs");
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        int processedCount = 0;
        var skippedRecords = new List<(string RecordId, string Reason)>();

        while (await reader.ReadAsync())
        {
            processedCount++;
            // Parse as int, not string
            int eventSelUserId = Convert.ToInt32(reader["EVENTSELUSERID"]);
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var userId = reader["USERID"] ?? DBNull.Value;

            string recordId = $"EVENTSELUSERID={eventSelUserId}";

            // Validate required foreign keys
            if (eventId == DBNull.Value)
            {
                _migrationLogger.LogSkipped("EVENTID is NULL", recordId, new Dictionary<string, object> { { "EVENTID", eventId } });
                skippedCount++;
                skippedRecords.Add((eventSelUserId.ToString(), "EVENTID is NULL"));
                continue;
            }

            int eventIdValue = Convert.ToInt32(eventId);
            if (!validEventIds.Contains(eventIdValue))
            {
                _migrationLogger.LogSkipped($"EVENTID {eventIdValue} not found in event_master", recordId, new Dictionary<string, object> { { "EVENTID", eventIdValue } });
                skippedCount++;
                skippedRecords.Add((eventSelUserId.ToString(), $"EVENTID {eventIdValue} not found in event_master"));
                continue;
            }

            if (userId == DBNull.Value)
            {
                _migrationLogger.LogSkipped("USERID is NULL", recordId, new Dictionary<string, object> { { "USERID", userId } });
                skippedCount++;
                skippedRecords.Add((eventSelUserId.ToString(), "USERID is NULL"));
                continue;
            }

            int userIdValue = Convert.ToInt32(userId);
            if (!validSupplierIds.Contains(userIdValue))
            {
                _migrationLogger.LogSkipped($"USERID {userIdValue} not found in supplier_master", recordId, new Dictionary<string, object> { { "USERID", userIdValue } });
                skippedCount++;
                skippedRecords.Add((eventSelUserId.ToString(), $"USERID {userIdValue} not found in supplier_master"));
                continue;
            }

            var participateStatus = reader["PARTICIPATESTATUS"] ?? DBNull.Value;
            var regrateComment = reader["REGRATECOMMENT"] ?? DBNull.Value;
            var priceBidStatus = reader["PRICEBIDSTATUS"] ?? DBNull.Value;
            var vendorCurrencyId = reader["VendorCurrencyId"] ?? DBNull.Value;
            var sendEmail = reader["SendEmail"] ?? DBNull.Value;
            var isSourceListUser = reader["IsSourceListUser"] ?? DBNull.Value;

            var bifurcationDone = reader["BifurcationDone"] ?? DBNull.Value;
            var vendorName = reader["VendorName"] ?? DBNull.Value;

            bool lotAuctionBifurcationFlag = false;
            if (bifurcationDone != DBNull.Value)
            {
                int bifurcationValue = Convert.ToInt32(bifurcationDone);
                lotAuctionBifurcationFlag = (bifurcationValue == 1);
            }

            var record = new Dictionary<string, object>
            {
                ["assigned_event_vendor_id"] = eventSelUserId, // int, not string
                ["event_id"] = eventId,
                ["supplier_id"] = userId,
                ["supplier_participation_status"] = participateStatus,
                ["supplier_event_regrate_remark"] = regrateComment,
                ["supplier_price_bid_status"] = priceBidStatus,
                ["supplier_price_bid_currency_id"] = vendorCurrencyId,
                ["supplier_email_address"] = sendEmail,
                ["supplier_source_status"] = isSourceListUser,
                ["lot_auctionbifurcation_flag"] = lotAuctionBifurcationFlag,
                ["partner_vendor_name"] = vendorName,
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
                _migrationLogger.LogInfo($"Inserting batch {batchNumber} with {batch.Count} records");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                batch.Clear();
            }

            if (processedCount % 1000 == 0)
            {
                _migrationLogger.LogInfo($"Processed {processedCount} records");
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _migrationLogger.LogInfo($"Inserting final batch {batchNumber} with {batch.Count} records");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
        }

        _migrationLogger.LogInfo($"AssignedEventVendor migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
        MigrationStatsExporter.ExportToExcel("assigned_event_vendor_migration_stats.xlsx", processedCount, insertedCount, skippedCount, _logger, skippedRecords);
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

    private async Task<HashSet<int>> LoadValidSupplierIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT supplier_id FROM supplier_master";

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

        var deduplicatedBatch = batch
            .GroupBy(r => r["assigned_event_vendor_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate assigned_event_vendor_id records.");
            _migrationLogger?.LogInfo($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate assigned_event_vendor_id records");
        }

        var columns = new List<string> {
            "assigned_event_vendor_id", "event_id", "supplier_id", "supplier_participation_status",
            "supplier_event_regrate_remark", "supplier_price_bid_status", "supplier_price_bid_currency_id",
            "supplier_email_address", "supplier_source_status", "lot_auctionbifurcation_flag",
            "partner_vendor_name", "created_by", "created_date", "modified_by", "modified_date",
            "is_deleted", "deleted_by", "deleted_date"
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

        var updateColumns = columns.Where(c => c != "assigned_event_vendor_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO assigned_event_vendor ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (assigned_event_vendor_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = 0;
        try
        {
            result = await insertCmd.ExecuteNonQueryAsync();
            _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
            _migrationLogger?.LogInfo($"Batch {batchNumber}: Inserted/Updated {result} records");

            foreach (var record in deduplicatedBatch)
            {
                var recordId = $"EVENTSELUSERID={record["assigned_event_vendor_id"]}";
                _migrationLogger?.LogInserted(recordId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Batch {batchNumber}: Error during insert. SQL: {sql}");
            foreach (var record in deduplicatedBatch)
            {
                var recordData = string.Join(", ", record.Select(kv => $"{kv.Key}={kv.Value ?? "NULL"}"));
                _logger.LogError($"Batch {batchNumber}: Record data: {recordData}");
            }
            _migrationLogger?.LogError($"Batch {batchNumber}: Error during insert", $"Batch={batchNumber}", ex);
            // Optionally, rollback the transaction if provided
            if (transaction != null)
            {
                try { transaction.Rollback(); } catch { }
            }
            // Optionally, rethrow or return 0 to continue with next batch
            return 0;
        }
        return result;
    }
}
