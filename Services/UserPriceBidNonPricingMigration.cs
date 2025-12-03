using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class UserPriceBidNonPricingMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<UserPriceBidNonPricingMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            PB_BuyerNonPricingId,
            PB_NonPricingTitle,
            EVENT_ID
        FROM TBL_PB_BuyerNonPricing
        ORDER BY PB_BuyerNonPricingId";

    protected override string InsertQuery => @"
        INSERT INTO user_price_bid_non_pricing (
            user_price_bid_non_pricing_id,
            none_pricing_title,
            event_id,
            mandatory,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @user_price_bid_non_pricing_id,
            @none_pricing_title,
            @event_id,
            @mandatory,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (user_price_bid_non_pricing_id) DO UPDATE SET
            none_pricing_title = EXCLUDED.none_pricing_title,
            event_id = EXCLUDED.event_id,
            mandatory = EXCLUDED.mandatory,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public UserPriceBidNonPricingMigration(IConfiguration configuration, ILogger<UserPriceBidNonPricingMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // user_price_bid_non_pricing_id
            "Direct",  // none_pricing_title
            "Direct",  // event_id
            "Fixed",   // mandatory
            "Fixed",   // created_by
            "Fixed",   // created_date
            "Fixed",   // modified_by
            "Fixed",   // modified_date
            "Fixed",   // is_deleted
            "Fixed",   // deleted_by
            "Fixed"    // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PB_BuyerNonPricingId", logic = "PB_BuyerNonPricingId -> user_price_bid_non_pricing_id (Primary key, autoincrement)", target = "user_price_bid_non_pricing_id" },
            new { source = "PB_NonPricingTitle", logic = "PB_NonPricingTitle -> none_pricing_title", target = "none_pricing_title" },
            new { source = "EVENT_ID", logic = "EVENT_ID -> event_id (Foreign key to event_master)", target = "event_id" },
            new { source = "-", logic = "mandatory -> false (Fixed Default)", target = "mandatory" },
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
        _logger.LogInformation("Starting User Price Bid Non Pricing migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid event IDs
            var validEventIds = await LoadValidEventIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validEventIds.Count} valid event IDs");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var pbBuyerNonPricingId = reader["PB_BuyerNonPricingId"];
                var pbNonPricingTitle = reader["PB_NonPricingTitle"];
                var eventId = reader["EVENT_ID"];

                // Skip if PB_BuyerNonPricingId is NULL
                if (pbBuyerNonPricingId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - PB_BuyerNonPricingId is NULL");
                    continue;
                }

                int pbBuyerNonPricingIdValue = Convert.ToInt32(pbBuyerNonPricingId);

                // Skip duplicates
                if (processedIds.Contains(pbBuyerNonPricingIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Validate event_id
                if (eventId != DBNull.Value)
                {
                    int eventIdValue = Convert.ToInt32(eventId);
                    if (!validEventIds.Contains(eventIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping PB_BuyerNonPricingId {pbBuyerNonPricingIdValue} - Invalid event_id: {eventIdValue}");
                        continue;
                    }
                }

                var record = new Dictionary<string, object>
                {
                    ["user_price_bid_non_pricing_id"] = pbBuyerNonPricingIdValue,
                    ["none_pricing_title"] = pbNonPricingTitle ?? DBNull.Value,
                    ["event_id"] = eventId ?? DBNull.Value,
                    ["mandatory"] = false,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(pbBuyerNonPricingIdValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchAsync(batch, pgConn, transaction);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchAsync(batch, pgConn, transaction);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"User Price Bid Non Pricing migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during User Price Bid Non Pricing migration");
            throw;
        }
    }

    private async Task<HashSet<int>> LoadValidEventIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT event_id FROM event_master WHERE event_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid event IDs from event_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid event IDs");
        }

        return validIds;
    }

    private async Task<int> InsertBatchAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        int insertedCount = 0;

        try
        {
            foreach (var record in batch)
            {
                using var cmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);

                foreach (var kvp in record)
                {
                    cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                await cmd.ExecuteNonQueryAsync();
                insertedCount++;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records");
            throw;
        }

        return insertedCount;
    }
}
