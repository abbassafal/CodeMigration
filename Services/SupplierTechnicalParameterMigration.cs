using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class SupplierTechnicalParameterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<SupplierTechnicalParameterMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            VendorTechItemTermId,
            TechItemTermId,
            Value,
            VendorId,
            PBID
        FROM TBL_VendorTechItemTerm
        ORDER BY VendorTechItemTermId";

    protected override string InsertQuery => @"
        INSERT INTO supplier_technical_parameter (
            supplier_technical_parameter_id,
            user_technical_parameter_id,
            supplier_technical_parameter_response,
            supplier_id,
            event_item_id,
            event_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_technical_parameter_id,
            @user_technical_parameter_id,
            @supplier_technical_parameter_response,
            @supplier_id,
            @event_item_id,
            @event_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (supplier_technical_parameter_id) DO UPDATE SET
            user_technical_parameter_id = EXCLUDED.user_technical_parameter_id,
            supplier_technical_parameter_response = EXCLUDED.supplier_technical_parameter_response,
            supplier_id = EXCLUDED.supplier_id,
            event_item_id = EXCLUDED.event_item_id,
            event_id = EXCLUDED.event_id,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public SupplierTechnicalParameterMigration(IConfiguration configuration, ILogger<SupplierTechnicalParameterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // supplier_technical_parameter_id
            "Direct",  // user_technical_parameter_id
            "Direct",  // supplier_technical_parameter_response
            "Direct",  // supplier_id
            "Lookup",  // event_item_id (from user_price_bid via PBID)
            "Lookup",  // event_id (from user_price_bid via PBID)
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
            new { source = "VendorTechItemTermId", logic = "VendorTechItemTermId -> supplier_technical_parameter_id (Primary key, autoincrement - SupplierTechnicalParameterId)", target = "supplier_technical_parameter_id" },
            new { source = "TechItemTermId", logic = "TechItemTermId -> user_technical_parameter_id (Foreign key to user_technical_parameter - UserTechnicalParameterId)", target = "user_technical_parameter_id" },
            new { source = "Value", logic = "Value -> supplier_technical_parameter_response (SupplierTechnicalParameterResponse)", target = "supplier_technical_parameter_response" },
            new { source = "VendorId", logic = "VendorId -> supplier_id (Foreign key to supplier_master - SupplierId)", target = "supplier_id" },
            new { source = "PBID", logic = "PBID -> event_item_id (Lookup from user_price_bid - EventItemsID)", target = "event_item_id" },
            new { source = "-", logic = "event_id -> Lookup from user_price_bid via PBID (EventId)", target = "event_id" },
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
        return await base.MigrateAsync(useTransaction: false);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting Supplier Technical Parameter migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid user_technical_parameter IDs
            var validUserTechParamIds = await LoadValidUserTechnicalParameterIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validUserTechParamIds.Count} valid user_technical_parameter IDs");

            // Load valid supplier IDs
            var validSupplierIds = await LoadValidSupplierIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validSupplierIds.Count} valid supplier IDs");

            // Load user_price_bid mappings (PBID -> event_item_id, event_id)
            var userPriceBidMap = await LoadUserPriceBidMapAsync(pgConn);
            _logger.LogInformation($"Loaded {userPriceBidMap.Count} user_price_bid mappings");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<long>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var vendorTechItemTermId = reader["VendorTechItemTermId"];
                var techItemTermId = reader["TechItemTermId"];
                var value = reader["Value"];
                var vendorId = reader["VendorId"];
                var pbId = reader["PBID"];

                // Skip if VendorTechItemTermId is NULL
                if (vendorTechItemTermId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - VendorTechItemTermId is NULL");
                    continue;
                }

                long vendorTechItemTermIdValue = Convert.ToInt64(vendorTechItemTermId);

                // Skip duplicates
                if (processedIds.Contains(vendorTechItemTermIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Skip if user_technical_parameter_id is NULL (NOT NULL constraint)
                if (techItemTermId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping VendorTechItemTermId {vendorTechItemTermIdValue} - user_technical_parameter_id is NULL");
                    continue;
                }

                long techItemTermIdValue = Convert.ToInt64(techItemTermId);

                // Validate user_technical_parameter_id
                if (!validUserTechParamIds.Contains(techItemTermIdValue))
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping VendorTechItemTermId {vendorTechItemTermIdValue} - Invalid user_technical_parameter_id: {techItemTermIdValue}");
                    continue;
                }

                // Skip if supplier_id is NULL (NOT NULL constraint)
                if (vendorId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping VendorTechItemTermId {vendorTechItemTermIdValue} - supplier_id is NULL");
                    continue;
                }

                int vendorIdValue = Convert.ToInt32(vendorId);

                // Validate supplier_id
                if (!validSupplierIds.Contains(vendorIdValue))
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping VendorTechItemTermId {vendorTechItemTermIdValue} - Invalid supplier_id: {vendorIdValue}");
                    continue;
                }

                // Get event_item_id and event_id from user_price_bid via PBID
                int? eventItemId = null;
                int? eventId = null;

                if (pbId != DBNull.Value)
                {
                    int pbIdValue = Convert.ToInt32(pbId);
                    if (userPriceBidMap.ContainsKey(pbIdValue))
                    {
                        var priceBidData = userPriceBidMap[pbIdValue];
                        eventItemId = priceBidData.EventItemId;
                        eventId = priceBidData.EventId;
                    }
                }

                var record = new Dictionary<string, object>
                {
                    ["supplier_technical_parameter_id"] = vendorTechItemTermIdValue,
                    ["user_technical_parameter_id"] = techItemTermIdValue,
                    ["supplier_technical_parameter_response"] = value ?? DBNull.Value,
                    ["supplier_id"] = vendorIdValue,
                    ["event_item_id"] = eventItemId.HasValue ? (object)eventItemId.Value : DBNull.Value,
                    ["event_id"] = eventId.HasValue ? (object)eventId.Value : DBNull.Value,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(vendorTechItemTermIdValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"Supplier Technical Parameter migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Technical Parameter migration");
            throw;
        }
    }

    private async Task<HashSet<long>> LoadValidUserTechnicalParameterIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<long>();

        try
        {
            var query = "SELECT user_technical_parameter_id FROM user_technical_parameter WHERE user_technical_parameter_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt64(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid user_technical_parameter IDs from user_technical_parameter");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid user_technical_parameter IDs");
        }

        return validIds;
    }

    private async Task<HashSet<int>> LoadValidSupplierIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT supplier_id FROM supplier_master WHERE supplier_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid supplier IDs from supplier_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid supplier IDs");
        }

        return validIds;
    }

    private async Task<Dictionary<int, (int? EventItemId, int? EventId)>> LoadUserPriceBidMapAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, (int? EventItemId, int? EventId)>();

        try
        {
            var query = "SELECT user_price_bid_id, event_items_id, event_id FROM user_price_bid WHERE user_price_bid_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                int userPriceBidId = reader.GetInt32(0);
                int? eventItemId = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
                int? eventId = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2);
                
                map[userPriceBidId] = (eventItemId, eventId);
            }

            _logger.LogInformation($"Loaded {map.Count} user_price_bid mappings");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading user_price_bid mappings");
        }

        return map;
    }

    private async Task<int> InsertBatchWithTransactionAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn)
    {
        int insertedCount = 0;

        using var transaction = await pgConn.BeginTransactionAsync();
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

            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records. Batch rolled back.");
            // Don't throw - continue with next batch
        }

        return insertedCount;
    }
}
