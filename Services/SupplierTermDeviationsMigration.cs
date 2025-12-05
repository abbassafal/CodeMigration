using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class SupplierTermDeviationsMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<SupplierTermDeviationsMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            VENDORDEVIATIONTRNID,
            VENDORDEVIATIONMSTID,
            DEVIATIONREMARKS,
            USERTYPE,
            ACTIONBY,
            ACTIONDATE,
            ISUPDATEDCLAUSE
        FROM TBL_VENDORDEVIATIONTRN
        ORDER BY VENDORDEVIATIONTRNID";

    protected override string InsertQuery => @"
        INSERT INTO supplier_term_deviations (
            supplier_term_deviation_id,
            supplier_term_id,
            event_id,
            deviation_remarks,
            user_id,
            supplier_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date,
            user_type
        ) VALUES (
            @supplier_term_deviation_id,
            @supplier_term_id,
            @event_id,
            @deviation_remarks,
            @user_id,
            @supplier_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date,
            @user_type
        )
        ON CONFLICT (supplier_term_deviation_id) DO UPDATE SET
            supplier_term_id = EXCLUDED.supplier_term_id,
            event_id = EXCLUDED.event_id,
            deviation_remarks = EXCLUDED.deviation_remarks,
            user_id = EXCLUDED.user_id,
            supplier_id = EXCLUDED.supplier_id,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date,
            user_type = EXCLUDED.user_type";

    public SupplierTermDeviationsMigration(IConfiguration configuration, ILogger<SupplierTermDeviationsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",      // supplier_term_deviation_id
            "Direct",      // supplier_term_id
            "Lookup",      // event_id (from supplier_terms via supplier_term_id)
            "Direct",      // deviation_remarks
            "Conditional", // user_id (ACTIONBY if USERTYPE != 'Vendor', else NULL)
            "Conditional", // supplier_id (ACTIONBY if USERTYPE = 'Vendor', else from supplier_terms lookup)
            "Fixed",       // created_by
            "Fixed",       // created_date
            "Fixed",       // modified_by
            "Fixed",       // modified_date
            "Fixed",       // is_deleted
            "Fixed",       // deleted_by
            "Fixed",       // deleted_date
            "Direct"       // user_type
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "VENDORDEVIATIONTRNID", logic = "VENDORDEVIATIONTRNID -> supplier_term_deviation_id (Primary key, autoincrement - SupplierTermDeviationId)", target = "supplier_term_deviation_id" },
            new { source = "VENDORDEVIATIONMSTID", logic = "VENDORDEVIATIONMSTID -> supplier_term_id (Foreign key to supplier_terms - SupplierTermId)", target = "supplier_term_id" },
            new { source = "-", logic = "event_id -> Lookup from supplier_terms (EventId)", target = "event_id" },
            new { source = "DEVIATIONREMARKS", logic = "DEVIATIONREMARKS -> deviation_remarks (DEVIATIONREMARKS)", target = "deviation_remarks" },
            new { source = "ACTIONBY", logic = "ACTIONBY -> user_id (Conditional: if USERTYPE != 'Vendor', else NULL)", target = "user_id" },
            new { source = "ACTIONBY", logic = "ACTIONBY -> supplier_id (Conditional: if USERTYPE = 'Vendor', else from supplier_terms lookup)", target = "supplier_id" },
            new { source = "ACTIONDATE", logic = "ACTIONDATE -> Not mapped to target table", target = "-" },
            new { source = "ISUPDATEDCLAUSE", logic = "ISUPDATEDCLAUSE -> Not mapped to target table", target = "-" },
            new { source = "USERTYPE", logic = "USERTYPE -> user_type", target = "user_type" },
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
        _logger.LogInformation("Starting Supplier Term Deviations migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid supplier_term IDs and their related event_id and supplier_id
            var supplierTermsMap = await LoadSupplierTermsMapAsync(pgConn);
            _logger.LogInformation($"Loaded {supplierTermsMap.Count} supplier_term mappings");

            // Load valid user IDs
            var validUserIds = await LoadValidUserIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validUserIds.Count} valid user IDs");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var vendorDeviationTrnId = reader["VENDORDEVIATIONTRNID"];
                var vendorDeviationMstId = reader["VENDORDEVIATIONMSTID"];
                var deviationRemarks = reader["DEVIATIONREMARKS"];
                var userType = reader["USERTYPE"];
                var actionBy = reader["ACTIONBY"];
                var actionDate = reader["ACTIONDATE"];
                var isUpdatedClause = reader["ISUPDATEDCLAUSE"];

                // Skip if VENDORDEVIATIONTRNID is NULL
                if (vendorDeviationTrnId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - VENDORDEVIATIONTRNID is NULL");
                    continue;
                }

                int vendorDeviationTrnIdValue = Convert.ToInt32(vendorDeviationTrnId);

                // Skip duplicates
                if (processedIds.Contains(vendorDeviationTrnIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Skip if supplier_term_id is NULL (NOT NULL constraint)
                if (vendorDeviationMstId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping VENDORDEVIATIONTRNID {vendorDeviationTrnIdValue} - supplier_term_id is NULL");
                    continue;
                }

                int vendorDeviationMstIdValue = Convert.ToInt32(vendorDeviationMstId);

                // Validate supplier_term_id and get related event_id and supplier_id
                if (!supplierTermsMap.ContainsKey(vendorDeviationMstIdValue))
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping VENDORDEVIATIONTRNID {vendorDeviationTrnIdValue} - Invalid supplier_term_id: {vendorDeviationMstIdValue}");
                    continue;
                }

                var supplierTermData = supplierTermsMap[vendorDeviationMstIdValue];

                // Determine user_id and supplier_id based on USERTYPE
                object userId = DBNull.Value;
                object supplierId = DBNull.Value;
                
                string userTypeValue = userType != DBNull.Value ? userType?.ToString()?.Trim() ?? string.Empty : string.Empty;
                
                if (string.Equals(userTypeValue, "Vendor", StringComparison.OrdinalIgnoreCase))
                {
                    // If USERTYPE = 'Vendor', ACTIONBY goes to supplier_id
                    supplierId = actionBy ?? DBNull.Value;
                }
                else
                {
                    // If USERTYPE != 'Vendor', ACTIONBY goes to user_id
                    userId = actionBy ?? DBNull.Value;
                    // Use supplier_id from supplier_terms lookup
                    supplierId = supplierTermData.SupplierId.HasValue ? (object)supplierTermData.SupplierId.Value : DBNull.Value;
                }
                
                // Validate user_id if not null and USERTYPE != 'Vendor'
                if (userId != DBNull.Value)
                {
                    int userIdValue = Convert.ToInt32(userId);
                    if (!validUserIds.Contains(userIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping VENDORDEVIATIONTRNID {vendorDeviationTrnIdValue} - Invalid user_id: {userIdValue}");
                        continue;
                    }
                }

                var record = new Dictionary<string, object>
                {
                    ["supplier_term_deviation_id"] = vendorDeviationTrnIdValue,
                    ["supplier_term_id"] = vendorDeviationMstIdValue,
                    ["event_id"] = supplierTermData.EventId.HasValue ? (object)supplierTermData.EventId.Value : DBNull.Value,
                    ["deviation_remarks"] = deviationRemarks ?? DBNull.Value,
                    ["user_id"] = userId,
                    ["supplier_id"] = supplierId,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value,
                    ["user_type"] = userType ?? DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(vendorDeviationTrnIdValue);

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

            _logger.LogInformation($"Supplier Term Deviations migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Term Deviations migration");
            throw;
        }
    }

    private async Task<Dictionary<int, (int? EventId, int? SupplierId)>> LoadSupplierTermsMapAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, (int? EventId, int? SupplierId)>();

        try
        {
            var query = "SELECT supplier_term_id, event_id, supplier_id FROM supplier_terms WHERE supplier_term_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                int supplierTermId = reader.GetInt32(0);
                int? eventId = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
                int? supplierId = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2);
                
                map[supplierTermId] = (eventId, supplierId);
            }

            _logger.LogInformation($"Loaded {map.Count} supplier_term mappings from supplier_terms");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading supplier_term mappings");
        }

        return map;
    }

    private async Task<HashSet<int>> LoadValidUserIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT user_id FROM users WHERE user_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid user IDs from users");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid user IDs");
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
