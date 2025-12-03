using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class SupplierTermsMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<SupplierTermsMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            SupplierDEVIATIONMSTID,
            EVENTID,
            SupplierID,
            CLAUSEEVENTWISEID,
            ISACCEPT,
            ISDEVIATE,
            ENT_DATE,
            ISUPDATED
        FROM TBL_VENDORDEVIATIONMASTER
        ORDER BY SupplierDEVIATIONMSTID";

    protected override string InsertQuery => @"
        INSERT INTO supplier_terms (
            supplier_term_id,
            event_id,
            supplier_id,
            user_term_id,
            term_accept,
            term_deviate,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_term_id,
            @event_id,
            @supplier_id,
            @user_term_id,
            @term_accept,
            @term_deviate,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (supplier_term_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            supplier_id = EXCLUDED.supplier_id,
            user_term_id = EXCLUDED.user_term_id,
            term_accept = EXCLUDED.term_accept,
            term_deviate = EXCLUDED.term_deviate,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public SupplierTermsMigration(IConfiguration configuration, ILogger<SupplierTermsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // supplier_term_id
            "Direct",  // event_id
            "Direct",  // supplier_id
            "Direct",  // user_term_id
            "Direct",  // term_accept
            "Direct",  // term_deviate
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
            new { source = "SupplierDEVIATIONMSTID", logic = "SupplierDEVIATIONMSTID -> supplier_term_id (Primary key, autoincrement - SupplierTermId)", target = "supplier_term_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Foreign key to event_master - EventId)", target = "event_id" },
            new { source = "SupplierID", logic = "SupplierID -> supplier_id (Foreign key to supplier_master - SupplierId)", target = "supplier_id" },
            new { source = "CLAUSEEVENTWISEID", logic = "CLAUSEEVENTWISEID -> user_term_id (Foreign key to user_term - UserTermId)", target = "user_term_id" },
            new { source = "ISACCEPT", logic = "ISACCEPT -> term_accept (Boolean - TermAccept)", target = "term_accept" },
            new { source = "ISDEVIATE", logic = "ISDEVIATE -> term_deviate (Boolean - TermDeviate)", target = "term_deviate" },
            new { source = "ENT_DATE", logic = "ENT_DATE -> Not mapped to target table", target = "-" },
            new { source = "ISUPDATED", logic = "ISUPDATED -> Not mapped to target table", target = "-" },
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
        _logger.LogInformation("Starting Supplier Terms migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid event IDs
            var validEventIds = await LoadValidEventIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validEventIds.Count} valid event IDs");

            // Load valid supplier IDs
            var validSupplierIds = await LoadValidSupplierIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validSupplierIds.Count} valid supplier IDs");

            // Load valid user_term IDs
            var validUserTermIds = await LoadValidUserTermIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validUserTermIds.Count} valid user_term IDs");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var supplierDeviationMstId = reader["SupplierDEVIATIONMSTID"];
                var eventId = reader["EVENTID"];
                var supplierId = reader["SupplierID"];
                var clauseEventWiseId = reader["CLAUSEEVENTWISEID"];
                var isAccept = reader["ISACCEPT"];
                var isDeviate = reader["ISDEVIATE"];
                var entDate = reader["ENT_DATE"];
                var isUpdated = reader["ISUPDATED"];

                // Skip if SupplierDEVIATIONMSTID is NULL
                if (supplierDeviationMstId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - SupplierDEVIATIONMSTID is NULL");
                    continue;
                }

                int supplierDeviationMstIdValue = Convert.ToInt32(supplierDeviationMstId);

                // Skip duplicates
                if (processedIds.Contains(supplierDeviationMstIdValue))
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
                        _logger.LogWarning($"Skipping SupplierDEVIATIONMSTID {supplierDeviationMstIdValue} - Invalid event_id: {eventIdValue}");
                        continue;
                    }
                }

                // Validate supplier_id
                if (supplierId != DBNull.Value)
                {
                    int supplierIdValue = Convert.ToInt32(supplierId);
                    if (!validSupplierIds.Contains(supplierIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping SupplierDEVIATIONMSTID {supplierDeviationMstIdValue} - Invalid supplier_id: {supplierIdValue}");
                        continue;
                    }
                }

                // Validate user_term_id
                if (clauseEventWiseId != DBNull.Value)
                {
                    int clauseEventWiseIdValue = Convert.ToInt32(clauseEventWiseId);
                    if (!validUserTermIds.Contains(clauseEventWiseIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping SupplierDEVIATIONMSTID {supplierDeviationMstIdValue} - Invalid user_term_id: {clauseEventWiseIdValue}");
                        continue;
                    }
                }

                // Convert ISACCEPT and ISDEVIATE to boolean
                bool termAccept = false;
                if (isAccept != DBNull.Value)
                {
                    int acceptValue = Convert.ToInt32(isAccept);
                    termAccept = acceptValue == 1;
                }

                bool termDeviate = false;
                if (isDeviate != DBNull.Value)
                {
                    int deviateValue = Convert.ToInt32(isDeviate);
                    termDeviate = deviateValue == 1;
                }

                var record = new Dictionary<string, object>
                {
                    ["supplier_term_id"] = supplierDeviationMstIdValue,
                    ["event_id"] = eventId ?? DBNull.Value,
                    ["supplier_id"] = supplierId ?? DBNull.Value,
                    ["user_term_id"] = clauseEventWiseId ?? DBNull.Value,
                    ["term_accept"] = termAccept,
                    ["term_deviate"] = termDeviate,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(supplierDeviationMstIdValue);

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

            _logger.LogInformation($"Supplier Terms migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Terms migration");
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

    private async Task<HashSet<int>> LoadValidUserTermIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT user_term_id FROM user_term WHERE user_term_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid user_term IDs from user_term");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid user_term IDs");
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
