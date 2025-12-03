using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class TermTemplateMasterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<TermTemplateMasterMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            CLAUSE_MASTER_ID,
            REF_KEY,
            DEFINATION,
            SUBCLAUSE,
            CLAUSE_MST_ID
        FROM TBL_CLAUSEMASTER
        ORDER BY CLAUSE_MASTER_ID";

    protected override string InsertQuery => @"
        INSERT INTO term_template_master (
            term_template_master_id,
            term_template_name,
            term_template_name,
            term_description,
            term_master_id
        ) VALUES (
            @term_template_master_id,
            @term_template_name,
            @term_template_name_2,
            @term_description,
            @term_master_id
        )
        ON CONFLICT (term_template_master_id) DO UPDATE SET
            term_template_name = EXCLUDED.term_template_name,
            term_description = EXCLUDED.term_description,
            term_master_id = EXCLUDED.term_master_id";

    public TermTemplateMasterMigration(IConfiguration configuration, ILogger<TermTemplateMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // term_template_master_id
            "Direct", // term_template_name (from REF_KEY)
            "Direct", // term_template_name (from DEFINATION)
            "Direct", // term_description
            "Direct"  // term_master_id
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "CLAUSE_MASTER_ID", logic = "CLAUSE_MASTER_ID -> term_template_master_id (Primary key, autoincrement)", target = "term_template_master_id" },
            new { source = "REF_KEY", logic = "REF_KEY -> term_template_name (Direct)", target = "term_template_name" },
            new { source = "DEFINATION", logic = "DEFINATION -> term_template_name (TermTemplateName)", target = "term_template_name" },
            new { source = "SUBCLAUSE", logic = "SUBCLAUSE -> term_description (TERMDESCRIPTION)", target = "term_description" },
            new { source = "CLAUSE_MST_ID", logic = "CLAUSE_MST_ID -> term_master_id (Ref from TermMaster Table, TermMasterId)", target = "term_master_id" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting Term Template Master migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid term_master IDs
            var validTermMasterIds = await LoadValidTermMasterIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validTermMasterIds.Count} valid term master IDs");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var clauseMasterId = reader["CLAUSE_MASTER_ID"];
                var refKey = reader["REF_KEY"];
                var defination = reader["DEFINATION"];
                var subClause = reader["SUBCLAUSE"];
                var clauseMstId = reader["CLAUSE_MST_ID"];

                // Skip if CLAUSE_MASTER_ID is NULL
                if (clauseMasterId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - CLAUSE_MASTER_ID is NULL");
                    continue;
                }

                int clauseMasterIdValue = Convert.ToInt32(clauseMasterId);

                // Skip duplicates
                if (processedIds.Contains(clauseMasterIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Validate term_master_id (CLAUSE_MST_ID)
                if (clauseMstId != DBNull.Value)
                {
                    int clauseMstIdValue = Convert.ToInt32(clauseMstId);
                    if (!validTermMasterIds.Contains(clauseMstIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping CLAUSE_MASTER_ID {clauseMasterIdValue} - Invalid term_master_id: {clauseMstIdValue}");
                        continue;
                    }
                }

                var record = new Dictionary<string, object>
                {
                    ["term_template_master_id"] = clauseMasterIdValue,
                    ["term_template_name"] = refKey ?? DBNull.Value,
                    ["term_template_name_2"] = defination ?? DBNull.Value,
                    ["term_description"] = subClause ?? DBNull.Value,
                    ["term_master_id"] = clauseMstId ?? DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(clauseMasterIdValue);

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

            _logger.LogInformation($"Term Template Master migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Term Template Master migration");
            throw;
        }
    }

    private async Task<HashSet<int>> LoadValidTermMasterIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT term_master_id FROM term_master WHERE term_master_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid term master IDs from term_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid term master IDs");
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
