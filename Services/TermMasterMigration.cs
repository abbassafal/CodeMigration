using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class TermMasterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<TermMasterMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            TERMID,
            TERMDESCRIPTION,
            ISACTIV
        FROM TBL_CLAUSETERMMASTER
        ORDER BY TERMID";

    protected override string InsertQuery => @"
        INSERT INTO term_master (
            term_master_id,
            term_description,
            user_term_name
        ) VALUES (
            @term_master_id,
            @term_description,
            @user_term_name
        )
        ON CONFLICT (term_master_id) DO UPDATE SET
            term_description = EXCLUDED.term_description,
            user_term_name = EXCLUDED.user_term_name";

    public TermMasterMigration(IConfiguration configuration, ILogger<TermMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // term_master_id
            "Direct", // term_description
            "Direct"  // user_term_name
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "TERMID", logic = "TERMID -> term_master_id (Primary key, autoincrement)", target = "term_master_id" },
            new { source = "TERMDESCRIPTION", logic = "TERMDESCRIPTION -> term_description (Direct)", target = "term_description" },
            new { source = "ISACTIV", logic = "ISACTIV -> user_term_name (Direct mapping)", target = "user_term_name" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting Term Master migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var termId = reader["TERMID"];
                var termDescription = reader["TERMDESCRIPTION"];
                var isActiv = reader["ISACTIV"];

                // Skip if TERMID is NULL
                if (termId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - TERMID is NULL");
                    continue;
                }

                int termIdValue = Convert.ToInt32(termId);

                // Skip duplicates
                if (processedIds.Contains(termIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                var record = new Dictionary<string, object>
                {
                    ["term_master_id"] = termIdValue,
                    ["term_description"] = termDescription ?? DBNull.Value,
                    ["user_term_name"] = isActiv ?? DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(termIdValue);

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

            _logger.LogInformation($"Term Master migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Term Master migration");
            throw;
        }
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
