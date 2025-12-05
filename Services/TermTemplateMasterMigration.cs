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
            TBL_CLAUSEMASTER.CLAUSE_MASTER_ID,
            TBL_CLAUSEMASTER.DEFINATION AS TemplateName,
            (
                SELECT TBL_CLAUSETERMMASTER.TERMID 
                FROM TBL_CLAUSETERMMASTER 
                WHERE TBL_CLAUSETERMMASTER.TERMDESCRIPTION = TBL_CLAUSETRN.SUBCLAUSE
            ) AS TermId
        FROM TBL_CLAUSEMASTER 
        INNER JOIN TBL_CLAUSETRN ON TBL_CLAUSETRN.CLAUSE_MST_ID = TBL_CLAUSEMASTER.CLAUSE_MASTER_ID
        ORDER BY TBL_CLAUSEMASTER.CLAUSE_MASTER_ID";

    protected override string InsertQuery => @"
        INSERT INTO term_template_master (
            term_template_master_id,
            term_template_name,
            term_master_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @term_template_master_id,
            @term_template_name,
            @term_master_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (term_template_master_id) DO UPDATE SET
            term_template_name = EXCLUDED.term_template_name,
            term_master_id = EXCLUDED.term_master_id,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public TermTemplateMasterMigration(IConfiguration configuration, ILogger<TermTemplateMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // term_template_master_id
            "Direct", // term_template_name
            "Query",  // term_master_id
            "Fixed",  // created_by
            "Fixed",  // created_date
            "Fixed",  // modified_by
            "Fixed",  // modified_date
            "Fixed",  // is_deleted
            "Fixed",  // deleted_by
            "Fixed"   // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "CLAUSE_MASTER_ID", logic = "CLAUSE_MASTER_ID -> term_template_master_id (Primary key, autoincrement)", target = "term_template_master_id" },
            new { source = "DEFINATION", logic = "DEFINATION -> term_template_name (TermTemplateName - Primary mapping)", target = "term_template_name" },
            new { source = "REF_KEY", logic = "REF_KEY -> Used as fallback if DEFINATION is NULL", target = "-" },
            new { source = "term_master", logic = "term_master_id -> First available ID from term_master table", target = "term_master_id" },
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
        _logger.LogInformation("Starting Term Template Master migration...");

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

                var clauseMasterId = reader["CLAUSE_MASTER_ID"];
                var templateName = reader["TemplateName"];
                var termId = reader["TermId"];

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

                // Skip if TermId is NULL (no matching term found)
                if (termId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {clauseMasterIdValue} - TermId is NULL");
                    continue;
                }

                var record = new Dictionary<string, object>
                {
                    ["term_template_master_id"] = clauseMasterIdValue,
                    ["term_template_name"] = templateName ?? DBNull.Value,
                    ["term_master_id"] = termId,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
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
