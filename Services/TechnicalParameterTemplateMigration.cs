using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.Linq;

public class TechnicalParameterTemplateMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<TechnicalParameterTemplateMigration> _logger;

    public TechnicalParameterTemplateMigration(
        IConfiguration configuration,
        ILogger<TechnicalParameterTemplateMigration> logger)
        : base(configuration)
    {
        _logger = logger;
    }

    protected override string SelectQuery => @"
        SELECT 
            tps.TechParaSub_Id,
            tpm.Item_Id,
            tpm.Title,
            tps.ParaName,
            tps.IsMandatory
        FROM TBL_TECHPARAMAIN tpm
        INNER JOIN TBL_TECHPARASUB tps ON tpm.TechParaMain_Id = tps.TechParaMain_Id
        ORDER BY tpm.TechParaMain_Id, tps.TechParaSub_Id";

    protected override string InsertQuery => @"
        INSERT INTO technical_parameter_template (
            technical_parameter_template_id,
            item_id,
            template_name,
            technical_parameter_name,
            technical_parameter_mandatory
        ) VALUES (
            @technical_parameter_template_id,
            @item_id,
            @template_name,
            @technical_parameter_name,
            @technical_parameter_mandatory
        )
        ON CONFLICT (technical_parameter_template_id) 
        DO UPDATE SET
            item_id = EXCLUDED.item_id,
            template_name = EXCLUDED.template_name,
            technical_parameter_name = EXCLUDED.technical_parameter_name,
            technical_parameter_mandatory = EXCLUDED.technical_parameter_mandatory";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // technical_parameter_template_id
            "Direct", // item_id
            "Direct", // template_name
            "Direct", // technical_parameter_name
            "Boolean" // technical_parameter_mandatory
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "TechParaSub_Id", logic = "TechParaSub_Id -> technical_parameter_template_id (Primary key from TBL_TECHPARASUB)", target = "technical_parameter_template_id" },
            new { source = "Item_Id", logic = "Item_Id -> item_id (Foreign key reference)", target = "item_id" },
            new { source = "Title", logic = "Title -> template_name (Template name from TBL_TECHPARAMAIN)", target = "template_name" },
            new { source = "ParaName", logic = "ParaName -> technical_parameter_name (Parameter name from TBL_TECHPARASUB)", target = "technical_parameter_name" },
            new { source = "IsMandatory", logic = "IsMandatory -> technical_parameter_mandatory (Boolean: 1=true, other=false)", target = "technical_parameter_mandatory" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            _logger.LogInformation("Starting Technical Parameter Template migration...");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var techParaSubId = reader["TechParaSub_Id"];
                var itemId = reader["Item_Id"];
                var title = reader["Title"];
                var paraName = reader["ParaName"];
                var isMandatory = reader["IsMandatory"];

                // Skip if TechParaSub_Id is NULL
                if (techParaSubId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - TechParaSub_Id is NULL");
                    continue;
                }

                int techParaSubIdValue = Convert.ToInt32(techParaSubId);

                // Skip duplicates
                if (processedIds.Contains(techParaSubIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Convert IsMandatory to boolean (1 = true, other = false)
                bool isMandatoryValue = false;
                if (isMandatory != DBNull.Value)
                {
                    int mandatoryInt = Convert.ToInt32(isMandatory);
                    isMandatoryValue = mandatoryInt == 1;
                }

                var record = new Dictionary<string, object>
                {
                    ["technical_parameter_template_id"] = techParaSubIdValue,
                    ["item_id"] = itemId,
                    ["template_name"] = title ?? DBNull.Value,
                    ["technical_parameter_name"] = paraName ?? DBNull.Value,
                    ["technical_parameter_mandatory"] = isMandatoryValue
                };

                batch.Add(record);
                processedIds.Add(techParaSubIdValue);

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

            var message = $"Technical Parameter Template migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}";
            _logger.LogInformation(message);

            return migratedRecords;
        }
        catch (Exception ex)
        {
            var errorMessage = $"Error during Technical Parameter Template migration: {ex.Message}";
            _logger.LogError(ex, errorMessage);
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
