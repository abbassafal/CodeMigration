using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class PRBoqItemsMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<PRBoqItemsMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    ItemId,
    PRID,
    PRTRANSID,
    ICode,
    IName,
    IUOM,
    IQty,
    IRate,
    Remarks,
    ICurrency,
    Line_Number,
    Prise_Unit,
    Net_Value_In_Document_Currency,
    Material_Group,
    Serial_Number_For_Preq_Account,
    GL_Account,
    COST_CENTER,
    LONG_TEXT,
    NETWORK,
    WBS_ELEMENT,
    0 AS created_by,
    NULL AS created_date,
    0 AS modified_by,
    NULL AS modified_date,
    0 AS is_deleted,
    NULL AS deleted_by,
    NULL AS deleted_date
FROM tbl_PRBOQItems
";

    protected override string InsertQuery => @"
INSERT INTO pr_boq_items (
    pr_boq_id, erp_pr_lines_id, pr_boq_material_code, pr_boq_name, pr_boq_description, 
    pr_boq_rem_qty, pr_boq_status, pr_boq_uom_code, pr_boq_qty, pr_boq_rate, 
    pr_boq_remark, pr_boq_currency, pr_boq_line_number, pr_boq_unit_price, pr_boq_total_value, 
    pr_boq_material_group, pr_boq_serial_number_for_preq_account, pr_boq_gl_account, pr_boq_cost_center, 
    pr_boq_long_text, pr_boq_network, pr_boq_wbs_element, created_by, created_date, 
    modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @pr_boq_id, @erp_pr_lines_id, @pr_boq_material_code, @pr_boq_name, @pr_boq_description, 
    @pr_boq_rem_qty, @pr_boq_status, @pr_boq_uom_code, @pr_boq_qty, @pr_boq_rate, 
    @pr_boq_remark, @pr_boq_currency, @pr_boq_line_number, @pr_boq_unit_price, @pr_boq_total_value, 
    @pr_boq_material_group, @pr_boq_serial_number_for_preq_account, @pr_boq_gl_account, @pr_boq_cost_center, 
    @pr_boq_long_text, @pr_boq_network, @pr_boq_wbs_element, @created_by, @created_date, 
    @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)";

    public PRBoqItemsMigration(IConfiguration configuration, ILogger<PRBoqItemsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // pr_boq_id
        "Direct", // erp_pr_lines_id
        "Direct", // pr_boq_material_code
        "Direct", // pr_boq_name
        "Direct", // pr_boq_description
        "Direct", // pr_boq_rem_qty
        "Direct", // pr_boq_status
        "Direct", // pr_boq_uom_code
        "Direct", // pr_boq_qty
        "Direct", // pr_boq_rate
        "Direct", // pr_boq_remark
        "Direct", // pr_boq_currency
        "Direct", // pr_boq_line_number
        "Direct", // pr_boq_unit_price
        "Direct", // pr_boq_total_value
        "Direct", // pr_boq_material_group
        "Direct", // pr_boq_serial_number_for_preq_account
        "Direct", // pr_boq_gl_account
        "Direct", // pr_boq_cost_center
        "Direct", // pr_boq_long_text
        "Direct", // pr_boq_network
        "Direct", // pr_boq_wbs_element
        "Direct", // created_by
        "Direct", // created_date
        "Direct", // modified_by
        "Direct", // modified_date
        "Direct", // is_deleted
        "Direct", // deleted_by
        "Direct"  // deleted_date
    };

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        int insertedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var record = new Dictionary<string, object>
            {
                ["pr_boq_id"] = reader["ItemId"] ?? DBNull.Value,
                ["erp_pr_lines_id"] = reader["PRID"] ?? DBNull.Value,
                ["pr_boq_material_code"] = reader["ICode"] ?? DBNull.Value,
                ["pr_boq_name"] = reader["IName"] ?? DBNull.Value,
                ["pr_boq_description"] = DBNull.Value, // New column - not in source
                ["pr_boq_rem_qty"] = DBNull.Value, // New column - not in source
                ["pr_boq_status"] = DBNull.Value, // New column - not in source
                ["pr_boq_uom_code"] = reader["IUOM"] ?? DBNull.Value,
                ["pr_boq_qty"] = reader["IQty"] ?? DBNull.Value,
                ["pr_boq_rate"] = reader["IRate"] ?? DBNull.Value,
                ["pr_boq_remark"] = reader["Remarks"] ?? DBNull.Value,
                ["pr_boq_currency"] = reader["ICurrency"] ?? DBNull.Value,
                ["pr_boq_line_number"] = reader["Line_Number"] ?? DBNull.Value,
                ["pr_boq_unit_price"] = reader["Prise_Unit"] ?? DBNull.Value,
                ["pr_boq_total_value"] = reader["Net_Value_In_Document_Currency"] ?? DBNull.Value,
                ["pr_boq_material_group"] = reader["Material_Group"] ?? DBNull.Value,
                ["pr_boq_serial_number_for_preq_account"] = reader["Serial_Number_For_Preq_Account"] ?? DBNull.Value,
                ["pr_boq_gl_account"] = reader["GL_Account"] ?? DBNull.Value,
                ["pr_boq_cost_center"] = reader["COST_CENTER"] ?? DBNull.Value,
                ["pr_boq_long_text"] = reader["LONG_TEXT"] ?? DBNull.Value,
                ["pr_boq_network"] = reader["NETWORK"] ?? DBNull.Value,
                ["pr_boq_wbs_element"] = reader["WBS_ELEMENT"] ?? DBNull.Value,
                ["created_by"] = reader["created_by"] ?? DBNull.Value,
                ["created_date"] = reader["created_date"] ?? DBNull.Value,
                ["modified_by"] = reader["modified_by"] ?? DBNull.Value,
                ["modified_date"] = reader["modified_date"] ?? DBNull.Value,
                ["is_deleted"] = Convert.ToInt32(reader["is_deleted"]) == 1,
                ["deleted_by"] = reader["deleted_by"] ?? DBNull.Value,
                ["deleted_date"] = reader["deleted_date"] ?? DBNull.Value
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
        }

        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}");
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        var columns = new List<string> {
            "pr_boq_id", "erp_pr_lines_id", "pr_boq_material_code", "pr_boq_name", "pr_boq_description",
            "pr_boq_rem_qty", "pr_boq_status", "pr_boq_uom_code", "pr_boq_qty", "pr_boq_rate",
            "pr_boq_remark", "pr_boq_currency", "pr_boq_line_number", "pr_boq_unit_price", "pr_boq_total_value",
            "pr_boq_material_group", "pr_boq_serial_number_for_preq_account", "pr_boq_gl_account", "pr_boq_cost_center",
            "pr_boq_long_text", "pr_boq_network", "pr_boq_wbs_element", "created_by", "created_date",
            "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in batch)
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

        var sql = $"INSERT INTO pr_boq_items ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {result} records into pr_boq_items.");
        return result;
    }
}
