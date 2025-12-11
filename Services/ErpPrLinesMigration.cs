using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class ErpPrLinesMigration : MigrationService
{
    private const int BATCH_SIZE = 100;
    private readonly ILogger<ErpPrLinesMigration> _logger;
    private MigrationLogger? _migrationLogger;

    public ErpPrLinesMigration(IConfiguration configuration, ILogger<ErpPrLinesMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override string SelectQuery => @"
SELECT
    t.PRTRANSID,
    pm.IsNumberGeneration AS temp_pr,
    CASE WHEN u.PERSON_ID IS NOT NULL AND t.BUYERID <> 0 THEN t.BUYERID ELSE NULL END AS user_id,
    u.FULL_NAME AS user_full_name,
    t.PRStatus AS pr_status,
    pm.PR_NUM AS pr_number,
    t.PR_LINE AS pr_line,
    pm.DESCRIPTION AS header_text,
    t.ClientSAPId AS company_id,
    t.ClientSAPCode AS company_code,
    t.UOMId AS uom_id,
    t.UOMCODE AS uom_code,
    t.Plant AS plant_id,
    t.PlantCode AS plant_code,
    t.MaterialGroupId AS material_group_id,
    t.MaterialGroup AS material_group_code,
    t.PurchasingGroup AS purchase_group_id,
    t.PurchaseGroupCode AS purchase_group_code,
    t.ItemCode AS material_code,
    t.ItemName AS material_short_text,
    t.ITEM_DESCRIPTION AS material_item_text,
    CASE WHEN ISNULL(t.ItemCode, '') <> '' THEN 'Material' ELSE 'Service' END AS item_type,
    t.MaterialPODescription AS material_po_description,
    t.AMOUNT AS amount,
    t.UNIT_PRICE AS unit_price,
    ISNULL(t.RemQty, 0) AS rem_qty,
    t.QUANTITY AS qty,
    t.PONo AS po_number,
    t.PODate AS po_creation_date,
    t.POQty AS po_qty,
    t.POVendorCode AS po_vendor_code,
    t.POVendorName AS po_vendor_name,
    t.POTotalGrossAmount AS po_item_value,
    t.LastPONo AS lpo_number,
    t.POItemNo AS lpo_line_number,
    t.PODocType AS lpo_doc_type,
    t.LastPODate AS lpo_creation_date,
    t.POUom AS lpo_uom,
    t.POUnitPrice AS lpo_unit_price,
    t.POItemCurrency AS lpo_line_currency,
    t.VendorCode AS lpo_vendor_code,
    t.VendorName AS lpo_vendor_name,
    t.LastPODate AS lpo_date,
    t.LastPOQty AS lpo_qty,
    t.TotalStock AS total_stock,
    t.CostCenter AS cost_center,
    t.StoreLocation AS store_location,
    t.Department AS department,
    t.AcctAssignmentCat AS acct_assignment_cat,
    t.AcctAssignmentCatDesc AS acct_assignment_cat_desc,
    t.PROJECT_ID AS wbs_element_code,
    NULL AS wbs_element_name,
    t.CurrencyCode AS currency_code,
    t.TrackingNumber AS tracking_number,
    pm.CreatedBy AS erp_created_by,
    pm.RequestBy AS erp_request_by,
    t.RequestDate AS erp_change_on_date,
    t.DeliveryDate AS delivery_date,
    CASE WHEN t.IsClosed = 'X' THEN 1 ELSE 0 END AS is_closed,
    t.itemBlocked,
    CASE WHEN ISNULL(t.itemBlocked, '') = '' THEN 0 ELSE 1 END AS item_block,
    0 AS created_by,
    NULL AS created_date,
    0 AS modified_by,
    NULL AS modified_date,
    CASE WHEN t.DeletionIndicator = 'X' THEN 1 ELSE 0 END AS is_deleted,
    NULL AS deleted_by,
    NULL AS deleted_date
FROM TBL_PRTRANSACTION t
INNER JOIN TBL_PRMASTER pm ON pm.PRID = t.PRID
LEFT JOIN TBL_USERMASTERFINAL u ON u.PERSON_ID = t.BUYERID
WHERE ISNULL(pm.PR_NUM, '') <> ''
";

    protected override string InsertQuery => @"
INSERT INTO erp_pr_lines (
    erp_pr_lines_id, temp_pr, user_id, user_full_name, pr_status, pr_number, pr_line, header_text, company_id, company_code,
    uom_id, uom_code, plant_id, plant_code, material_group_id, material_group_code, purchase_group_id, purchase_group_code,
    material_code, material_short_text, material_item_text, item_type, material_po_description, amount, unit_price, rem_qty,
    qty, po_number, po_creation_date, po_qty, po_vendor_code, po_vendor_name, po_item_value, lpo_number, lpo_line_number, lpo_doc_type,
    lpo_creation_date, lpo_uom, lpo_unit_price, lpo_line_currency, lpo_vendor_code, lpo_vendor_name, lpo_date, lpo_qty,
    total_stock, cost_center, store_location, department, acct_assignment_cat, acct_assignment_cat_desc, wbs_element_code, wbs_element_name,
    currency_code, tracking_number, erp_created_by, erp_request_by, erp_change_on_date, delivery_date, is_closed, item_block, created_by,
    created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @erp_pr_lines_id, @temp_pr, @user_id, @user_full_name, @pr_status, @pr_number, @pr_line, @header_text, @company_id, @company_code,
    @uom_id, @uom_code, @plant_id, @plant_code, @material_group_id, @material_group_code, @purchase_group_id, @purchase_group_code,
    @material_code, @material_short_text, @material_item_text, @item_type, @material_po_description, @amount, @unit_price, @rem_qty,
    @qty, @po_number, @po_creation_date, @po_qty, @po_vendor_code, @po_vendor_name, @po_item_value, @lpo_number, @lpo_line_number, @lpo_doc_type,
    @lpo_creation_date, @lpo_uom, @lpo_unit_price, @lpo_line_currency, @lpo_vendor_code, @lpo_vendor_name, @lpo_date, @lpo_qty,
    @total_stock, @cost_center, @store_location, @department, @acct_assignment_cat, @acct_assignment_cat_desc, @wbs_element_code, @wbs_element_name,
    @currency_code, @tracking_number, @erp_created_by, @erp_request_by, @erp_change_on_date, @delivery_date, @is_closed, @item_block, @created_by,
    @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)";

    private async Task<HashSet<int>> LoadValidUomIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT uom_id FROM uom_master";
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        return validIds;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct",
        "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct",
        "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct",
        "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct",
        "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct",
        "Direct", "Direct", "Direct", "Direct", "Direct", "Conditional", "Direct", "Direct", "Direct", "Direct",
        "Direct", "Direct", "Direct", "Direct", "Direct", "Direct"
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PRTRANSID", logic = "Direct", target = "erp_pr_lines_id" },
            new { source = "pm.IsNumberGeneration", logic = "Direct", target = "temp_pr" },
            new { source = "t.BUYERID", logic = "Conditional", target = "user_id" },
            new { source = "u.FULL_NAME", logic = "Direct", target = "user_full_name" },
            new { source = "t.PRStatus", logic = "Direct", target = "pr_status" },
            new { source = "pm.PR_NUM", logic = "Direct", target = "pr_number" },
            new { source = "t.PR_LINE", logic = "Direct", target = "pr_line" },
            new { source = "pm.DESCRIPTION", logic = "Direct", target = "header_text" },
            new { source = "t.ClientSAPId", logic = "Direct", target = "company_id" },
            new { source = "t.ClientSAPCode", logic = "Direct", target = "company_code" },
            new { source = "t.UOMId", logic = "Direct", target = "uom_id" },
            new { source = "t.UOMCODE", logic = "Direct", target = "uom_code" },
            new { source = "t.Plant", logic = "Direct", target = "plant_id" },
            new { source = "t.PlantCode", logic = "Direct", target = "plant_code" },
            new { source = "t.MaterialGroupId", logic = "Direct", target = "material_group_id" },
            new { source = "t.MaterialGroup", logic = "Direct", target = "material_group_code" },
            new { source = "t.PurchasingGroup", logic = "Direct", target = "purchase_group_id" },
            new { source = "t.PurchaseGroupCode", logic = "Direct", target = "purchase_group_code" },
            new { source = "t.ItemCode", logic = "Direct", target = "material_code" },
            new { source = "t.ItemName", logic = "Direct", target = "material_short_text" },
            new { source = "t.ITEM_DESCRIPTION", logic = "Direct", target = "material_item_text" },
            new { source = "CASE WHEN ItemCode", logic = "Conditional", target = "item_type" },
            new { source = "t.MaterialPODescription", logic = "Direct", target = "material_po_description" },
            new { source = "t.AMOUNT", logic = "Direct", target = "amount" },
            new { source = "t.UNIT_PRICE", logic = "Direct", target = "unit_price" },
            new { source = "t.RemQty", logic = "Direct", target = "rem_qty" },
            new { source = "t.QUANTITY", logic = "Direct", target = "qty" },
            new { source = "t.PONo", logic = "Direct", target = "po_number" },
            new { source = "t.PODate", logic = "Direct", target = "po_creation_date" },
            new { source = "t.POQty", logic = "Direct", target = "po_qty" },
            new { source = "t.POVendorCode", logic = "Direct", target = "po_vendor_code" },
            new { source = "t.POVendorName", logic = "Direct", target = "po_vendor_name" },
            new { source = "t.POTotalGrossAmount", logic = "Direct", target = "po_item_value" },
            new { source = "t.LastPONo", logic = "Direct", target = "lpo_number" },
            new { source = "t.POItemNo", logic = "Direct", target = "lpo_line_number" },
            new { source = "t.PODocType", logic = "Direct", target = "lpo_doc_type" },
            new { source = "t.LastPODate", logic = "Direct", target = "lpo_creation_date" },
            new { source = "t.POUom", logic = "Direct", target = "lpo_uom" },
            new { source = "t.POUnitPrice", logic = "Direct", target = "lpo_unit_price" },
            new { source = "t.POItemCurrency", logic = "Direct", target = "lpo_line_currency" },
            new { source = "t.VendorCode", logic = "Direct", target = "lpo_vendor_code" },
            new { source = "t.VendorName", logic = "Direct", target = "lpo_vendor_name" },
            new { source = "t.LastPODate", logic = "Direct", target = "lpo_date" },
            new { source = "t.LastPOQty", logic = "Direct", target = "lpo_qty" },
            new { source = "t.TotalStock", logic = "Direct", target = "total_stock" },
            new { source = "t.CostCenter", logic = "Direct", target = "cost_center" },
            new { source = "t.StoreLocation", logic = "Direct", target = "store_location" },
            new { source = "t.Department", logic = "Direct", target = "department" },
            new { source = "t.AcctAssignmentCat", logic = "Direct", target = "acct_assignment_cat" },
            new { source = "t.AcctAssignmentCatDesc", logic = "Direct", target = "acct_assignment_cat_desc" },
            new { source = "t.PROJECT_ID", logic = "Direct", target = "wbs_element_code" },
            new { source = "NULL", logic = "Default", target = "wbs_element_name" },
            new { source = "t.CurrencyCode", logic = "Direct", target = "currency_code" },
            new { source = "t.TrackingNumber", logic = "Direct", target = "tracking_number" },
            new { source = "pm.CreatedBy", logic = "Direct", target = "erp_created_by" },
            new { source = "pm.RequestBy", logic = "Direct", target = "erp_request_by" },
            new { source = "t.RequestDate", logic = "Direct", target = "erp_change_on_date" },
            new { source = "t.DeliveryDate", logic = "Direct", target = "delivery_date" },
            new { source = "t.IsClosed", logic = "Conditional", target = "is_closed" },
            new { source = "t.itemBlocked", logic = "Conditional", target = "item_block" },
            new { source = "-", logic = "Default: 0", target = "created_by" },
            new { source = "-", logic = "Default: NULL", target = "created_date" },
            new { source = "-", logic = "Default: 0", target = "modified_by" },
            new { source = "-", logic = "Default: NULL", target = "modified_date" },
            new { source = "t.DeletionIndicator", logic = "Conditional", target = "is_deleted" },
            new { source = "-", logic = "Default: NULL", target = "deleted_by" },
            new { source = "-", logic = "Default: NULL", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        _migrationLogger = new MigrationLogger(_logger, "erp_pr_lines");
        _migrationLogger.LogInfo("Starting migration");

        var result = await base.MigrateAsync(useTransaction: true);

        _migrationLogger.LogInfo($"Completed: {_migrationLogger.InsertedCount} inserted, {_migrationLogger.SkippedCount} skipped");
        return result;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "erp_pr_lines");
        _migrationLogger.LogInfo("Starting migration");
        var validUomIds = await LoadValidUomIdsAsync(pgConn, transaction);
        _migrationLogger.LogInfo($"Loaded {validUomIds.Count} valid UOM IDs from uom_master.");
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var uomIdValue = reader["uom_id"];
            var prTransId = reader["PRTRANSID"]?.ToString() ?? "";
            if (uomIdValue != DBNull.Value)
            {
                int uomId = Convert.ToInt32(uomIdValue);
                if (uomId == 0)
                {
                    skippedCount++;
                    _migrationLogger.LogSkipped(
                        "UOM ID is 0",
                        $"PRTRANSID={prTransId}",
                        new Dictionary<string, object> { { "uom_id", uomId } }
                    );
                    continue;
                }
                if (!validUomIds.Contains(uomId))
                {
                    skippedCount++;
                    _migrationLogger.LogSkipped(
                        $"UOM ID {uomId} not found in uom_master",
                        $"PRTRANSID={prTransId}",
                        new Dictionary<string, object> { { "uom_id", uomId } }
                    );
                    continue;
                }
            }
            var record = new Dictionary<string, object>
            {
                ["erp_pr_lines_id"] = reader["PRTRANSID"] ?? DBNull.Value,
                ["temp_pr"] = reader["temp_pr"] ?? DBNull.Value,
                ["user_id"] = reader["user_id"] ?? DBNull.Value,
                ["user_full_name"] = reader["user_full_name"] ?? DBNull.Value,
                ["pr_status"] = reader["pr_status"] ?? DBNull.Value,
                ["pr_number"] = reader["pr_number"] ?? DBNull.Value,
                ["pr_line"] = reader["pr_line"] ?? DBNull.Value,
                ["header_text"] = reader["header_text"] ?? DBNull.Value,
                ["company_id"] = reader["company_id"] ?? DBNull.Value,
                ["company_code"] = reader["company_code"] ?? DBNull.Value,
                ["uom_id"] = reader["uom_id"] ?? DBNull.Value,
                ["uom_code"] = reader["uom_code"] ?? DBNull.Value,
                ["plant_id"] = reader["plant_id"] ?? DBNull.Value,
                ["plant_code"] = reader["plant_code"] ?? DBNull.Value,
                ["material_group_id"] = reader["material_group_id"] ?? DBNull.Value,
                ["material_group_code"] = reader["material_group_code"] ?? DBNull.Value,
                ["purchase_group_id"] = reader["purchase_group_id"] ?? DBNull.Value,
                ["purchase_group_code"] = reader["purchase_group_code"] ?? DBNull.Value,
                ["material_code"] = reader["material_code"] ?? DBNull.Value,
                ["material_short_text"] = reader["material_short_text"] ?? DBNull.Value,
                ["material_item_text"] = reader["material_item_text"] ?? DBNull.Value,
                ["item_type"] = reader["item_type"] ?? DBNull.Value,
                ["material_po_description"] = reader["material_po_description"] ?? DBNull.Value,
                ["amount"] = reader["amount"] ?? DBNull.Value,
                ["unit_price"] = reader["unit_price"] ?? DBNull.Value,
                ["rem_qty"] = reader["rem_qty"] ?? DBNull.Value,
                ["qty"] = reader["qty"] ?? DBNull.Value,
                ["po_number"] = reader["po_number"] ?? DBNull.Value,
                ["po_creation_date"] = reader["po_creation_date"] ?? DBNull.Value,
                ["po_qty"] = reader["po_qty"] ?? DBNull.Value,
                ["po_vendor_code"] = reader["po_vendor_code"] ?? DBNull.Value,
                ["po_vendor_name"] = reader["po_vendor_name"] ?? DBNull.Value,
                ["po_item_value"] = reader["po_item_value"] ?? DBNull.Value,
                ["lpo_number"] = reader["lpo_number"] ?? DBNull.Value,
                ["lpo_line_number"] = reader["lpo_line_number"] ?? DBNull.Value,
                ["lpo_doc_type"] = reader["lpo_doc_type"] ?? DBNull.Value,
                ["lpo_creation_date"] = reader["lpo_creation_date"] ?? DBNull.Value,
                ["lpo_uom"] = reader["lpo_uom"] ?? DBNull.Value,
                ["lpo_unit_price"] = reader["lpo_unit_price"] ?? DBNull.Value,
                ["lpo_line_currency"] = reader["lpo_line_currency"] ?? DBNull.Value,
                ["lpo_vendor_code"] = reader["lpo_vendor_code"] ?? DBNull.Value,
                ["lpo_vendor_name"] = reader["lpo_vendor_name"] ?? DBNull.Value,
                ["lpo_date"] = reader["lpo_date"] ?? DBNull.Value,
                ["lpo_qty"] = reader["lpo_qty"] ?? DBNull.Value,
                ["total_stock"] = reader["total_stock"] ?? DBNull.Value,
                ["cost_center"] = reader["cost_center"] ?? DBNull.Value,
                ["store_location"] = reader["store_location"] ?? DBNull.Value,
                ["department"] = reader["department"] ?? DBNull.Value,
                ["acct_assignment_cat"] = reader["acct_assignment_cat"] ?? DBNull.Value,
                ["acct_assignment_cat_desc"] = reader["acct_assignment_cat_desc"] ?? DBNull.Value,
                ["wbs_element_code"] = reader["wbs_element_code"] ?? DBNull.Value,
                ["wbs_element_name"] = reader["wbs_element_name"] ?? DBNull.Value,
                ["currency_code"] = reader["currency_code"] ?? DBNull.Value,
                ["tracking_number"] = reader["tracking_number"] ?? DBNull.Value,
                ["erp_created_by"] = reader["erp_created_by"] ?? DBNull.Value,
                ["erp_request_by"] = reader["erp_request_by"] ?? DBNull.Value,
                ["erp_change_on_date"] = reader["erp_change_on_date"] ?? DBNull.Value,
                ["delivery_date"] = reader["delivery_date"] ?? DBNull.Value,
                ["is_closed"] = Convert.ToInt32(reader["is_closed"]) == 1,
                ["item_block"] = Convert.ToInt32(reader["item_block"]) == 1,
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
                _migrationLogger.LogInfo($"Starting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                _migrationLogger.LogInfo($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _migrationLogger.LogInfo($"Starting batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            _migrationLogger.LogInfo($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
        }

        _migrationLogger.LogInfo($"Migration finished. Total records inserted: {insertedCount}, Skipped: {skippedCount}");

        // Export migration stats to Excel
        var skippedLogEntries = _migrationLogger?.GetSkippedRecords() ?? new List<MigrationLogEntry>();
        var skippedRecords = skippedLogEntries.Select(e => (e.RecordIdentifier, e.Message)).ToList();
        var excelPath = Path.Combine("migration_outputs", $"ErpPrLinesMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
        MigrationStatsExporter.ExportToExcel(
            excelPath,
            insertedCount + skippedCount,
            insertedCount,
            skippedCount,
            _logger,
            skippedRecords
        );
        _logger.LogInformation($"Migration stats exported to {excelPath}");

        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        var columns = new List<string> {
            "erp_pr_lines_id", "temp_pr", "user_id", "user_full_name", "pr_status", "pr_number", "pr_line", "header_text", 
            "company_id", "company_code", "uom_id", "uom_code", "plant_id", "plant_code", "material_group_id", "material_group_code", 
            "purchase_group_id", "purchase_group_code", "material_code", "material_short_text", "material_item_text", "item_type", 
            "material_po_description", "amount", "unit_price", "rem_qty", "qty", "po_number", "po_creation_date", "po_qty", 
            "po_vendor_code", "po_vendor_name", "po_item_value", "lpo_number", "lpo_line_number", "lpo_doc_type", "lpo_creation_date", 
            "lpo_uom", "lpo_unit_price", "lpo_line_currency", "lpo_vendor_code", "lpo_vendor_name", "lpo_date", "lpo_qty", 
            "total_stock", "cost_center", "store_location", "department", "acct_assignment_cat", "acct_assignment_cat_desc", 
            "wbs_element_code", "wbs_element_name", "currency_code", "tracking_number", "erp_created_by", "erp_request_by", 
            "erp_change_on_date", "delivery_date", "is_closed", "item_block", "created_by", "created_date", "modified_by", "modified_date", 
            "is_deleted", "deleted_by", "deleted_date"
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
                parameters.Add(new NpgsqlParameter(paramName, record[col]));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }

        var sql = $"INSERT INTO erp_pr_lines ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _migrationLogger?.LogInfo($"Batch {batchNumber}: Inserted {result} records into erp_pr_lines.");
        foreach (var record in batch)
        {
            var id = record.ContainsKey("erp_pr_lines_id") ? record["erp_pr_lines_id"]?.ToString() : null;
            if (!string.IsNullOrEmpty(id))
                _migrationLogger?.LogInserted($"PRTRANSID={id}");
        }
        return result;
    }
}
