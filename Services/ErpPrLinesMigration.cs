using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class ErpPrLinesMigration : MigrationService
{
    private const int BATCH_SIZE = 100; // Reduced to avoid 65535 parameter limit (67 columns x 100 rows = 6700 parameters)
    private readonly ILogger<ErpPrLinesMigration> _logger;

    // 1. The SQL query covers all fields and lookups as per your mapping.
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
    t.RemQty AS rem_qty,
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
    NULL AS wbs_element_name, -- Placeholder. Add join/lookup if needed.
    t.CurrencyCode AS currency_code,
    t.TrackingNumber AS tracking_number,
    pm.CreatedBy AS erp_created_by,
    pm.RequestBy AS erp_request_by,
    t.RequestDate AS erp_change_on_date,
    t.DeliveryDate AS delivery_date,
    CASE WHEN t.IsClosed = 'X' THEN 1 ELSE 0 END AS is_closed,
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

    // 2. The Postgres insert covers all fields as per your mapping
    protected override string InsertQuery => @"
INSERT INTO erp_pr_lines (
    erp_pr_lines_id, temp_pr, user_id, user_full_name, pr_status, pr_number, pr_line, header_text, company_id, company_code,
    uom_id, uom_code, plant_id, plant_code, material_group_id, material_group_code, purchase_group_id, purchase_group_code,
    material_code, material_short_text, material_item_text, item_type, material_po_description, amount, unit_price, rem_qty,
    qty, po_number, po_creation_date, po_qty, po_vendor_code, po_vendor_name, po_item_value, lpo_number, lpo_line_number, lpo_doc_type,
    lpo_creation_date, lpo_uom, lpo_unit_price, lpo_line_currency, lpo_vendor_code, lpo_vendor_name, lpo_date, lpo_qty,
    total_stock, cost_center, store_location, department, acct_assignment_cat, acct_assignment_cat_desc, wbs_element_code, wbs_element_name,
    currency_code, tracking_number, erp_created_by, erp_request_by, erp_change_on_date, delivery_date, is_closed, created_by,
    created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @erp_pr_lines_id, @temp_pr, @user_id, @user_full_name, @pr_status, @pr_number, @pr_line, @header_text, @company_id, @company_code,
    @uom_id, @uom_code, @plant_id, @plant_code, @material_group_id, @material_group_code, @purchase_group_id, @purchase_group_code,
    @material_code, @material_short_text, @material_item_text, @item_type, @material_po_description, @amount, @unit_price, @rem_qty,
    @qty, @po_number, @po_creation_date, @po_qty, @po_vendor_code, @po_vendor_name, @po_item_value, @lpo_number, @lpo_line_number, @lpo_doc_type,
    @lpo_creation_date, @lpo_uom, @lpo_unit_price, @lpo_line_currency, @lpo_vendor_code, @lpo_vendor_name, @lpo_date, @lpo_qty,
    @total_stock, @cost_center, @store_location, @department, @acct_assignment_cat, @acct_assignment_cat_desc, @wbs_element_code, @wbs_element_name,
    @currency_code, @tracking_number, @erp_created_by, @erp_request_by, @erp_change_on_date, @delivery_date, @is_closed, @created_by,
    @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)";

    public ErpPrLinesMigration(IConfiguration configuration, ILogger<ErpPrLinesMigration> logger) : base(configuration) 
    {
        _logger = logger;
    }

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
        "Direct", // erp_pr_lines_id
        "Direct", // temp_pr
        "Direct", // user_id
        "Direct", // user_full_name
        "Direct", // pr_status
        "Direct", // pr_number
        "Direct", // pr_line
        "Direct", // header_text
        "Direct", // company_id
        "Direct", // company_code
        "Direct", // uom_id
        "Direct", // uom_code
        "Direct", // plant_id
        "Direct", // plant_code
        "Direct", // material_group_id
        "Direct", // material_group_code
        "Direct", // purchase_group_id
        "Direct", // purchase_group_code
        "Direct", // material_code
        "Direct", // material_short_text
        "Direct", // material_item_text
        "Direct", // item_type
        "Direct", // material_po_description
        "Direct", // amount
        "Direct", // unit_price
        "Direct", // rem_qty
        "Direct", // qty
        "Direct", // po_number
        "Direct", // po_creation_date
        "Direct", // po_qty
        "Direct", // po_vendor_code
        "Direct", // po_vendor_name
        "Direct", // po_item_value
        "Direct", // lpo_number
        "Direct", // lpo_line_number
        "Direct", // lpo_doc_type
        "Direct", // lpo_creation_date
        "Direct", // lpo_uom
        "Direct", // lpo_unit_price
        "Direct", // lpo_line_currency
        "Direct", // lpo_vendor_code
        "Direct", // lpo_vendor_name
        "Direct", // lpo_date
        "Direct", // lpo_qty
        "Direct", // total_stock
        "Direct", // cost_center
        "Direct", // store_location
        "Direct", // department
        "Direct", // acct_assignment_cat
        "Direct", // acct_assignment_cat_desc
        "Direct", // wbs_element_code
        "Direct", // wbs_element_name
        "Direct", // currency_code
        "Direct", // tracking_number
        "Direct", // erp_created_by
        "Direct", // erp_request_by
        "Direct", // erp_change_on_date
        "Direct", // delivery_date
        "Direct", // is_closed
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
        // Load valid UOM IDs
        var validUomIds = await LoadValidUomIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validUomIds.Count} valid UOM IDs from uom_master.");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();
        
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            // Validate UOM ID
            var uomIdValue = reader["uom_id"];
            if (uomIdValue != DBNull.Value)
            {
                int uomId = Convert.ToInt32(uomIdValue);
                
                // Skip if UOM ID is 0
                if (uomId == 0)
                {
                    _logger.LogWarning($"Skipping PRTRANSID {reader["PRTRANSID"]}: UOM ID is 0.");
                    skippedCount++;
                    continue;
                }
                
                // Skip if UOM ID not present in uom_master
                if (!validUomIds.Contains(uomId))
                {
                    _logger.LogWarning($"Skipping PRTRANSID {reader["PRTRANSID"]}: UOM ID {uomId} not found in uom_master.");
                    skippedCount++;
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
        
        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}, Skipped: {skippedCount}");
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
            "erp_change_on_date", "delivery_date", "is_closed", "created_by", "created_date", "modified_by", "modified_date", 
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
        _logger.LogInformation($"Batch {batchNumber}: Inserted {result} records into erp_pr_lines.");
        return result;
    }
}