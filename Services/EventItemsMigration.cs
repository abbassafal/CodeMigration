using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class EventItemsMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<EventItemsMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    PBID,
    EVENTID,
    PRTRANSID,
    SEQUENCEID,
    QTY,
    UNITPRICE,
    AMOUNT,
    ExtChargeHeader1,
    ExtChargeHeader2,
    ExtChargeHeader3,
    ExtChargeHeader4,
    ExtChargeHeader5,
    ExtChargeHeader6,
    ExtChargeHeader7,
    ExtChargeHeader8,
    ExtChargeHeader9,
    ExtChargeHeader10,
    HEADER1,
    HEADER2,
    HEADER3,
    HEADER4,
    HEADER5,
    HEADER6,
    HEADER7,
    HEADER8,
    HEADER9,
    HEADER10,
    TechRemarks,
    PBType,
    Remarks,
    BasePriceGms,
    IBIASiteDate,
    PPOId,
    BasicPrice,
    BasicDiscountPer,
    TDSPer,
    LotNo,
    ClientSAPId
FROM TBL_PB_BUYER
";

    protected override string InsertQuery => @"
INSERT INTO event_items (
    event_item_id, event_id, erp_pr_lines_id, material_code, material_short_text, 
    material_item_text, material_po_description, uom_id, uom_code, company_id, 
    qty, item_type, created_by, created_date, modified_by, modified_date, 
    is_deleted, deleted_by, deleted_date, items_status, recalled_date, 
    recalled_for_partial_qty, recalled_remarks
) VALUES (
    @event_item_id, @event_id, @erp_pr_lines_id, @material_code, @material_short_text, 
    @material_item_text, @material_po_description, @uom_id, @uom_code, @company_id, 
    @qty, @item_type, @created_by, @created_date, @modified_by, @modified_date, 
    @is_deleted, @deleted_by, @deleted_date, @items_status, @recalled_date, 
    @recalled_for_partial_qty, @recalled_remarks
)
ON CONFLICT (event_item_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    erp_pr_lines_id = EXCLUDED.erp_pr_lines_id,
    material_code = EXCLUDED.material_code,
    material_short_text = EXCLUDED.material_short_text,
    material_item_text = EXCLUDED.material_item_text,
    material_po_description = EXCLUDED.material_po_description,
    uom_id = EXCLUDED.uom_id,
    uom_code = EXCLUDED.uom_code,
    company_id = EXCLUDED.company_id,
    qty = EXCLUDED.qty,
    item_type = EXCLUDED.item_type,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date,
    items_status = EXCLUDED.items_status,
    recalled_date = EXCLUDED.recalled_date,
    recalled_for_partial_qty = EXCLUDED.recalled_for_partial_qty,
    recalled_remarks = EXCLUDED.recalled_remarks";

    public EventItemsMigration(IConfiguration configuration, ILogger<EventItemsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // event_item_id
        "Direct", // event_id
        "Direct", // erp_pr_lines_id
        "Lookup", // material_code (from erp_pr_lines)
        "Lookup", // material_short_text (from erp_pr_lines)
        "Lookup", // material_item_text (from erp_pr_lines)
        "Lookup", // material_po_description (from erp_pr_lines)
        "Lookup", // uom_id (from uom_master)
        "Lookup", // uom_code (from erp_pr_lines)
        "Direct", // company_id
        "Direct", // qty
        "Fixed",  // item_type
        "Fixed",  // created_by
        "Fixed",  // created_date
        "Fixed",  // modified_by
        "Fixed",  // modified_date
        "Fixed",  // is_deleted
        "Fixed",  // deleted_by
        "Fixed",  // deleted_date
        "Fixed",  // items_status
        "Fixed",  // recalled_date
        "Fixed",  // recalled_for_partial_qty
        "Fixed"   // recalled_remarks
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PBID", logic = "PBID -> event_item_id (Direct)", target = "event_item_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Direct from EventMaster)", target = "event_id" },
            new { source = "PRTRANSID", logic = "PRTRANSID -> erp_pr_lines_id (Direct from ErpPrLines)", target = "erp_pr_lines_id" },
            new { source = "erp_pr_lines", logic = "MaterialCode from erp_pr_lines (Lookup)", target = "material_code" },
            new { source = "erp_pr_lines", logic = "MaterialShortText from erp_pr_lines (Lookup)", target = "material_short_text" },
            new { source = "erp_pr_lines", logic = "MaterialItemText from erp_pr_lines (Lookup)", target = "material_item_text" },
            new { source = "erp_pr_lines", logic = "MaterialPoDescription from erp_pr_lines (Lookup)", target = "material_po_description" },
            new { source = "uom_master", logic = "UomId from uom_master (Lookup)", target = "uom_id" },
            new { source = "erp_pr_lines", logic = "UomCode from erp_pr_lines (Lookup)", target = "uom_code" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Direct from Clinet SAP Master)", target = "company_id" },
            new { source = "QTY", logic = "QTY -> qty (Direct)", target = "qty" },
            new { source = "-", logic = "item_type -> text (Fixed Default)", target = "item_type" },
            new { source = "-", logic = "created_by -> NULL (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" },
            new { source = "-", logic = "items_status -> NULL (Fixed Default)", target = "items_status" },
            new { source = "-", logic = "recalled_date -> NULL (Fixed Default)", target = "recalled_date" },
            new { source = "-", logic = "recalled_for_partial_qty -> NULL (Fixed Default)", target = "recalled_for_partial_qty" },
            new { source = "-", logic = "recalled_remarks -> NULL (Fixed Default)", target = "recalled_remarks" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting EventItems migration...");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        // Load lookup data
        var erpPrLinesData = await LoadErpPrLinesDataAsync(pgConn, transaction);
        var uomMasterData = await LoadUomMasterDataAsync(pgConn, transaction);
        
        _logger.LogInformation($"Loaded {erpPrLinesData.Count} ERP PR Lines records and {uomMasterData.Count} UOM records for lookup.");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var pbId = reader["PBID"] ?? DBNull.Value;
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var prTransId = reader["PRTRANSID"] ?? DBNull.Value;
            
            // Validate required foreign keys
            if (eventId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping PBID {pbId}: EVENTID is NULL.");
                skippedCount++;
                continue;
            }
            
            if (prTransId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping PBID {pbId}: PRTRANSID is NULL.");
                skippedCount++;
                continue;
            }

            // Lookup material data from erp_pr_lines
            int prTransIdValue = Convert.ToInt32(prTransId);
            string? materialCode = null;
            string? materialShortText = null;
            string? materialItemText = null;
            string? materialPoDescription = null;
            string? uomCode = null;
            int? uomId = null;

            if (erpPrLinesData.ContainsKey(prTransIdValue))
            {
                var prLineData = erpPrLinesData[prTransIdValue];
                materialCode = prLineData.MaterialCode;
                materialShortText = prLineData.MaterialShortText;
                materialItemText = prLineData.MaterialItemText;
                materialPoDescription = prLineData.MaterialPoDescription;
                uomCode = prLineData.UomCode;
                
                // Lookup UOM ID
                if (!string.IsNullOrEmpty(uomCode) && uomMasterData.ContainsKey(uomCode))
                {
                    uomId = uomMasterData[uomCode];
                }
            }

            var qty = reader["QTY"] ?? DBNull.Value;
            var clientSapId = reader["ClientSAPId"] ?? DBNull.Value;

            var record = new Dictionary<string, object>
            {
                ["event_item_id"] = pbId,
                ["event_id"] = eventId,
                ["erp_pr_lines_id"] = prTransId,
                ["material_code"] = materialCode != null ? (object)materialCode : DBNull.Value,
                ["material_short_text"] = materialShortText != null ? (object)materialShortText : DBNull.Value,
                ["material_item_text"] = materialItemText != null ? (object)materialItemText : DBNull.Value,
                ["material_po_description"] = materialPoDescription != null ? (object)materialPoDescription : DBNull.Value,
                ["uom_id"] = uomId.HasValue ? (object)uomId.Value : DBNull.Value,
                ["uom_code"] = uomCode != null ? (object)uomCode : DBNull.Value,
                ["company_id"] = clientSapId,
                ["qty"] = qty,
                ["item_type"] = "text",
                ["created_by"] = DBNull.Value,
                ["created_date"] = DBNull.Value,
                ["modified_by"] = DBNull.Value,
                ["modified_date"] = DBNull.Value,
                ["is_deleted"] = false,
                ["deleted_by"] = DBNull.Value,
                ["deleted_date"] = DBNull.Value,
                ["items_status"] = DBNull.Value,
                ["recalled_date"] = DBNull.Value,
                ["recalled_for_partial_qty"] = DBNull.Value,
                ["recalled_remarks"] = DBNull.Value
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Inserting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Inserting final batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
        }

        _logger.LogInformation($"EventItems migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<Dictionary<int, ErpPrLineData>> LoadErpPrLinesDataAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var data = new Dictionary<int, ErpPrLineData>();
        var query = @"SELECT erp_pr_lines_id, material_code, material_short_text, material_item_text, 
                      material_po_description, uom_code FROM erp_pr_lines";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32(0);
            data[id] = new ErpPrLineData
            {
                MaterialCode = reader.IsDBNull(1) ? null : reader.GetString(1),
                MaterialShortText = reader.IsDBNull(2) ? null : reader.GetString(2),
                MaterialItemText = reader.IsDBNull(3) ? null : reader.GetString(3),
                MaterialPoDescription = reader.IsDBNull(4) ? null : reader.GetString(4),
                UomCode = reader.IsDBNull(5) ? null : reader.GetString(5)
            };
        }
        
        return data;
    }

    private async Task<Dictionary<string, int>> LoadUomMasterDataAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var data = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var query = "SELECT uom_id, uom_code FROM uom_master";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var uomId = reader.GetInt32(0);
            var uomCode = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (!string.IsNullOrEmpty(uomCode))
            {
                data[uomCode] = uomId;
            }
        }
        
        return data;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        // Deduplicate by event_item_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["event_item_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate event_item_id records.");
        }

        var columns = new List<string> {
            "event_item_id", "event_id", "erp_pr_lines_id", "material_code", "material_short_text",
            "material_item_text", "material_po_description", "uom_id", "uom_code", "company_id",
            "qty", "item_type", "created_by", "created_date", "modified_by", "modified_date",
            "is_deleted", "deleted_by", "deleted_date", "items_status", "recalled_date",
            "recalled_for_partial_qty", "recalled_remarks"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in deduplicatedBatch)
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

        var updateColumns = columns.Where(c => c != "event_item_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO event_items ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (event_item_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }

    private class ErpPrLineData
    {
        public string? MaterialCode { get; set; }
        public string? MaterialShortText { get; set; }
        public string? MaterialItemText { get; set; }
        public string? MaterialPoDescription { get; set; }
        public string? UomCode { get; set; }
    }
}
