using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class NfaLineMigration : MigrationService
{
    private const int BATCH_SIZE = 200;
    private readonly ILogger<NfaLineMigration> _logger;
    private MigrationLogger? _migrationLogger;

    public NfaLineMigration(IConfiguration configuration, ILogger<NfaLineMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override string SelectQuery => @"
        SELECT
            TBL_AWARDEVENTITEM.AWARDEVENTITEM,
            TBL_AWARDEVENTITEM.AWARDEVENTMAINID,
            TBL_AWARDEVENTITEM.PRTRANSID,
            TBL_AWARDEVENTITEM.UOM,
            TBL_AWARDEVENTITEM.EVENTQTY,
            TBL_AWARDEVENTITEM.EVENTPRICE,
            TBL_AWARDEVENTITEM.TOTALAMOUNT,
            TBL_AWARDEVENTITEM.ASSIGNQTY,
            TBL_AWARDEVENTITEM.ENTBY,
            TBL_AWARDEVENTITEM.ENTDATE,
            TBL_AWARDEVENTITEM.IsApprove,
            TBL_AWARDEVENTITEM.ApprovedBy,
            TBL_AWARDEVENTITEM.ApprovedDate,
            TBL_AWARDEVENTITEM.BuyerCSRemarks,
            TBL_AWARDEVENTITEM.TotalOtherCharges,
            TBL_AWARDEVENTITEM.AwardStatus,
            TBL_AWARDEVENTITEM.DiscountPer,
            TBL_AWARDEVENTITEM.GSTPer,
            TBL_AWARDEVENTITEM.POConditionTypeId,
            TBL_AWARDEVENTITEM.TaxCode_Master_Id,
            TBL_AWARDEVENTITEM.DeliveryDate,
            TBL_AWARDEVENTITEM.ItemDescription,
            TBL_AWARDEVENTITEM.ItemText,
            TBL_AWARDEVENTITEM.ReferenceNo,
            TBL_AWARDEVENTITEM.AWARDEVENTITEMREF,
            TBL_AWARDEVENTITEM.ARCSubId,
            TBL_AWARDEVENTITEM.PRtoARCPOSubId,
            TBL_AWARDEVENTITEM.ValuationTypeId,
            TBL_AWARDEVENTITEM.ItemId,
            TBL_AWARDEVENTITEM.ItemCode,
            TBL_AWARDEVENTITEM.ItemName,
            TBL_AWARDEVENTITEM.Uomid,
            TBL_AWARDEVENTITEM.POReferanceID,
            TBL_AWARDEVENTITEM.POSubReferanceID,
            TBL_AWARDEVENTITEM.isPRMode,
            TBL_AWARDEVENTITEM.isItemMode,
            TBL_AWARDEVENTMAIN.EventId,
            TBL_PB_BUYER.PBID AS EVENTITEMSID,
            (SELECT TOP 1 TaxCode_Master_Id FROM TBL_AwardEventTaxCode WHERE TBL_AwardEventTaxCode.AwardEventItemId = TBL_AWARDEVENTITEM.AWARDEVENTITEM) AS TaxMasterId
        FROM TBL_AWARDEVENTITEM
        INNER JOIN TBL_AWARDEVENTMAIN ON TBL_AWARDEVENTMAIN.AWARDEVENTMAINID = TBL_AWARDEVENTITEM.AWARDEVENTMAINID
        LEFT JOIN TBL_PB_BUYER ON TBL_PB_BUYER.EVENTID = TBL_AWARDEVENTMAIN.EventId AND TBL_AWARDEVENTITEM.PRTRANSID = TBL_PB_BUYER.PRTRANSID
        ORDER BY TBL_AWARDEVENTITEM.AWARDEVENTITEM";

    protected override string InsertQuery => @"
        INSERT INTO nfa_line (
            nfa_line_id,
            nfa_header_id,
            event_id,
            erp_pr_lines_id,
            uom_code,
            event_qty,
            unit_price,
            item_total,
            nfa_qty,
            discount_percentage,
            final_unit_price,
            tax_percentage,
            tax_amount,
            tax_code_id,
            item_delivery_date,
            item_description,
            item_text,
            arc_lines_id,
            arcpo_line_id,
            valuation_type_id,
            material_code,
            material_name,
            uom_id,
            repeat_poid,
            repeat_po_line_id,
            event_item_id,
            po_condition_id,
            tax_master_id,
            saving_at_lpo,
            saving_first_bid_vs_last_bid,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @nfa_line_id,
            @nfa_header_id,
            @event_id,
            @erp_pr_lines_id,
            @uom_code,
            @event_qty,
            @unit_price,
            @item_total,
            @nfa_qty,
            @discount_percentage,
            @final_unit_price,
            @tax_percentage,
            @tax_amount,
            @tax_code_id,
            @item_delivery_date,
            @item_description,
            @item_text,
            @arc_lines_id,
            @arcpo_line_id,
            @valuation_type_id,
            @material_code,
            @material_name,
            @uom_id,
            @repeat_poid,
            @repeat_po_line_id,
            @event_item_id,
            @po_condition_id,
            @tax_master_id,
            @saving_at_lpo,
            @saving_first_bid_vs_last_bid,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (nfa_line_id) DO UPDATE SET
            nfa_header_id = EXCLUDED.nfa_header_id,
            event_id = EXCLUDED.event_id,
            erp_pr_lines_id = EXCLUDED.erp_pr_lines_id,
            uom_code = EXCLUDED.uom_code,
            event_qty = EXCLUDED.event_qty,
            unit_price = EXCLUDED.unit_price,
            item_total = EXCLUDED.item_total,
            nfa_qty = EXCLUDED.nfa_qty,
            discount_percentage = EXCLUDED.discount_percentage,
            final_unit_price = EXCLUDED.final_unit_price,
            tax_percentage = EXCLUDED.tax_percentage,
            tax_amount = EXCLUDED.tax_amount,
            tax_code_id = EXCLUDED.tax_code_id,
            item_delivery_date = EXCLUDED.item_delivery_date,
            item_description = EXCLUDED.item_description,
            item_text = EXCLUDED.item_text,
            arc_lines_id = EXCLUDED.arc_lines_id,
            arcpo_line_id = EXCLUDED.arcpo_line_id,
            valuation_type_id = EXCLUDED.valuation_type_id,
            material_code = EXCLUDED.material_code,
            material_name = EXCLUDED.material_name,
            uom_id = EXCLUDED.uom_id,
            repeat_poid = EXCLUDED.repeat_poid,
            repeat_po_line_id = EXCLUDED.repeat_po_line_id,
            event_item_id = EXCLUDED.event_item_id,
            po_condition_id = EXCLUDED.po_condition_id,
            tax_master_id = EXCLUDED.tax_master_id,
            saving_at_lpo = EXCLUDED.saving_at_lpo,
            saving_first_bid_vs_last_bid = EXCLUDED.saving_first_bid_vs_last_bid,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // nfa_line_id
            "Direct",  // nfa_header_id
            "Direct",  // event_id (from nfa_header via AWARDEVENTMAINID)
            "Direct",  // erp_pr_lines_id
            "Direct",  // uom_code
            "Direct",  // event_qty
            "Direct",  // unit_price
            "Direct",  // item_total
            "Direct",  // nfa_qty
            "Direct",  // discount_percentage
            "Direct",  // final_unit_price
            "Direct",  // tax_percentage
            "Direct",  // tax_amount
            "Direct",  // tax_code_id
            "Direct",  // item_delivery_date
            "Direct",  // item_description
            "Direct",  // item_text
            "Direct",  // arc_lines_id
            "Direct",  // arcpo_line_id
            "Direct",  // valuation_type_id
            "Direct",  // material_code
            "Direct",  // material_name
            "Direct",  // uom_id
            "Direct",  // repeat_poid
            "Direct",  // repeat_po_line_id
            "Direct",  // event_item_id
            "Direct",  // po_condition_id
            "Lookup",  // tax_master_id
            "Fixed",   // saving_at_lpo
            "Fixed",   // saving_first_bid_vs_last_bid
            "Direct",  // created_by
            "Direct",  // created_date
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
            new { source = "AWARDEVENTITEM", logic = "AWARDEVENTITEM -> nfa_line_id (Primary key)", target = "nfa_line_id" },
            new { source = "AWARDEVENTMAINID", logic = "AWARDEVENTMAINID -> nfa_header_id (Foreign key to nfa_header)", target = "nfa_header_id" },
            new { source = "EventId", logic = "EventId -> event_id (From TBL_AWARDEVENTMAIN JOIN)", target = "event_id" },
            new { source = "PRTRANSID", logic = "PRTRANSID -> erp_pr_lines_id (Foreign key to erp_pr_lines)", target = "erp_pr_lines_id" },
            new { source = "UOM", logic = "UOM -> uom_code (UomCode)", target = "uom_code" },
            new { source = "EVENTQTY", logic = "EVENTQTY -> event_qty (EventQty)", target = "event_qty" },
            new { source = "EVENTPRICE", logic = "EVENTPRICE -> unit_price (UnitPrice)", target = "unit_price" },
            new { source = "EVENTPRICE, DiscountPer, ASSIGNQTY", logic = "(EVENTPRICE - (EVENTPRICE * DiscountPer / 100)) * ASSIGNQTY -> item_total", target = "item_total" },
            new { source = "ASSIGNQTY", logic = "ASSIGNQTY -> nfa_qty (NFAQty)", target = "nfa_qty" },
            new { source = "DiscountPer", logic = "DiscountPer -> discount_percentage (DiscountPercentage)", target = "discount_percentage" },
            new { source = "EVENTPRICE, DiscountPer", logic = "EVENTPRICE - (EVENTPRICE * DiscountPer / 100) -> final_unit_price", target = "final_unit_price" },
            new { source = "GSTPer", logic = "GSTPer -> tax_percentage (TaxPercentage)", target = "tax_percentage" },
            new { source = "EVENTPRICE, DiscountPer, ASSIGNQTY, GSTPer", logic = "((EVENTPRICE - (EVENTPRICE * DiscountPer / 100)) * ASSIGNQTY) * GSTPer / 100 -> tax_amount", target = "tax_amount" },
            new { source = "TaxCode_Master_Id", logic = "TaxCode_Master_Id -> tax_code_id (Foreign key to tax_master)", target = "tax_code_id" },
            new { source = "DeliveryDate", logic = "DeliveryDate -> item_delivery_date (ItemDeliveryDate)", target = "item_delivery_date" },
            new { source = "ItemDescription", logic = "ItemDescription -> item_description (ItemDescription)", target = "item_description" },
            new { source = "ItemText", logic = "ItemText -> item_text (ItemText)", target = "item_text" },
            new { source = "ARCSubId", logic = "ARCSubId -> arc_lines_id (ARCLineId)", target = "arc_lines_id" },
            new { source = "PRtoARCPOSubId", logic = "PRtoARCPOSubId -> arcpo_line_id (ARCPOLineId)", target = "arcpo_line_id" },
            new { source = "ValuationTypeId", logic = "ValuationTypeId -> valuation_type_id (Foreign key to valuation_type_master)", target = "valuation_type_id" },
            new { source = "ItemCode", logic = "ItemCode -> material_code (MaterialCode)", target = "material_code" },
            new { source = "ItemName", logic = "ItemName -> material_name (MaterialName)", target = "material_name" },
            new { source = "UOM", logic = "UOM -> uom_id (Lookup from uom_master via uom_code)", target = "uom_id" },
            new { source = "POReferanceID", logic = "POReferanceID -> repeat_poid (RepeatPOID)", target = "repeat_poid" },
            new { source = "POSubReferanceID", logic = "POSubReferanceID -> repeat_po_line_id (RepeatPOLineId)", target = "repeat_po_line_id" },
            new { source = "EVENTITEMSID", logic = "EVENTITEMSID (TBL_PB_BUYER.PBID) -> event_item_id (Foreign key to event_items)", target = "event_item_id" },
            new { source = "TBL_AwardEventPoCondition", logic = "PoConditionId array from TBL_AwardEventPoCondition -> po_condition_id (ARRAY)", target = "po_condition_id" },
            new { source = "TaxMasterId", logic = "TaxMasterId (Subquery from TBL_AwardEventTaxCode) -> tax_master_id", target = "tax_master_id" },
            new { source = "-", logic = "saving_at_lpo -> NULL (Fixed Default)", target = "saving_at_lpo" },
            new { source = "-", logic = "saving_first_bid_vs_last_bid -> NULL (Fixed Default)", target = "saving_first_bid_vs_last_bid" },
            new { source = "ENTBY", logic = "ENTBY -> created_by", target = "created_by" },
            new { source = "ENTDATE", logic = "ENTDATE -> created_date", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "nfa_line");
        _migrationLogger.LogInfo("Starting migration");

        _logger.LogInformation("Starting NFA Line migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        var skippedDetails = new List<(string RecordId, string Reason)>();

        try
        {
            // Load PO Condition arrays for each AwardEventItem
            var poConditionArrays = await LoadPoConditionArraysAsync(sqlConn);
            _logger.LogInformation($"Loaded {poConditionArrays.Count} PO condition arrays");

            // Load UOM code to ID mapping from PostgreSQL
            var uomCodeMap = await LoadUomCodeMapAsync(pgConn);
            _logger.LogInformation($"Loaded {uomCodeMap.Count} UOM code mappings");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var awardEventItem = reader["AWARDEVENTITEM"];

                // Skip if AWARDEVENTITEM is NULL
                if (awardEventItem == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - AWARDEVENTITEM is NULL");
                    skippedDetails.Add(("NULL", "AWARDEVENTITEM is NULL"));
                    continue;
                }
                int awardEventItemValue = Convert.ToInt32(awardEventItem);

                // Skip duplicates
                if (processedIds.Contains(awardEventItemValue))
                {
                    skippedRecords++;
                    skippedDetails.Add((awardEventItemValue.ToString(), "Duplicate AWARDEVENTITEM"));
                    continue;
                }

                // Get nfa_header_id (AWARDEVENTMAINID)
                var awardEventMainId = reader["AWARDEVENTMAINID"];
                if (awardEventMainId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventItemValue} - AWARDEVENTMAINID is NULL");
                    skippedDetails.Add((awardEventItemValue.ToString(), "AWARDEVENTMAINID is NULL"));
                    continue;
                }

                // Verify nfa_header_id exists in nfa_header table
                int awardEventMainIdValue = Convert.ToInt32(awardEventMainId);
                bool headerExists = await VerifyNfaHeaderExistsAsync(pgConn, awardEventMainIdValue);
                if (!headerExists)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventItemValue} - nfa_header_id {awardEventMainIdValue} does not exist in nfa_header table");
                    skippedDetails.Add((awardEventItemValue.ToString(), $"nfa_header_id {awardEventMainIdValue} does not exist"));
                    continue;
                }

                // Get event_id from JOIN, default to 0 if NULL
                var eventId = reader["EventId"];
                int eventIdValue = (eventId == DBNull.Value) ? 0 : Convert.ToInt32(eventId);

                // Get values for calculations
                decimal eventPrice = reader["EVENTPRICE"] != DBNull.Value ? Convert.ToDecimal(reader["EVENTPRICE"]) : 0;
                decimal discountPer = reader["DiscountPer"] != DBNull.Value ? Convert.ToDecimal(reader["DiscountPer"]) : 0;
                decimal assignQty = reader["ASSIGNQTY"] != DBNull.Value ? Convert.ToDecimal(reader["ASSIGNQTY"]) : 0;
                decimal gstPer = reader["GSTPer"] != DBNull.Value ? Convert.ToDecimal(reader["GSTPer"]) : 0;

                // Calculate values
                // final_unit_price = EVENTPRICE - (EVENTPRICE * DiscountPer / 100)
                decimal finalUnitPrice = eventPrice - (eventPrice * discountPer / 100);
                
                // item_total = (EVENTPRICE - (EVENTPRICE * DiscountPer / 100)) * AssignQty
                decimal itemTotal = finalUnitPrice * assignQty;
                
                // tax_amount = ((EVENTPRICE - (EVENTPRICE * DiscountPer / 100)) * AssignQty) * GSTPer / 100
                decimal taxAmount = itemTotal * gstPer / 100;

                // First try to lookup uom_id from uom_master using Uomid from source
                var sourceUomId = reader["Uomid"];
                int? uomId = null;
                string? uomCodeValue = null;
                
                if (sourceUomId != DBNull.Value && Convert.ToInt32(sourceUomId) > 0)
                {
                    int sourceUomIdValue = Convert.ToInt32(sourceUomId);
                    // Check if this Uomid exists in uom_master
                    var checkQuery = "SELECT uom_code FROM uom_master WHERE uom_id = @uomId";
                    using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
                    checkCmd.Parameters.AddWithValue("@uomId", sourceUomIdValue);
                    var result = await checkCmd.ExecuteScalarAsync();
                    if (result != null)
                    {
                        uomId = sourceUomIdValue;
                        uomCodeValue = result.ToString();
                    }
                }
                
                // If Uomid lookup failed, try UOM code lookup
                if (!uomId.HasValue)
                {
                    var uomCode = reader["UOM"];
                    if (uomCode != DBNull.Value && !string.IsNullOrEmpty(uomCode.ToString()))
                    {
                        string uomCodeStr = uomCode.ToString()!.Trim();
                        if (uomCodeMap.ContainsKey(uomCodeStr))
                        {
                            uomId = uomCodeMap[uomCodeStr];
                            uomCodeValue = uomCodeStr;
                        }
                    }
                }

                // Skip record if uom_id is still not found
                if (!uomId.HasValue)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventItemValue} - Could not find uom_id for Uomid: {sourceUomId} or UOM code: {reader["UOM"]}");
                    skippedDetails.Add((awardEventItemValue.ToString(), $"Could not find uom_id for Uomid: {sourceUomId} or UOM code: {reader["UOM"]}"));
                    continue;
                }

                var record = new Dictionary<string, object>
                {
                    ["nfa_line_id"] = awardEventItemValue,
                    ["nfa_header_id"] = awardEventMainId,
                    ["event_id"] = eventIdValue,
                    ["erp_pr_lines_id"] = reader["PRTRANSID"] ?? DBNull.Value,
                    ["uom_code"] = uomCodeValue ?? (reader["UOM"] != DBNull.Value ? reader["UOM"] : DBNull.Value),
                    ["event_qty"] = reader["EVENTQTY"] ?? DBNull.Value,
                    ["unit_price"] = eventPrice,
                    ["item_total"] = itemTotal,
                    ["nfa_qty"] = assignQty,
                    ["discount_percentage"] = discountPer,
                    ["final_unit_price"] = finalUnitPrice,
                    ["tax_percentage"] = gstPer,
                    ["tax_amount"] = taxAmount,
                    ["tax_code_id"] = reader["TaxCode_Master_Id"] ?? DBNull.Value,
                    ["item_delivery_date"] = reader["DeliveryDate"] ?? DBNull.Value,
                    ["item_description"] = reader["ItemDescription"] ?? DBNull.Value,
                    ["item_text"] = reader["ItemText"] ?? DBNull.Value,
                    ["arc_lines_id"] = reader["ARCSubId"] ?? DBNull.Value,
                    ["arcpo_line_id"] = reader["PRtoARCPOSubId"] ?? DBNull.Value,
                    ["valuation_type_id"] = reader["ValuationTypeId"] ?? DBNull.Value,
                    ["material_code"] = reader["ItemCode"] ?? DBNull.Value,
                    ["material_name"] = reader["ItemName"] ?? DBNull.Value,
                    ["uom_id"] = uomId.Value,
                    ["repeat_poid"] = reader["POReferanceID"] ?? DBNull.Value,
                    ["repeat_po_line_id"] = reader["POSubReferanceID"] ?? DBNull.Value,
                    ["event_item_id"] = reader["EVENTITEMSID"] ?? DBNull.Value,
                    ["po_condition_id"] = poConditionArrays.ContainsKey(awardEventItemValue) ? poConditionArrays[awardEventItemValue] : (object)DBNull.Value,
                    ["tax_master_id"] = reader["TaxMasterId"] ?? DBNull.Value,
                    ["saving_at_lpo"] = DBNull.Value,
                    ["saving_first_bid_vs_last_bid"] = DBNull.Value,
                    ["created_by"] = reader["ENTBY"] ?? DBNull.Value,
                    ["created_date"] = reader["ENTDATE"] ?? DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(awardEventItemValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn, transaction);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn, transaction);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"NFA Line migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            // Export migration stats to Excel
            DataMigration.Services.MigrationStatsExporter.ExportToExcel(
                "NfaLineMigrationStats.xlsx",
                totalRecords,
                migratedRecords,
                skippedRecords,
                _logger,
                skippedDetails
            );

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during NFA Line migration");
            throw;
        }
    }

    private async Task<bool> VerifyNfaHeaderExistsAsync(NpgsqlConnection pgConn, int nfaHeaderId)
    {
        try
        {
            var query = "SELECT EXISTS(SELECT 1 FROM nfa_header WHERE nfa_header_id = @nfaHeaderId)";
            using var command = new NpgsqlCommand(query, pgConn);
            command.Parameters.AddWithValue("@nfaHeaderId", nfaHeaderId);
            var result = await command.ExecuteScalarAsync();
            return result != null && (bool)result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error verifying nfa_header_id {nfaHeaderId} exists");
            return false;
        }
    }

    private async Task<Dictionary<string, int>> LoadUomCodeMapAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var query = "SELECT uom_id, uom_code FROM uom_master WHERE uom_id IS NOT NULL AND uom_code IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                int uomId = reader.GetInt32(0);
                string uomCode = reader.GetString(1).Trim();
                if (!map.ContainsKey(uomCode))
                {
                    map[uomCode] = uomId;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading UOM code mapping");
        }
        return map;
    }

    private async Task<Dictionary<int, int[]>> LoadPoConditionArraysAsync(SqlConnection sqlConn)
    {
        var map = new Dictionary<int, List<int>>();
        try
        {
            var query = "SELECT AwardEventItemId, PoConditionId FROM TBL_AwardEventPoCondition WHERE AwardEventItemId IS NOT NULL AND PoConditionId IS NOT NULL ORDER BY AwardEventItemId";
            using var command = new SqlCommand(query, sqlConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                int awardEventItemId = reader.GetInt32(0);
                int poConditionId = reader.GetInt32(1);
                
                if (!map.ContainsKey(awardEventItemId))
                {
                    map[awardEventItemId] = new List<int>();
                }
                map[awardEventItemId].Add(poConditionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading PO condition arrays");
        }
        
        // Convert Lists to arrays
        return map.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray());
    }

    private async Task<int> InsertBatchWithTransactionAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn, NpgsqlTransaction? parentTransaction = null)
    {
        int insertedCount = 0;
        NpgsqlTransaction? transaction = parentTransaction;
        bool ownTransaction = false;
        if (transaction == null)
        {
            transaction = await pgConn.BeginTransactionAsync();
            ownTransaction = true;
        }
        try
        {
            foreach (var record in batch)
            {
                using var cmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);
                cmd.CommandTimeout = 300; // Set 5 minute timeout for each insert
                foreach (var kvp in record)
                {
                    cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value);
                }
                await cmd.ExecuteNonQueryAsync();
                insertedCount++;
            }
            if (ownTransaction)
            {
                await transaction.CommitAsync();
            }
            return insertedCount;
        }
        catch (Exception ex)
        {
            if (ownTransaction && transaction != null)
            {
                await transaction.RollbackAsync();
            }
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records");
            throw;
        }
        finally
        {
            if (ownTransaction && transaction != null)
            {
                await transaction.DisposeAsync();
            }
        }
    }
}
