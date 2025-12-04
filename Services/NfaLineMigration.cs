using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class NfaLineMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<NfaLineMigration> _logger;

    public NfaLineMigration(IConfiguration configuration, ILogger<NfaLineMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

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
            TBL_AWARDEVENTITEM.POReferenceID,
            TBL_AWARDEVENTITEM.POSubReferenceID,
            TBL_AWARDEVENTITEM.isPRMode,
            TBL_AWARDEVENTITEM.isItemMode
        FROM TBL_AWARDEVENTITEM
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
            new { source = "-", logic = "event_id -> Lookup from nfa_header via AWARDEVENTMAINID", target = "event_id" },
            new { source = "PRTRANSID", logic = "PRTRANSID -> erp_pr_lines_id (Foreign key to erp_pr_lines)", target = "erp_pr_lines_id" },
            new { source = "UOM", logic = "UOM -> uom_code (UomCode)", target = "uom_code" },
            new { source = "EVENTQTY", logic = "EVENTQTY -> event_qty (EventQty)", target = "event_qty" },
            new { source = "EVENTPRICE", logic = "EVENTPRICE -> unit_price (UnitPrice)", target = "unit_price" },
            new { source = "TOTALAMOUNT", logic = "TOTALAMOUNT -> item_total (ItemTotal)", target = "item_total" },
            new { source = "ASSIGNQTY", logic = "ASSIGNQTY -> nfa_qty (NFAQty)", target = "nfa_qty" },
            new { source = "DiscountPer", logic = "DiscountPer -> discount_percentage (DiscountPercentage)", target = "discount_percentage" },
            new { source = "-", logic = "final_unit_price -> EVENTPRICE (same as unit_price)", target = "final_unit_price" },
            new { source = "GSTPer", logic = "GSTPer -> tax_percentage (TaxPercentage)", target = "tax_percentage" },
            new { source = "-", logic = "tax_amount -> Calculated from TOTALAMOUNT * GSTPer / 100", target = "tax_amount" },
            new { source = "TaxCode_Master_Id", logic = "TaxCode_Master_Id -> tax_code_id (Foreign key to tax_master)", target = "tax_code_id" },
            new { source = "DeliveryDate", logic = "DeliveryDate -> item_delivery_date (ItemDeliveryDate)", target = "item_delivery_date" },
            new { source = "ItemDescription", logic = "ItemDescription -> item_description (ItemDescription)", target = "item_description" },
            new { source = "ItemText", logic = "ItemText -> item_text (ItemText)", target = "item_text" },
            new { source = "ARCSubId", logic = "ARCSubId -> arc_lines_id (ARCLineId)", target = "arc_lines_id" },
            new { source = "PRtoARCPOSubId", logic = "PRtoARCPOSubId -> arcpo_line_id (ARCPOLineId)", target = "arcpo_line_id" },
            new { source = "ValuationTypeId", logic = "ValuationTypeId -> valuation_type_id (Foreign key to valuation_type_master)", target = "valuation_type_id" },
            new { source = "ItemCode", logic = "ItemCode -> material_code (MaterialCode)", target = "material_code" },
            new { source = "ItemName", logic = "ItemName -> material_name (MaterialName)", target = "material_name" },
            new { source = "Uomid", logic = "Uomid -> uom_id (Foreign key to uom_master)", target = "uom_id" },
            new { source = "POReferenceID", logic = "POReferenceID -> repeat_poid (RepeatPOID)", target = "repeat_poid" },
            new { source = "POSubReferenceID", logic = "POSubReferenceID -> repeat_po_line_id (RepeatPOLineId)", target = "repeat_po_line_id" },
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
        _logger.LogInformation("Starting NFA Line migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load event_id mapping from nfa_header
            var nfaHeaderEventMap = await LoadNfaHeaderEventMappingAsync(pgConn);
            _logger.LogInformation($"Loaded {nfaHeaderEventMap.Count} nfa_header to event_id mappings");

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
                    continue;
                }

                int awardEventItemValue = Convert.ToInt32(awardEventItem);

                // Skip duplicates
                if (processedIds.Contains(awardEventItemValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Get nfa_header_id (AWARDEVENTMAINID)
                var awardEventMainId = reader["AWARDEVENTMAINID"];
                if (awardEventMainId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventItemValue} - AWARDEVENTMAINID is NULL");
                    continue;
                }

                int nfaHeaderId = Convert.ToInt32(awardEventMainId);

                // Get event_id from nfa_header mapping
                int? eventId = null;
                if (nfaHeaderEventMap.ContainsKey(nfaHeaderId))
                {
                    eventId = nfaHeaderEventMap[nfaHeaderId];
                }

                if (!eventId.HasValue)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventItemValue} - Could not find event_id for nfa_header_id {nfaHeaderId}");
                    continue;
                }

                // Calculate tax_amount
                decimal totalAmount = reader["TOTALAMOUNT"] != DBNull.Value ? Convert.ToDecimal(reader["TOTALAMOUNT"]) : 0;
                decimal gstPer = reader["GSTPer"] != DBNull.Value ? Convert.ToDecimal(reader["GSTPer"]) : 0;
                decimal taxAmount = totalAmount * gstPer / 100;

                var record = new Dictionary<string, object>
                {
                    ["nfa_line_id"] = awardEventItemValue,
                    ["nfa_header_id"] = nfaHeaderId,
                    ["event_id"] = eventId.Value,
                    ["erp_pr_lines_id"] = reader["PRTRANSID"] ?? DBNull.Value,
                    ["uom_code"] = reader["UOM"] ?? DBNull.Value,
                    ["event_qty"] = reader["EVENTQTY"] ?? DBNull.Value,
                    ["unit_price"] = reader["EVENTPRICE"] ?? DBNull.Value,
                    ["item_total"] = reader["TOTALAMOUNT"] ?? DBNull.Value,
                    ["nfa_qty"] = reader["ASSIGNQTY"] ?? DBNull.Value,
                    ["discount_percentage"] = reader["DiscountPer"] ?? DBNull.Value,
                    ["final_unit_price"] = reader["EVENTPRICE"] ?? DBNull.Value,
                    ["tax_percentage"] = reader["GSTPer"] ?? DBNull.Value,
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
                    ["uom_id"] = reader["Uomid"] ?? DBNull.Value,
                    ["repeat_poid"] = reader["POReferenceID"] ?? DBNull.Value,
                    ["repeat_po_line_id"] = reader["POSubReferenceID"] ?? DBNull.Value,
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
                    int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"NFA Line migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during NFA Line migration");
            throw;
        }
    }

    private async Task<Dictionary<int, int>> LoadNfaHeaderEventMappingAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, int>();
        try
        {
            var query = "SELECT nfa_header_id, event_id FROM nfa_header WHERE nfa_header_id IS NOT NULL AND event_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.GetInt32(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading nfa_header to event_id mapping");
        }
        return map;
    }

    private async Task<int> InsertBatchWithTransactionAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn)
    {
        NpgsqlTransaction? batchTransaction = null;
        try
        {
            batchTransaction = await pgConn.BeginTransactionAsync();

            foreach (var record in batch)
            {
                using var cmd = new NpgsqlCommand(InsertQuery, pgConn, batchTransaction);

                foreach (var kvp in record)
                {
                    cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value);
                }

                await cmd.ExecuteNonQueryAsync();
            }

            await batchTransaction.CommitAsync();
            return batch.Count;
        }
        catch (Exception ex)
        {
            if (batchTransaction != null)
            {
                await batchTransaction.RollbackAsync();
            }
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records");
            throw;
        }
    }
}
