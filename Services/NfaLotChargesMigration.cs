using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class NfaLotChargesMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<NfaLotChargesMigration> _logger;

    public NfaLotChargesMigration(IConfiguration configuration, ILogger<NfaLotChargesMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override string SelectQuery => @"
        SELECT
            TBL_AWARDEVENTLOTCHARGES.AwardEventLotChargesId,
            TBL_AWARDEVENTLOTCHARGES.AWARDEVENTMAINID,
            TBL_AWARDEVENTLOTCHARGES.PB_BuyerChargesId,
            TBL_AWARDEVENTLOTCHARGES.Percentage,
            TBL_AWARDEVENTLOTCHARGES.Amount,
            TBL_AWARDEVENTLOTCHARGES.ChargesName,
            TBL_AWARDEVENTLOTCHARGES.GSTPer,
            TBL_AWARDEVENTLOTCHARGES.GSTAmount
        FROM TBL_AWARDEVENTLOTCHARGES
        ORDER BY TBL_AWARDEVENTLOTCHARGES.AwardEventLotChargesId";

    protected override string InsertQuery => @"
        INSERT INTO nfa_lot_charges (
            nfa_lot_charges_id,
            nfa_header_id,
            user_price_bid_lot_charges_id,
            price_bid_charges_id,
            percentage,
            basic_lot_charges_amount,
            charges_name,
            tax_percentage,
            tax_amount,
            tax_master_id,
            total_lot_charges_amount,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @nfa_lot_charges_id,
            @nfa_header_id,
            @user_price_bid_lot_charges_id,
            @price_bid_charges_id,
            @percentage,
            @basic_lot_charges_amount,
            @charges_name,
            @tax_percentage,
            @tax_amount,
            @tax_master_id,
            @total_lot_charges_amount,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (nfa_lot_charges_id) DO UPDATE SET
            nfa_header_id = EXCLUDED.nfa_header_id,
            user_price_bid_lot_charges_id = EXCLUDED.user_price_bid_lot_charges_id,
            price_bid_charges_id = EXCLUDED.price_bid_charges_id,
            percentage = EXCLUDED.percentage,
            basic_lot_charges_amount = EXCLUDED.basic_lot_charges_amount,
            charges_name = EXCLUDED.charges_name,
            tax_percentage = EXCLUDED.tax_percentage,
            tax_amount = EXCLUDED.tax_amount,
            tax_master_id = EXCLUDED.tax_master_id,
            total_lot_charges_amount = EXCLUDED.total_lot_charges_amount,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // nfa_lot_charges_id
            "Direct",  // nfa_header_id
            "Direct",  // user_price_bid_lot_charges_id
            "Lookup",  // price_bid_charges_id
            "Direct",  // percentage
            "Direct",  // basic_lot_charges_amount
            "Direct",  // charges_name
            "Direct",  // tax_percentage
            "Direct",  // tax_amount
            "Lookup",  // tax_master_id
            "Calculated", // total_lot_charges_amount
            "Fixed",   // created_by
            "Fixed",   // created_date
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
            new { source = "AwardEventLotChargesId", logic = "AwardEventLotChargesId -> nfa_lot_charges_id (Primary key autoincrement)", target = "nfa_lot_charges_id" },
            new { source = "AWARDEVENTMAINID", logic = "AWARDEVENTMAINID -> nfa_header_id (Foreign key to nfa_header - NFAHeaderId)", target = "nfa_header_id" },
            new { source = "PB_BuyerChargesId", logic = "PB_BuyerChargesId -> user_price_bid_lot_charges_id (Foreign key to user_price_bid_lot_charges - UserPriceBidLotChargesID)", target = "user_price_bid_lot_charges_id" },
            new { source = "PB_BuyerChargesId", logic = "PB_BuyerChargesId -> price_bid_charges_id (Lookup from price_bid_charges_master - PriceBidChargesId)", target = "price_bid_charges_id" },
            new { source = "Percentage", logic = "Percentage -> percentage (Percentage)", target = "percentage" },
            new { source = "Amount", logic = "Amount -> basic_lot_charges_amount (BasicLotChargesAmount)", target = "basic_lot_charges_amount" },
            new { source = "ChargesName", logic = "ChargesName -> charges_name (ChargesName)", target = "charges_name" },
            new { source = "GSTPer", logic = "GSTPer -> tax_percentage (TaxPercentage)", target = "tax_percentage" },
            new { source = "GSTAmount", logic = "GSTAmount -> tax_amount (TaxAmount)", target = "tax_amount" },
            new { source = "GSTPer", logic = "GSTPer -> tax_master_id (Lookup from tax_master via GSTPer - TaxMasterId)", target = "tax_master_id" },
            new { source = "Amount, GSTAmount", logic = "(Amount + GSTAmount) -> total_lot_charges_amount (TotalLotChargesAmount)", target = "total_lot_charges_amount" },
            new { source = "-", logic = "created_by -> NULL (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _logger.LogInformation("Starting NFA Lot Charges migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load lookup data
            var priceBidChargesMap = await LoadPriceBidChargesAsync(pgConn);
            var taxMasterMap = await LoadTaxMasterAsync(pgConn);
            
            _logger.LogInformation($"Loaded lookup data: PriceBidCharges={priceBidChargesMap.Count}, TaxMaster={taxMasterMap.Count}");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var awardEventLotChargesId = reader["AwardEventLotChargesId"];

                // Skip if AwardEventLotChargesId is NULL
                if (awardEventLotChargesId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - AwardEventLotChargesId is NULL");
                    continue;
                }

                int awardEventLotChargesIdValue = Convert.ToInt32(awardEventLotChargesId);

                // Skip duplicates
                if (processedIds.Contains(awardEventLotChargesIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Get nfa_header_id (AWARDEVENTMAINID)
                var awardEventMainId = reader["AWARDEVENTMAINID"];
                if (awardEventMainId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventLotChargesIdValue} - AWARDEVENTMAINID is NULL");
                    continue;
                }

                // Lookup price_bid_charges_id from user_price_bid_lot_charges
                var pbBuyerChargesId = reader["PB_BuyerChargesId"];
                int? priceBidChargesId = null;
                if (pbBuyerChargesId != DBNull.Value && priceBidChargesMap.ContainsKey(Convert.ToInt32(pbBuyerChargesId)))
                {
                    priceBidChargesId = priceBidChargesMap[Convert.ToInt32(pbBuyerChargesId)];
                }

                // Lookup tax_master_id from tax_master via GSTPer
                var gstPer = reader["GSTPer"];
                int? taxMasterId = null;
                if (gstPer != DBNull.Value)
                {
                    decimal gstPerValue = Convert.ToDecimal(gstPer);
                    if (taxMasterMap.ContainsKey(gstPerValue))
                    {
                        taxMasterId = taxMasterMap[gstPerValue];
                    }
                }

                // Calculate total_lot_charges_amount
                decimal amount = reader["Amount"] != DBNull.Value ? Convert.ToDecimal(reader["Amount"]) : 0;
                decimal gstAmount = reader["GSTAmount"] != DBNull.Value ? Convert.ToDecimal(reader["GSTAmount"]) : 0;
                decimal totalLotChargesAmount = amount + gstAmount;

                var record = new Dictionary<string, object>
                {
                    ["nfa_lot_charges_id"] = awardEventLotChargesIdValue,
                    ["nfa_header_id"] = awardEventMainId,
                    ["user_price_bid_lot_charges_id"] = pbBuyerChargesId ?? DBNull.Value,
                    ["price_bid_charges_id"] = priceBidChargesId.HasValue ? (object)priceBidChargesId.Value : DBNull.Value,
                    ["percentage"] = reader["Percentage"] ?? DBNull.Value,
                    ["basic_lot_charges_amount"] = reader["Amount"] ?? DBNull.Value,
                    ["charges_name"] = reader["ChargesName"] ?? DBNull.Value,
                    ["tax_percentage"] = reader["GSTPer"] ?? DBNull.Value,
                    ["tax_amount"] = reader["GSTAmount"] ?? DBNull.Value,
                    ["tax_master_id"] = taxMasterId.HasValue ? (object)taxMasterId.Value : DBNull.Value,
                    ["total_lot_charges_amount"] = totalLotChargesAmount,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(awardEventLotChargesIdValue);

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

            _logger.LogInformation($"NFA Lot Charges migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during NFA Lot Charges migration");
            throw;
        }
    }

    private async Task<Dictionary<int, int>> LoadPriceBidChargesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, int>();
        try
        {
            var query = "SELECT user_price_bid_lot_charges_id, price_bid_charges_id FROM user_price_bid_lot_charges WHERE user_price_bid_lot_charges_id IS NOT NULL AND price_bid_charges_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.GetInt32(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading price bid charges mapping");
        }
        return map;
    }

    private async Task<Dictionary<decimal, int>> LoadTaxMasterAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<decimal, int>();
        try
        {
            var query = "SELECT tax_master_id, tax_percentage FROM tax_master WHERE tax_master_id IS NOT NULL AND tax_percentage IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                decimal taxPercentage = reader.GetDecimal(1);
                int taxMasterId = reader.GetInt32(0);
                if (!map.ContainsKey(taxPercentage))
                {
                    map[taxPercentage] = taxMasterId;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading tax master mapping");
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
