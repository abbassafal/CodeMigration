using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

namespace DataMigration.Services;

public class EventSettingMigrationService
{
    private readonly ILogger<EventSettingMigrationService> _logger;
    private readonly IConfiguration _configuration;
    private MigrationLogger? _migrationLogger;

    public EventSettingMigrationService(IConfiguration configuration, ILogger<EventSettingMigrationService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "EVENTID", target = "event_id", type = "int -> integer" },
            new { source = "EventMode", target = "event_mode", type = "int -> text" },
            new { source = "TiePreventLot", target = "tie_prevent_lot", type = "int -> boolean" },
            new { source = "TiePreventItem", target = "tie_prevent_item", type = "int -> boolean" },
            new { source = "IsTargetPriceApplicable", target = "target_price_applicable", type = "int -> boolean" },
            new { source = "IsAutoExtendedEnable", target = "auto_extended_enable", type = "int -> boolean" },
            new { source = "NoofTimesAutoExtended", target = "no_of_times_auto_extended", type = "int -> integer" },
            new { source = "AutoExtendedMinutes", target = "auto_extended_minutes", type = "int -> integer" },
            new { source = "ApplyExtendedTimes", target = "apply_extended_times", type = "int -> integer" },
            new { source = "GREENPERCENTAGE", target = "green_percentage", type = "decimal -> numeric" },
            new { source = "YELLOWPERCENTAGE", target = "yellow_percentage", type = "decimal -> numeric" },
            new { source = "IsItemLevelRankShow", target = "show_item_level_rank", type = "int -> boolean" },
            new { source = "IsLotLevelRankShow", target = "show_lot_level_rank", type = "int -> boolean" },
            new { source = "IsBasicPriceApplicable/IsLotLevelAuction", target = "basic_price_applicable", type = "conditional logic -> boolean" },
            new { source = "IsBasicPriceValidationReq", target = "basic_price_validation_mandatory", type = "int -> boolean" },
            new { source = "IsMinMaxBidApplicable", target = "min_max_bid_applicable", type = "int -> boolean" },
            new { source = "IsLowestBidShow", target = "show_lower_bid", type = "int -> boolean" },
            new { source = "BesideAuctionFirstBid", target = "apply_all_settings_in_price_bid", type = "int -> boolean" },
            new { source = "MinBid", target = "min_lot_auction_bid_value", type = "int -> numeric" },
            new { source = "MaxBid", target = "max_lot_auction_bid_value", type = "int -> numeric" },
            new { source = "IsLotLevelAuction", target = "configure_lot_level_auction", type = "int -> boolean" },
            new { source = "LotLevelBasicPrice", target = "lot_level_basic_price", type = "decimal -> numeric" },
            new { source = "IsPriceBidAttachmentcompulsory", target = "price_bid_attachment_mandatory", type = "bit -> boolean" },
            new { source = "IsDiscountApplicable", target = "discount_applicable", type = "int -> boolean" },
            new { source = "IsGSTCompulsory", target = "gst_mandatory", type = "int -> boolean" },
            new { source = "IsTechnicalAttachmentcompulsory", target = "technical_attachment_mandatory", type = "bit -> boolean" },
            new { source = "IsProposedQty", target = "proposed_qty", type = "int -> boolean" },
            new { source = "IsRedyStockmandatory", target = "ready_stock_mandatory", type = "int -> boolean" },
            new { source = "default: 0", target = "created_by", type = "integer" },
            new { source = "default: UTC now", target = "created_date", type = "timestamp with time zone" },
            new { source = "default: 0", target = "lot_level_target_price", type = "numeric" },
            new { source = "MinBidMode", target = "max_lot_bid_type", type = "int -> text" },
            new { source = "MaxBidMode", target = "min_lot_bid_type", type = "int -> text" },
            new { source = "default: false", target = "allow_currency_selection", type = "boolean" }
        };
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    public async Task<int> MigrateAsync()
    {
        int migratedCount = 0;
        int skippedCount = 0;
        var skippedDetails = new List<(string RecordId, string Reason)>();
        _migrationLogger = new MigrationLogger(_logger, "event_setting");
        _migrationLogger.LogInfo("Starting migration");
        using var sqlConnection = new SqlConnection(_configuration.GetConnectionString("SqlServer"));
        using var pgConnection = new NpgsqlConnection(_configuration.GetConnectionString("PostgreSql"));
        await sqlConnection.OpenAsync();
        await pgConnection.OpenAsync();
        var selectQuery = @"SELECT * FROM TBL_EVENTMASTER ORDER BY EVENTID";
        using var sqlCmd = new SqlCommand(selectQuery, sqlConnection);
        using var reader = await sqlCmd.ExecuteReaderAsync();
        var copyCommand = @"COPY event_setting (
            event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
            auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
            apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
            show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
            min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
            min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
            lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
            gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
            created_by, created_date, modified_by, modified_date, deleted_by, deleted_date,
            is_deleted, lot_level_target_price, max_lot_bid_type, min_lot_bid_type, 
            allow_currency_selection
        ) FROM STDIN (FORMAT TEXT, DELIMITER '|')";
        using var writer = await pgConnection.BeginTextImportAsync(copyCommand);
        var now = DateTime.UtcNow;
        while (await reader.ReadAsync())
        {
            var eventId = reader["EVENTID"];
            if (eventId == null || eventId == DBNull.Value)
            {
                _migrationLogger.LogSkipped("EVENTID is NULL", null, new Dictionary<string, object> { { "EVENTID", eventId } });
                skippedCount++;
                skippedDetails.Add((reader["EVENTSCHEDULARID"]?.ToString() ?? "NULL", "EVENTID is NULL"));
                continue;
            }
            var fields = new string[]
            {
                FormatInteger(reader["EVENTID"]),
                FormatValue(reader["EventMode"]),
                Bool(reader["TiePreventLot"]),
                Bool(reader["TiePreventItem"]),
                Bool(reader["IsTargetPriceApplicable"]),
                Bool(reader["IsAutoExtendedEnable"]),
                FormatInteger(reader["NoofTimesAutoExtended"]),
                FormatInteger(reader["AutoExtendedMinutes"]),
                FormatInteger(reader["ApplyExtendedTimes"]),
                FormatNumeric(reader["GREENPERCENTAGE"]),
                FormatNumeric(reader["YELLOWPERCENTAGE"]),
                Bool(reader["IsItemLevelRankShow"]),
                Bool(reader["IsLotLevelRankShow"]),
                ConditionalBool(reader["IsLotLevelAuction"], reader["IsBasicPriceApplicable"]),
                Bool(reader["IsBasicPriceValidationReq"]),
                Bool(reader["IsMinMaxBidApplicable"]),
                Bool(reader["IsLowestBidShow"]),
                Bool(reader["BesideAuctionFirstBid"]),
                FormatNumeric(reader["MinBid"]),
                FormatNumeric(reader["MaxBid"]),
                Bool(reader["IsLotLevelAuction"]),
                FormatNumeric(reader["LotLevelBasicPrice"]),
                Bool(reader["IsPriceBidAttachmentcompulsory"]),
                Bool(reader["IsDiscountApplicable"]),
                Bool(reader["IsGSTCompulsory"]),
                Bool(reader["IsTechnicalAttachmentcompulsory"]),
                Bool(reader["IsProposedQty"]),
                Bool(reader["IsRedyStockmandatory"]),
                "0",
                now.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00"),
                "\\N",
                "\\N",
                "\\N",
                "\\N",
                "f",
                "0",
                FormatValue(reader["MinBidMode"]),
                FormatValue(reader["MaxBidMode"]),
                "f"
            };
            var row = string.Join("|", fields);
            await writer.WriteLineAsync(row);
            migratedCount++;
            _migrationLogger.LogInserted($"EVENTID={eventId}");
        }
        writer.Close();
        _migrationLogger.LogInfo($"Migrated {migratedCount} event_setting records. Skipped {skippedCount} records.");
        // Export migration stats and skipped records to Excel
        MigrationStatsExporter.ExportToExcel(
            "EventSettingMigrationStats.xlsx",
            migratedCount + skippedCount,
            migratedCount,
            skippedCount,
            _logger,
            skippedDetails
        );
        return migratedCount;
    }

    // Helper method to format general values (handles NULL)
    private static string FormatValue(object? value)
    {
        if (value == null || value == DBNull.Value)
            return @"\N";
        
        var str = value.ToString();
        if (string.IsNullOrEmpty(str))
            return @"\N";
        
        // Escape special characters for COPY TEXT format with pipe delimiter
        return str
            .Replace("\\", "\\\\")   // Backslash must be first
            .Replace("|", "\\|")     // Pipe (our delimiter)
            .Replace("\t", "\\t")    // Tab
            .Replace("\n", "\\n")    // Newline
            .Replace("\r", "\\r");   // Carriage return
    }

    // Helper method specifically for integer fields (NOT NULL columns)
    private static string FormatInteger(object? value)
    {
        if (value == null || value == DBNull.Value)
            return "0";  // Return 0 instead of \N for NOT NULL columns
        
        var str = value.ToString();
        if (string.IsNullOrWhiteSpace(str))
            return "0";
        
        return str;
    }

    // Helper method specifically for numeric fields (NOT NULL columns)
    private static string FormatNumeric(object? value)
    {
        if (value == null || value == DBNull.Value)
            return "0";  // Return 0 instead of \N for NOT NULL columns
        
        var str = value.ToString();
        if (string.IsNullOrWhiteSpace(str))
            return "0";
        
        return str;
    }

    // Helper method for boolean conversion
    private static string Bool(object? value)
    {
        if (value == null || value == DBNull.Value) return "f";
        var v = value.ToString();
        return (v == "1" || v == "True" || v == "true") ? "t" : "f";
    }

    // Helper method for conditional boolean logic
    private static string ConditionalBool(object? lotLevelAuction, object? basicPriceApplicable)
    {
        // If IsLotLevelAuction == 1, return true, else use IsBasicPriceApplicable
        var lotLevel = lotLevelAuction?.ToString();
        if (lotLevel == "1" || lotLevel == "True" || lotLevel == "true")
            return "t";
        
        return Bool(basicPriceApplicable);
    }
}
