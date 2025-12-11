using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using System.Collections.Concurrent;
using System.Diagnostics;
using NpgsqlTypes;
using DataMigration.Services;

public class EventMasterMigration : MigrationService
{
    private readonly ILogger<EventMasterMigration> _logger;
    private MigrationLogger? _migrationLogger;
    private readonly int _batchSize;
    private readonly int _transformWorkerCount;
    
    // Cache for valid foreign key IDs
    private HashSet<int> _validCurrencyIds = new HashSet<int>();
    private HashSet<int> _validCompanyIds = new HashSet<int>();
    private int _defaultCurrencyId = 1;
    private int _defaultCompanyId = 1;
    private int _skippedInvalidCurrency = 0;
    private int _skippedInvalidCompany = 0;

    public EventMasterMigration(IConfiguration configuration, ILogger<EventMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
        _batchSize = configuration.GetValue<int?>("Migration:EventMaster:BatchSize") ?? 1000;
        _transformWorkerCount = configuration.GetValue<int?>("Migration:EventMaster:TransformWorkerCount") ?? Math.Max(1, Environment.ProcessorCount - 1);
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    // Helper method to sanitize strings - removes null bytes and invalid UTF8 characters
    private static string SanitizeString(string? input)
    {
        if (string.IsNullOrEmpty(input))
            return string.Empty;
        
        // Most aggressive approach: rebuild string character-by-character
        // Skip null bytes and invalid control characters
        var chars = new List<char>(input.Length);
        foreach (var c in input)
        {
            // Keep only valid characters:
            // - Not null byte (\0)
            // - Either printable (>= 32) OR whitespace (tab, newline, carriage return)
            if (c != '\0' && c != '\u0000' && (c >= 32 || c == '\t' || c == '\n' || c == '\r'))
            {
                chars.Add(c);
            }
        }
        
        return new string(chars.ToArray());
    }

    // Helper method to escape text for PostgreSQL COPY TEXT format with pipe delimiter
    private static string EscapeTextCopy(string? input)
    {
        if (string.IsNullOrEmpty(input))
            return @"\N";  // NULL in COPY TEXT format
        
        // First sanitize to remove null bytes
        var sanitized = SanitizeString(input);
        
        // Escape special characters for COPY TEXT format
        // Replace: backslash, pipe (our delimiter), tab, newline, carriage return
        return sanitized
            .Replace("\\", "\\\\")   // Backslash must be first
            .Replace("|", "\\|")     // Pipe (our delimiter)
            .Replace("\t", "\\t")    // Tab
            .Replace("\n", "\\n")    // Newline
            .Replace("\r", "\\r");   // Carriage return
    }

    // Helper to format value for COPY TEXT
    private static string FormatCopyValue(object? value)
    {
        if (value == null)
            return "\\N";
        
        if (value is string str)
            return EscapeTextCopy(str);
        
        if (value is bool b)
            return b ? "t" : "f";
        
        if (value is DateTime dt)
        {
            var utcDt = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return utcDt.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz");
        }
        
        return value.ToString() ?? "\\N";
    }

    // Helper to format nullable DateTime for COPY
    private static string FormatDateTime(DateTime? dt)
    {
        if (dt == null)
            return @"\N";
        
        var utcDt = dt.Value.Kind == DateTimeKind.Utc ? dt.Value : DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc);
        return utcDt.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00");
    }

    // Record classes for batch processing
    private class RawEventRecord
    {
        public int EventId { get; set; }
        public string? EventCode { get; set; }
        public string? EventName { get; set; }
        public string? EventDesc { get; set; }
        public int Round { get; set; }
        public int EventType { get; set; }
        public string? CurrentStatus { get; set; }
        public int ParentId { get; set; }
        public int PricingStatus { get; set; }
        public int IsExtend { get; set; }
        public int EventCurrencyId { get; set; }
        public int IschkIsSendMail { get; set; }
        public int ClientSAPId { get; set; }
        public DateTime? TechnicalApprovalSendDate { get; set; }
        public DateTime? TechnicalApprovalApprovedDate { get; set; }
        public string? TechnicalApprovalStatus { get; set; }
        public int IsStandalone { get; set; }
        public int? EnterBy { get; set; }
        public DateTime? EnterDate { get; set; }
        // Event Setting fields
        public int EventMode { get; set; }
        public int TiePreventLot { get; set; }
        public int TiePreventItem { get; set; }
        public int IsTargetPriceApplicable { get; set; }
        public int IsAutoExtendedEnable { get; set; }
        public int NoofTimesAutoExtended { get; set; }
        public int AutoExtendedMinutes { get; set; }
        public int ApplyExtendedTimes { get; set; }
        public decimal GreenPercentage { get; set; }
        public decimal YellowPercentage { get; set; }
        public int IsItemLevelRankShow { get; set; }
        public int IsLotLevelRankShow { get; set; }
        public int IsLotLevelAuction { get; set; }
        public int IsBasePriceforLotLevel { get; set; }
        public int IsBasicPriceApplicable { get; set; }
        public int IsBasicPriceValidationReq { get; set; }
        public int IsMinMaxBidApplicable { get; set; }
        public int IsLowestBidShow { get; set; }
        public int BesideAuctionFirstBid { get; set; }
        public decimal MinBid { get; set; }
        public decimal MaxBid { get; set; }
        public decimal LotLevelBasicPrice { get; set; }
        public int IsPriceBidAttachmentcompulsory { get; set; }
        public int IsDiscountApplicable { get; set; }
        public int IsGSTCompulsory { get; set; }
        public int IsTechnicalAttachmentcompulsory { get; set; }
        public int IsProposedQty { get; set; }
        public int IsRedyStockmandatory { get; set; }
        public int MinBidMode { get; set; }
        public int MaxBidMode { get; set; }
    }

    private class ProcessedEventRecord
    {
        public int EventId { get; set; }
        public string EventCode { get; set; } = "";
        public string EventName { get; set; } = "";
        public string EventDescription { get; set; } = "";
        public int Round { get; set; }
        public string EventType { get; set; } = "";
        public string EventStatus { get; set; } = "";
        public int ParentId { get; set; }
        public string PriceBidTemplate { get; set; } = "";
        public bool IsStandalone { get; set; }
        public bool PricingStatus { get; set; }
        public bool EventExtended { get; set; }
        public int EventCurrencyId { get; set; }
        public bool DisableMailInNextRound { get; set; }
        public int CompanyId { get; set; }
        public DateTime? TechnicalApprovalSendDate { get; set; }
        public DateTime? TechnicalApprovalApprovedDate { get; set; }
        public string? TechnicalApprovalStatus { get; set; }
        public int? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        // Event Setting fields
        public string EventMode { get; set; } = "";
        public bool TiePreventLot { get; set; }
        public bool TiePreventItem { get; set; }
        public bool TargetPriceApplicable { get; set; }
        public bool AutoExtendedEnable { get; set; }
        public int NoofTimesAutoExtended { get; set; }
        public int AutoExtendedMinutes { get; set; }
        public bool ApplyExtendedTimes { get; set; }
        public decimal GreenPercentage { get; set; }
        public decimal YellowPercentage { get; set; }
        public bool ShowItemLevelRank { get; set; }
        public bool ShowLotLevelRank { get; set; }
        public bool BasicPriceApplicable { get; set; }
        public bool BasicPriceValidationMandatory { get; set; }
        public bool MinMaxBidApplicable { get; set; }
        public bool ShowLowerBid { get; set; }
        public bool ApplyAllSettingsInPriceBid { get; set; }
        public decimal MinLotAuctionBidValue { get; set; }
        public decimal MaxLotAuctionBidValue { get; set; }
        public bool ConfigureLotLevelAuction { get; set; }
        public decimal LotLevelBasicPrice { get; set; }
        public bool PriceBidAttachmentMandatory { get; set; }
        public bool DiscountApplicable { get; set; }
        public bool GstMandatory { get; set; }
        public bool TechnicalAttachmentMandatory { get; set; }
        public bool ProposedQty { get; set; }
        public bool ReadyStockMandatory { get; set; }
        public int MaxLotBidType { get; set; }
        public int MinLotBidType { get; set; }
    }

    protected override string SelectQuery => @"
        SELECT 
            e.EVENTID, e.EVENTCODE, e.EVENTNAME, e.EVENTDESC, e.ROUND, e.EVENTTYPE, e.CURRENTSTATUS, 
            e.PARENTID, e.PRICINGSTATUS, e.ISEXTEND, e.EventCurrencyId, e.IschkIsSendMail, e.ClientSAPId,
            e.TechnicalApprovalSendDate, e.TechnicalApprovalApprovedDate, e.TechnicalApprovalStatus,
            e.ENTERBY, e.ENTERDATE,
            e.EventMode, e.TiePreventLot, e.TiePreventItem, e.IsTargetPriceApplicable, 
            e.IsAutoExtendedEnable, e.NoofTimesAutoExtended, e.AutoExtendedMinutes, e.ApplyExtendedTimes,
            e.GREENPERCENTAGE, e.YELLOWPERCENTAGE, e.IsItemLevelRankShow, e.IsLotLevelRankShow,
            e.IsLotLevelAuction, e.IsBasePriceforLotLevel, e.IsBasicPriceApplicable, e.IsBasicPriceValidationReq, 
            e.IsMinMaxBidApplicable, e.IsLowestBidShow, e.BesideAuctionFirstBid, e.MinBid, e.MaxBid,
            e.LotLevelBasicPrice, e.IsPriceBidAttachmentcompulsory, e.IsDiscountApplicable,
            e.IsGSTCompulsory, e.IsTechnicalAttachmentcompulsory, e.IsProposedQty, e.IsRedyStockmandatory,
            e.MinBidMode, e.MaxBidMode,
            CASE WHEN EXISTS (
                SELECT 1 FROM tbl_PB_BUyer pb
                INNER JOIN TBL_PRTRANSACTION pt ON pt.PRTRANSID = pb.PRTRANSID
                INNER JOIN TBL_PRMASTER pm ON pm.PRID = pt.PRID
                WHERE pb.EVENTID = e.EVENTID AND ISNULL(pm.PR_NUM,'') = ''
            ) THEN 1 ELSE 0 END AS IS_STANDALONE
        FROM TBL_EVENTMASTER e
        ORDER BY e.EVENTID";

    protected override string InsertQuery => @"
        INSERT INTO event_master (
            event_id, event_code, event_name, event_description, round, event_type, 
            event_status, parent_id, price_bid_template, is_standalone, pricing_status, 
            event_extended, event_currency_id, disable_mail_in_next_round, company_id,
            technical_approval_send_date, technical_approval_approved_date, 
            technical_approval_status, created_by, created_date
        ) VALUES (
            @event_id, @event_code, @event_name, @event_description, @round, @event_type, 
            @event_status, @parent_id, @price_bid_template, @is_standalone, @pricing_status, 
            @event_extended, @event_currency_id, @disable_mail_in_next_round, @company_id,
            @technical_approval_send_date, @technical_approval_approved_date, 
            @technical_approval_status, @created_by, @created_date
        ) RETURNING event_id";

    private string InsertEventSettingQuery => @"
        INSERT INTO event_setting (
            event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
            auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
            apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
            show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
            min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
            min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
            lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
            gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
            created_by, created_date, lot_level_target_price, max_lot_bid_type, 
            min_lot_bid_type, allow_currency_selection
        ) VALUES (
            @event_id, @event_mode, @tie_prevent_lot, @tie_prevent_item, @target_price_applicable,
            @auto_extended_enable, @no_of_times_auto_extended, @auto_extended_minutes,
            @apply_extended_times, @green_percentage, @yellow_percentage, @show_item_level_rank,
            @show_lot_level_rank, @basic_price_applicable, @basic_price_validation_mandatory,
            @min_max_bid_applicable, @show_lower_bid, @apply_all_settings_in_price_bid,
            @min_lot_auction_bid_value, @max_lot_auction_bid_value, @configure_lot_level_auction,
            @lot_level_basic_price, @price_bid_attachment_mandatory, @discount_applicable,
            @gst_mandatory, @technical_attachment_mandatory, @proposed_qty, @ready_stock_mandatory,
            @created_by, @created_date, @lot_level_target_price, @max_lot_bid_type,
            @min_lot_bid_type, @allow_currency_selection
        )";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "EVENTID -> event_id (Direct)",
            "EVENTCODE -> event_code (Direct)",
            "EVENTNAME -> event_name (Direct)",
            "EVENTDESC -> event_description (Direct)",
            "ROUND -> round (Direct)",
            "EVENTTYPE -> event_type (Transform: 1=RFQ, 2=Reverse Auction, 3=Forward Auction)",
            "CURRENTSTATUS -> event_status (Direct)",
            "PARENTID -> parent_id (Direct, 0 if NULL)",
            "price_bid_template -> TBL_PB_BUYER.PBType (Lookup: 1=material, 14=service)",
            "is_standalone -> Calculated (true if PR_NUM is empty in related records via joins)",
            "PRICINGSTATUS -> pricing_status (Direct)",
            "ISEXTEND -> event_extended (Direct)",
            "EventCurrencyId -> event_currency_id (Direct)",
            "IschkIsSendMail -> disable_mail_in_next_round (Direct)",
            "ClientSAPId -> company_id (Direct)",
            "TechnicalApprovalSendDate -> technical_approval_send_date (Direct)",
            "TechnicalApprovalApprovedDate -> technical_approval_approved_date (Direct)",
            "TechnicalApprovalStatus -> technical_approval_status (Direct)",
            "ENTERBY -> created_by (Direct, defaults to 0 if NULL)",
            "ENTERDATE -> created_date (Direct, defaults to NOW() if NULL)",
            "--- Event Setting Table ---",
            "event_id -> event_id (From event_master)",
            "EventMode -> event_mode (Transform: 1='Rank', 2='Color', else '')",
            "TiePreventLot -> tie_prevent_lot (Direct)",
            "TiePreventItem -> tie_prevent_item (Direct)",
            "IsTargetPriceApplicable -> target_price_applicable (Direct)",
            "IsAutoExtendedEnable -> auto_extended_enable (Direct)",
            "NoofTimesAutoExtended -> no_of_times_auto_extended (Direct)",
            "AutoExtendedMinutes -> auto_extended_minutes (Direct)",
            "ApplyExtendedTimes -> apply_extended_times (Direct)",
            "GREENPERCENTAGE -> green_percentage (Direct)",
            "YELLOWPERCENTAGE -> yellow_percentage (Direct)",
            "IsItemLevelRankShow -> show_item_level_rank (Direct)",
            "IsLotLevelRankShow -> show_lot_level_rank (Direct)",
            "basic_price_applicable -> IF(IsLotLevelAuction > 0) THEN IsBasePriceforLotLevel ELSE IsBasicPriceApplicable (Conditional)",
            "IsBasicPriceValidationReq -> basic_price_validation_mandatory (Direct)",
            "IsMinMaxBidApplicable -> min_max_bid_applicable (Direct)",
            "IsLowestBidShow -> show_lower_bid (Direct)",
            "BesideAuctionFirstBid -> apply_all_settings_in_price_bid (Direct)",
            "MinBid -> min_lot_auction_bid_value (Direct)",
            "MaxBid -> max_lot_auction_bid_value (Direct)",
            "IsLotLevelAuction -> configure_lot_level_auction (Direct)",
            "LotLevelBasicPrice -> lot_level_basic_price (Direct)",
            "IsPriceBidAttachmentcompulsory -> price_bid_attachment_mandatory (Direct)",
            "IsDiscountApplicable -> discount_applicable (Direct)",
            "IsGSTCompulsory -> gst_mandatory (Direct)",
            "IsTechnicalAttachmentcompulsory -> technical_attachment_mandatory (Direct)",
            "IsProposedQty -> proposed_qty (Direct)",
            "IsRedyStockmandatory -> ready_stock_mandatory (Direct)",
            "lot_level_target_price -> 0 (Fixed)",
            "MinBidMode -> max_lot_bid_type (Direct)",
            "MaxBidMode -> min_lot_bid_type (Direct)",
            "allow_currency_selection -> 0 (Fixed)"
        };
    }

    public async Task<(int SuccessCount, int FailedCount, List<string> Errors)> MigrateAsync()
    {
        var stopwatch = Stopwatch.StartNew();
        int successCount = 0;
        int failedCount = 0;
        int totalRecords = 0;
        var errors = new List<string>();
        var skippedRecords = new List<(int EventId, string Reason)>();

        SqlConnection? sqlConnection = null;
        NpgsqlConnection? pgConnection = null;

        try
        {
            _logger.LogInformation("Starting optimized EventMaster migration...");

            sqlConnection = GetSqlServerConnection();
            pgConnection = GetPostgreSqlConnection();

            await sqlConnection.OpenAsync();
            _logger.LogInformation("SQL Server connection opened successfully");
            
            // Small delay to ensure connection is fully established
            await Task.Delay(100);
            
            await pgConnection.OpenAsync();
            _logger.LogInformation("PostgreSQL connection opened successfully");

            // Get total count for progress tracking
            using (var countCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_EVENTMASTER", sqlConnection))
            {
                var scalarResult = await countCmd.ExecuteScalarAsync();
                totalRecords = scalarResult != null && scalarResult != DBNull.Value ? Convert.ToInt32(scalarResult) : 0;
                _logger.LogInformation($"Total records to migrate: {totalRecords}");
                
                if (totalRecords == 0)
                {
                    _logger.LogWarning("No records found in TBL_EVENTMASTER. Migration will be skipped.");
                    return (0, 0, new List<string> { "No records found in source table" });
                }
            }

            // Pre-load price_bid_template mappings to avoid repeated lookups
            var priceBidTemplateCache = await LoadPriceBidTemplateCacheAsync(sqlConnection);
            _logger.LogInformation($"Loaded {priceBidTemplateCache.Count} price_bid_template mappings");

            // Load valid foreign key IDs
            _logger.LogInformation("🔍 Loading valid foreign key IDs...");
            _validCurrencyIds = await LoadValidCurrencyIdsAsync(pgConnection);
            _validCompanyIds = await LoadValidCompanyIdsAsync(pgConnection);
            _defaultCurrencyId = await GetDefaultCurrencyIdAsync(pgConnection);
            _defaultCompanyId = await GetDefaultCompanyIdAsync(pgConnection);

            // Use optimized bulk migration
            var result = await ExecuteOptimizedBulkMigrationAsync(sqlConnection, pgConnection, totalRecords, priceBidTemplateCache, stopwatch);
            
            successCount = result.SuccessCount;
            failedCount = result.FailedCount;
            errors = result.Errors;
        }
        catch (Exception ex)
        {
            var error = $"Migration failed with exception: {ex.Message}\nStack Trace: {ex.StackTrace}";
            _logger.LogError(error);
            errors.Add(error);
        }
        finally
        {
            // Ensure connections are properly disposed
            if (pgConnection != null)
            {
                try
                {
                    if (pgConnection.State == ConnectionState.Open)
                    {
                        await pgConnection.CloseAsync();
                        _logger.LogInformation("PostgreSQL connection closed");
                    }
                    await pgConnection.DisposeAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error closing PostgreSQL connection: {ex.Message}");
                }
            }
            
            if (sqlConnection != null)
            {
                try
                {
                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        await sqlConnection.CloseAsync();
                        _logger.LogInformation("SQL Server connection closed");
                    }
                    await sqlConnection.DisposeAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"Error closing SQL Server connection: {ex.Message}");
                }
            }
        }

        stopwatch.Stop();
        _logger.LogInformation($"EventMaster migration completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds. Success: {successCount}, Failed: {failedCount}, Total: {totalRecords}");
        
        if (errors.Any())
        {
            _logger.LogWarning($"Migration completed with {errors.Count} errors:");
            foreach (var error in errors.Take(10))
            {
                _logger.LogWarning($"  - {error}");
            }
        }

        // Export migration stats and skipped records to Excel
        MigrationStatsExporter.ExportToExcel(
            "EventMasterMigrationStats.xlsx",
            totalRecords,
            successCount,
            skippedRecords.Count,
            _logger,
            skippedRecords.Select(r => (r.EventId.ToString(), r.Reason)).ToList()
        );

        return (successCount, failedCount, errors);
    }

    private async Task<Dictionary<int, string>> LoadPriceBidTemplateCacheAsync(SqlConnection sqlConnection)
    {
        var cache = new Dictionary<int, string>();
        try
        {
            var query = @"
                SELECT DISTINCT pb.EVENTID, pb.PBType
                FROM TBL_PB_BUYER pb
                WHERE pb.SEQUENCEID = 0";

            using var cmd = new SqlCommand(query, sqlConnection);
            using var reader = await cmd.ExecuteReaderAsync();
            
            while (await reader.ReadAsync())
            {
                var eventId = reader.GetInt32(0);
                var pbType = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                
                var template = pbType switch
                {
                    1 => "Material",
                    14 => "Service",
                    _ => $"Type_{pbType}"
                };
                
                cache[eventId] = template;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning($"Failed to load price_bid_template cache: {ex.Message}");
        }
        
        return cache;
    }

    private async Task<HashSet<int>> LoadValidCurrencyIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();
        try
        {
            var query = "SELECT currency_id FROM currency_master WHERE is_deleted = false OR is_deleted IS NULL";
            using var cmd = new NpgsqlCommand(query, pgConn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }
            _logger.LogInformation($"✓ Loaded {validIds.Count:N0} valid currency IDs");
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Failed to load valid currency IDs: {ex.Message}");
        }
        return validIds;
    }

    private async Task<HashSet<int>> LoadValidCompanyIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();
        try
        {
            var query = "SELECT company_id FROM company_master WHERE is_deleted = false OR is_deleted IS NULL";
            using var cmd = new NpgsqlCommand(query, pgConn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }
            _logger.LogInformation($"✓ Loaded {validIds.Count:N0} valid company IDs");
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Failed to load valid company IDs: {ex.Message}");
        }
        return validIds;
    }

    private async Task<int> GetDefaultCurrencyIdAsync(NpgsqlConnection pgConn)
    {
        try
        {
            var query = @"
                SELECT currency_id 
                FROM currency_master 
                WHERE LOWER(currency_code) IN ('inr', 'usd', 'eur')
                AND (is_deleted = false OR is_deleted IS NULL)
                ORDER BY CASE LOWER(currency_code) WHEN 'inr' THEN 1 WHEN 'usd' THEN 2 ELSE 3 END
                LIMIT 1";
            
            using var cmd = new NpgsqlCommand(query, pgConn);
            var result = await cmd.ExecuteScalarAsync();
            
            if (result != null)
            {
                int defaultId = Convert.ToInt32(result);
                _logger.LogInformation($"✓ Default currency ID: {defaultId}");
                return defaultId;
            }
            
            // Fallback: get any currency
            query = "SELECT currency_id FROM currency_master LIMIT 1";
            cmd.CommandText = query;
            result = await cmd.ExecuteScalarAsync();
            
            if (result != null)
            {
                return Convert.ToInt32(result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Failed to get default currency: {ex.Message}");
        }
        
        _logger.LogWarning("⚠️ No currency found, using fallback ID: 1");
        return 1;
    }

    private async Task<int> GetDefaultCompanyIdAsync(NpgsqlConnection pgConn)
    {
        try
        {
            var query = @"
                SELECT company_id 
                FROM company_master 
                WHERE is_deleted = false OR is_deleted IS NULL
                ORDER BY company_id
                LIMIT 1";
            
            using var cmd = new NpgsqlCommand(query, pgConn);
            var result = await cmd.ExecuteScalarAsync();
            
            if (result != null)
            {
                int defaultId = Convert.ToInt32(result);
                _logger.LogInformation($"✓ Default company ID: {defaultId}");
                return defaultId;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Failed to get default company: {ex.Message}");
        }
        
        _logger.LogWarning("⚠️ No company found, using fallback ID: 1");
        return 1;
    }

    private async Task<(int SuccessCount, int FailedCount, List<string> Errors)> ExecuteOptimizedBulkMigrationAsync(
        SqlConnection sqlConn,
        NpgsqlConnection pgConn,
        int totalRecords,
        Dictionary<int, string> priceBidTemplateCache,
        Stopwatch stopwatch)
    {
        int insertedCount = 0;
        int processedCount = 0;
        int errorCount = 0;
        var errors = new ConcurrentBag<string>();

        var rawQueue = new BlockingCollection<RawEventRecord>(Math.Max(1000, _transformWorkerCount * 500));
        var writeQueue = new BlockingCollection<List<ProcessedEventRecord>>(Math.Max(10, _transformWorkerCount));
        var cts = new CancellationTokenSource();
        var token = cts.Token;

        Exception? backgroundException = null;

        // Writer task (single writer for transaction safety)
        var writerTask = Task.Run(async () =>
        {
            try
            {
                var now = DateTime.UtcNow;

                foreach (var batch in writeQueue.GetConsumingEnumerable(token))
                {
                    try
                    {
                        // Insert event_master records using COPY
                        await InsertEventMasterBatchAsync(pgConn, batch, now);

                        Interlocked.Add(ref insertedCount, batch.Count);

                        if (insertedCount % 1000 == 0)
                        {
                            _logger.LogInformation($"Inserted {insertedCount}/{totalRecords} records ({insertedCount * 100.0 / totalRecords:F1}%) in {stopwatch.Elapsed.TotalSeconds:F1}s");
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Add(ref errorCount, batch.Count);
                        
                        // Log first few event IDs in the failed batch for debugging
                        var eventIds = string.Join(", ", batch.Take(5).Select(r => r.EventId));
                        var error = $"Error writing batch of {batch.Count} records (Event IDs: {eventIds}...): {ex.Message}";
                        errors.Add(error);
                        _logger.LogError(error);
                        
                        // Try to find which record has the issue
                        foreach (var record in batch.Take(3))
                        {
                            var nullByteFields = new List<string>();
                            if (record.EventCode.Contains('\0')) nullByteFields.Add("EventCode");
                            if (record.EventName.Contains('\0')) nullByteFields.Add("EventName");
                            if (record.EventDescription.Contains('\0')) nullByteFields.Add("EventDescription");
                            if (record.EventType.Contains('\0')) nullByteFields.Add("EventType");
                            if (record.EventStatus.Contains('\0')) nullByteFields.Add("EventStatus");
                            if (record.PriceBidTemplate.Contains('\0')) nullByteFields.Add("PriceBidTemplate");
                            if (record.TechnicalApprovalStatus?.Contains('\0') == true) nullByteFields.Add("TechnicalApprovalStatus");
                            
                            if (nullByteFields.Any())
                            {
                                _logger.LogError($"  Event ID {record.EventId} has null bytes in: {string.Join(", ", nullByteFields)}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                backgroundException = ex;
                cts.Cancel();
            }
        }, token);

        // Transform worker tasks
        var transformTasks = new List<Task>();
        for (int w = 0; w < _transformWorkerCount; w++)
        {
            transformTasks.Add(Task.Run(() =>
            {
                try
                {
                    var localBatch = new List<ProcessedEventRecord>(_batchSize);
                    
                    foreach (var raw in rawQueue.GetConsumingEnumerable(token))
                    {
                        if (token.IsCancellationRequested) break;

                        try
                        {
                            var processed = TransformEventRecord(raw, priceBidTemplateCache);
                            localBatch.Add(processed);

                            if (localBatch.Count >= _batchSize)
                            {
                                writeQueue.Add(localBatch, token);
                                localBatch = new List<ProcessedEventRecord>(_batchSize);
                            }
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref errorCount);
                            var error = $"Error transforming Event ID {raw.EventId}: {ex.Message}";
                            errors.Add(error);
                            _logger.LogError(error);
                        }
                    }

                    if (localBatch.Count > 0)
                    {
                        writeQueue.Add(localBatch, token);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // Expected
                }
                catch (Exception ex)
                {
                    backgroundException = ex;
                    cts.Cancel();
                }
            }, token));
        }

        // Reader task
        try
        {
            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 600;
            using var reader = await sqlCommand.ExecuteReaderAsync();

            // Cache ordinals
            var ordinals = new
            {
                EventId = reader.GetOrdinal("EVENTID"),
                EventCode = reader.GetOrdinal("EVENTCODE"),
                EventName = reader.GetOrdinal("EVENTNAME"),
                EventDesc = reader.GetOrdinal("EVENTDESC"),
                Round = reader.GetOrdinal("ROUND"),
                EventType = reader.GetOrdinal("EVENTTYPE"),
                CurrentStatus = reader.GetOrdinal("CURRENTSTATUS"),
                ParentId = reader.GetOrdinal("PARENTID"),
                PricingStatus = reader.GetOrdinal("PRICINGSTATUS"),
                IsExtend = reader.GetOrdinal("ISEXTEND"),
                EventCurrencyId = reader.GetOrdinal("EventCurrencyId"),
                IschkIsSendMail = reader.GetOrdinal("IschkIsSendMail"),
                ClientSAPId = reader.GetOrdinal("ClientSAPId"),
                TechnicalApprovalSendDate = reader.GetOrdinal("TechnicalApprovalSendDate"),
                TechnicalApprovalApprovedDate = reader.GetOrdinal("TechnicalApprovalApprovedDate"),
                TechnicalApprovalStatus = reader.GetOrdinal("TechnicalApprovalStatus"),
                IsStandalone = reader.GetOrdinal("IS_STANDALONE"),
                EnterBy = reader.GetOrdinal("ENTERBY"),
                EnterDate = reader.GetOrdinal("ENTERDATE"),
                EventMode = reader.GetOrdinal("EventMode"),
                TiePreventLot = reader.GetOrdinal("TiePreventLot"),
                TiePreventItem = reader.GetOrdinal("TiePreventItem"),
                IsTargetPriceApplicable = reader.GetOrdinal("IsTargetPriceApplicable"),
                IsAutoExtendedEnable = reader.GetOrdinal("IsAutoExtendedEnable"),
                NoofTimesAutoExtended = reader.GetOrdinal("NoofTimesAutoExtended"),
                AutoExtendedMinutes = reader.GetOrdinal("AutoExtendedMinutes"),
                ApplyExtendedTimes = reader.GetOrdinal("ApplyExtendedTimes"),
                GreenPercentage = reader.GetOrdinal("GREENPERCENTAGE"),
                YellowPercentage = reader.GetOrdinal("YELLOWPERCENTAGE"),
                IsItemLevelRankShow = reader.GetOrdinal("IsItemLevelRankShow"),
                IsLotLevelRankShow = reader.GetOrdinal("IsLotLevelRankShow"),
                IsLotLevelAuction = reader.GetOrdinal("IsLotLevelAuction"),
                IsBasePriceforLotLevel = reader.GetOrdinal("IsBasePriceforLotLevel"),
                IsBasicPriceApplicable = reader.GetOrdinal("IsBasicPriceApplicable"),
                IsBasicPriceValidationReq = reader.GetOrdinal("IsBasicPriceValidationReq"),
                IsMinMaxBidApplicable = reader.GetOrdinal("IsMinMaxBidApplicable"),
                IsLowestBidShow = reader.GetOrdinal("IsLowestBidShow"),
                BesideAuctionFirstBid = reader.GetOrdinal("BesideAuctionFirstBid"),
                MinBid = reader.GetOrdinal("MinBid"),
                MaxBid = reader.GetOrdinal("MaxBid"),
                LotLevelBasicPrice = reader.GetOrdinal("LotLevelBasicPrice"),
                IsPriceBidAttachmentcompulsory = reader.GetOrdinal("IsPriceBidAttachmentcompulsory"),
                IsDiscountApplicable = reader.GetOrdinal("IsDiscountApplicable"),
                IsGSTCompulsory = reader.GetOrdinal("IsGSTCompulsory"),
                IsTechnicalAttachmentcompulsory = reader.GetOrdinal("IsTechnicalAttachmentcompulsory"),
                IsProposedQty = reader.GetOrdinal("IsProposedQty"),
                IsRedyStockmandatory = reader.GetOrdinal("IsRedyStockmandatory"),
                MinBidMode = reader.GetOrdinal("MinBidMode"),
                MaxBidMode = reader.GetOrdinal("MaxBidMode")
            };

            while (await reader.ReadAsync())
            {
                if (token.IsCancellationRequested) break;

                Interlocked.Increment(ref processedCount);

                var raw = new RawEventRecord
                {
                    EventId = reader.GetInt32(ordinals.EventId),
                    EventCode = reader.IsDBNull(ordinals.EventCode) ? null : reader.GetString(ordinals.EventCode),
                    EventName = reader.IsDBNull(ordinals.EventName) ? null : reader.GetString(ordinals.EventName),
                    EventDesc = reader.IsDBNull(ordinals.EventDesc) ? null : reader.GetString(ordinals.EventDesc),
                    Round = reader.IsDBNull(ordinals.Round) ? 0 : reader.GetInt32(ordinals.Round),
                    EventType = reader.IsDBNull(ordinals.EventType) ? 0 : reader.GetInt32(ordinals.EventType),
                    CurrentStatus = reader.IsDBNull(ordinals.CurrentStatus) ? null : reader.GetString(ordinals.CurrentStatus),
                    ParentId = reader.IsDBNull(ordinals.ParentId) ? 0 : reader.GetInt32(ordinals.ParentId),
                    PricingStatus = reader.IsDBNull(ordinals.PricingStatus) ? 0 : reader.GetInt32(ordinals.PricingStatus),
                    IsExtend = reader.IsDBNull(ordinals.IsExtend) ? 0 : reader.GetInt32(ordinals.IsExtend),
                    EventCurrencyId = reader.IsDBNull(ordinals.EventCurrencyId) || reader.GetInt32(ordinals.EventCurrencyId) == 0 
                        ? _defaultCurrencyId 
                        : reader.GetInt32(ordinals.EventCurrencyId),
                    IschkIsSendMail = reader.IsDBNull(ordinals.IschkIsSendMail) ? 0 : reader.GetInt32(ordinals.IschkIsSendMail),
                    ClientSAPId = reader.IsDBNull(ordinals.ClientSAPId) || reader.GetInt32(ordinals.ClientSAPId) == 0 
                        ? _defaultCompanyId 
                        : reader.GetInt32(ordinals.ClientSAPId),
                    TechnicalApprovalSendDate = reader.IsDBNull(ordinals.TechnicalApprovalSendDate) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinals.TechnicalApprovalSendDate), DateTimeKind.Utc),
                    TechnicalApprovalApprovedDate = reader.IsDBNull(ordinals.TechnicalApprovalApprovedDate) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinals.TechnicalApprovalApprovedDate), DateTimeKind.Utc),
                    TechnicalApprovalStatus = reader.IsDBNull(ordinals.TechnicalApprovalStatus) ? null : reader.GetString(ordinals.TechnicalApprovalStatus),
                    IsStandalone = reader.IsDBNull(ordinals.IsStandalone) ? 0 : reader.GetInt32(ordinals.IsStandalone),
                    EnterBy = reader.IsDBNull(ordinals.EnterBy) ? null : reader.GetInt32(ordinals.EnterBy),
                    EnterDate = reader.IsDBNull(ordinals.EnterDate) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinals.EnterDate), DateTimeKind.Utc),
                    EventMode = reader.IsDBNull(ordinals.EventMode) ? 0 : reader.GetInt32(ordinals.EventMode),
                    TiePreventLot = reader.IsDBNull(ordinals.TiePreventLot) ? 0 : reader.GetInt32(ordinals.TiePreventLot),
                    TiePreventItem = reader.IsDBNull(ordinals.TiePreventItem) ? 0 : reader.GetInt32(ordinals.TiePreventItem),
                    IsTargetPriceApplicable = reader.IsDBNull(ordinals.IsTargetPriceApplicable) ? 0 : reader.GetInt32(ordinals.IsTargetPriceApplicable),
                    IsAutoExtendedEnable = reader.IsDBNull(ordinals.IsAutoExtendedEnable) ? 0 : reader.GetInt32(ordinals.IsAutoExtendedEnable),
                    NoofTimesAutoExtended = reader.IsDBNull(ordinals.NoofTimesAutoExtended) ? 0 : reader.GetInt32(ordinals.NoofTimesAutoExtended),
                    AutoExtendedMinutes = reader.IsDBNull(ordinals.AutoExtendedMinutes) ? 0 : reader.GetInt32(ordinals.AutoExtendedMinutes),
                    ApplyExtendedTimes = reader.IsDBNull(ordinals.ApplyExtendedTimes) ? 0 : reader.GetInt32(ordinals.ApplyExtendedTimes),
                    GreenPercentage = reader.IsDBNull(ordinals.GreenPercentage) ? 0 : reader.GetDecimal(ordinals.GreenPercentage),
                    YellowPercentage = reader.IsDBNull(ordinals.YellowPercentage) ? 0 : reader.GetDecimal(ordinals.YellowPercentage),
                    IsItemLevelRankShow = reader.IsDBNull(ordinals.IsItemLevelRankShow) ? 0 : reader.GetInt32(ordinals.IsItemLevelRankShow),
                    IsLotLevelRankShow = reader.IsDBNull(ordinals.IsLotLevelRankShow) ? 0 : reader.GetInt32(ordinals.IsLotLevelRankShow),
                    IsLotLevelAuction = reader.IsDBNull(ordinals.IsLotLevelAuction) ? 0 : reader.GetInt32(ordinals.IsLotLevelAuction),
                    IsBasePriceforLotLevel = reader.IsDBNull(ordinals.IsBasePriceforLotLevel) ? 0 : reader.GetInt32(ordinals.IsBasePriceforLotLevel),
                    IsBasicPriceApplicable = reader.IsDBNull(ordinals.IsBasicPriceApplicable) ? 0 : reader.GetInt32(ordinals.IsBasicPriceApplicable),
                    IsBasicPriceValidationReq = reader.IsDBNull(ordinals.IsBasicPriceValidationReq) ? 0 : reader.GetInt32(ordinals.IsBasicPriceValidationReq),
                    IsMinMaxBidApplicable = reader.IsDBNull(ordinals.IsMinMaxBidApplicable) ? 0 : reader.GetInt32(ordinals.IsMinMaxBidApplicable),
                    IsLowestBidShow = reader.IsDBNull(ordinals.IsLowestBidShow) ? 0 : reader.GetInt32(ordinals.IsLowestBidShow),
                    BesideAuctionFirstBid = reader.IsDBNull(ordinals.BesideAuctionFirstBid) ? 0 : reader.GetInt32(ordinals.BesideAuctionFirstBid),
                    MinBid = reader.IsDBNull(ordinals.MinBid) ? 0 : reader.GetDecimal(ordinals.MinBid),
                    MaxBid = reader.IsDBNull(ordinals.MaxBid) ? 0 : reader.GetDecimal(ordinals.MaxBid),
                    LotLevelBasicPrice = reader.IsDBNull(ordinals.LotLevelBasicPrice) ? 0 : reader.GetDecimal(ordinals.LotLevelBasicPrice),
                    IsPriceBidAttachmentcompulsory = reader.IsDBNull(ordinals.IsPriceBidAttachmentcompulsory) ? 0 : (reader.GetBoolean(ordinals.IsPriceBidAttachmentcompulsory) ? 1 : 0),
                    IsDiscountApplicable = reader.IsDBNull(ordinals.IsDiscountApplicable) ? 0 : reader.GetInt32(ordinals.IsDiscountApplicable),
                    IsGSTCompulsory = reader.IsDBNull(ordinals.IsGSTCompulsory) ? 0 : reader.GetInt32(ordinals.IsGSTCompulsory),
                    IsTechnicalAttachmentcompulsory = reader.IsDBNull(ordinals.IsTechnicalAttachmentcompulsory) ? 0 : (reader.GetBoolean(ordinals.IsTechnicalAttachmentcompulsory) ? 1 : 0),
                    IsProposedQty = reader.IsDBNull(ordinals.IsProposedQty) ? 0 : reader.GetInt32(ordinals.IsProposedQty),
                    IsRedyStockmandatory = reader.IsDBNull(ordinals.IsRedyStockmandatory) ? 0 : reader.GetInt32(ordinals.IsRedyStockmandatory),
                    MinBidMode = reader.IsDBNull(ordinals.MinBidMode) ? 0 : reader.GetInt32(ordinals.MinBidMode),
                    MaxBidMode = reader.IsDBNull(ordinals.MaxBidMode) ? 0 : reader.GetInt32(ordinals.MaxBidMode)
                };

                // Validate foreign keys
                if (!_validCurrencyIds.Contains(raw.EventCurrencyId))
                {
                    if (_skippedInvalidCurrency < 10)
                    {
                        _logger.LogWarning($"⚠️ Event {raw.EventId}: Invalid currency_id {raw.EventCurrencyId}, using default {_defaultCurrencyId}");
                    }
                    raw.EventCurrencyId = _defaultCurrencyId;
                    Interlocked.Increment(ref _skippedInvalidCurrency);
                }

                if (!_validCompanyIds.Contains(raw.ClientSAPId))
                {
                    if (_skippedInvalidCompany < 10)
                    {
                        _logger.LogWarning($"⚠️ Event {raw.EventId}: Invalid company_id {raw.ClientSAPId}, using default {_defaultCompanyId}");
                    }
                    raw.ClientSAPId = _defaultCompanyId;
                    Interlocked.Increment(ref _skippedInvalidCompany);
                }

                // Ensure Round is at least 1
                if (raw.Round == 0)
                {
                    raw.Round = 1;
                }

                rawQueue.Add(raw, token);

                if (processedCount % 5000 == 0)
                {
                    _logger.LogInformation($"Read {processedCount}/{totalRecords} records ({processedCount * 100.0 / totalRecords:F1}%)");
                }
            }

            rawQueue.CompleteAdding();
            await Task.WhenAll(transformTasks);
            writeQueue.CompleteAdding();
            await writerTask;

            if (backgroundException != null)
            {
                throw backgroundException;
            }
        }
        catch (Exception ex)
        {
            cts.Cancel();
            var error = $"Migration failed after processing {processedCount} records: {ex.Message}";
            errors.Add(error);
            _logger.LogError(error);
            throw;
        }
        finally
        {
            rawQueue.Dispose();
            writeQueue.Dispose();
            cts.Dispose();
        }

        // Log validation statistics
        _logger.LogInformation("═══════════════════════════════════════");
        _logger.LogInformation("📊 Event Master Migration Summary");
        _logger.LogInformation("═══════════════════════════════════════");
        _logger.LogInformation($"  ✓ Successfully inserted: {insertedCount:N0}");
        _logger.LogInformation($"  ⚠️ Invalid currency (corrected): {_skippedInvalidCurrency:N0}");
        _logger.LogInformation($"  ⚠️ Invalid company (corrected): {_skippedInvalidCompany:N0}");
        _logger.LogInformation($"  ❌ Failed (errors): {errorCount:N0}");
        _logger.LogInformation("═══════════════════════════════════════");

        return (insertedCount, errorCount, errors.ToList());
    }

    private ProcessedEventRecord TransformEventRecord(RawEventRecord raw, Dictionary<int, string> priceBidTemplateCache)
    {
        var eventType = raw.EventType switch
        {
            1 => "RFQ",
            2 => "Reverse Auction",
            3 => "Forward Auction",
            _ => $"Unknown_{raw.EventType}"
        };

        // Transform EventMode: 1 = 'Rank', 2 = 'Color', else ''
        var eventMode = raw.EventMode switch
        {
            1 => "Rank",
            2 => "Color",
            _ => ""
        };

        priceBidTemplateCache.TryGetValue(raw.EventId, out var priceBidTemplate);

        // Conditional logic for basic_price_applicable:
        // If IsLotLevelAuction > 0, use IsBasePriceforLotLevel, else use IsBasicPriceApplicable
        var isLotLevelAuction = raw.IsLotLevelAuction != 0;
        var basicPriceApplicable = isLotLevelAuction ? (raw.IsBasePriceforLotLevel != 0) : (raw.IsBasicPriceApplicable != 0);

        return new ProcessedEventRecord
        {
            EventId = raw.EventId,
            EventCode = SanitizeString(raw.EventCode),
            EventName = SanitizeString(raw.EventName),
            EventDescription = SanitizeString(raw.EventDesc),
            Round = raw.Round,
            EventType = SanitizeString(eventType),
            EventStatus = SanitizeString(raw.CurrentStatus),
            ParentId = raw.ParentId,
            PriceBidTemplate = SanitizeString(priceBidTemplate),
            IsStandalone = raw.IsStandalone != 0,
            PricingStatus = raw.PricingStatus != 0,
            EventExtended = raw.IsExtend != 0,
            EventCurrencyId = raw.EventCurrencyId,
            DisableMailInNextRound = raw.IschkIsSendMail != 0,
            CompanyId = raw.ClientSAPId,
            TechnicalApprovalSendDate = raw.TechnicalApprovalSendDate,
            TechnicalApprovalApprovedDate = raw.TechnicalApprovalApprovedDate,
            TechnicalApprovalStatus = SanitizeString(raw.TechnicalApprovalStatus),
            CreatedBy = raw.EnterBy,
            CreatedDate = raw.EnterDate,
            EventMode = eventMode,
            TiePreventLot = raw.TiePreventLot != 0,
            TiePreventItem = raw.TiePreventItem != 0,
            TargetPriceApplicable = raw.IsTargetPriceApplicable != 0,
            AutoExtendedEnable = raw.IsAutoExtendedEnable != 0,
            NoofTimesAutoExtended = raw.NoofTimesAutoExtended,
            AutoExtendedMinutes = raw.AutoExtendedMinutes,
            ApplyExtendedTimes = raw.ApplyExtendedTimes != 0,
            GreenPercentage = raw.GreenPercentage,
            YellowPercentage = raw.YellowPercentage,
            ShowItemLevelRank = raw.IsItemLevelRankShow != 0,
            ShowLotLevelRank = raw.IsLotLevelRankShow != 0,
            BasicPriceApplicable = basicPriceApplicable,
            BasicPriceValidationMandatory = raw.IsBasicPriceValidationReq != 0,
            MinMaxBidApplicable = raw.IsMinMaxBidApplicable != 0,
            ShowLowerBid = raw.IsLowestBidShow != 0,
            ApplyAllSettingsInPriceBid = raw.BesideAuctionFirstBid != 0,
            MinLotAuctionBidValue = raw.MinBid,
            MaxLotAuctionBidValue = raw.MaxBid,
            ConfigureLotLevelAuction = raw.IsLotLevelAuction != 0,
            LotLevelBasicPrice = raw.LotLevelBasicPrice,
            PriceBidAttachmentMandatory = raw.IsPriceBidAttachmentcompulsory != 0,
            DiscountApplicable = raw.IsDiscountApplicable != 0,
            GstMandatory = raw.IsGSTCompulsory != 0,
            TechnicalAttachmentMandatory = raw.IsTechnicalAttachmentcompulsory != 0,
            ProposedQty = raw.IsProposedQty != 0,
            ReadyStockMandatory = raw.IsRedyStockmandatory != 0,
            MaxLotBidType = raw.MinBidMode,
            MinLotBidType = raw.MaxBidMode
        };
    }

    private async Task InsertEventMasterBatchAsync(NpgsqlConnection pgConn, List<ProcessedEventRecord> batch, DateTime now)
    {
        var tempTableName = $"temp_event_master_{Guid.NewGuid():N}";
        
        try
        {
            // Create a temporary table for COPY, then use INSERT ... ON CONFLICT
            // NOTE: No "ON COMMIT DROP" - we'll manually drop it to ensure it exists for the entire operation
            var createTempTableCmd = $@"
                CREATE TEMP TABLE {tempTableName} (LIKE event_master INCLUDING DEFAULTS)";
            
            using (var cmd = new NpgsqlCommand(createTempTableCmd, pgConn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            // Use BINARY COPY to load data into temp table
            var copyCommand = $@"COPY {tempTableName} (
                event_id, event_code, event_name, event_description, round, event_type, 
                event_status, parent_id, price_bid_template, is_standalone, pricing_status, 
                event_extended, event_currency_id, disable_mail_in_next_round, company_id,
                technical_approval_send_date, technical_approval_approved_date, 
                technical_approval_status, created_by, created_date
            ) FROM STDIN (FORMAT BINARY)";

            using (var writer = await pgConn.BeginBinaryImportAsync(copyCommand))
            {
                foreach (var record in batch)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(record.EventId, NpgsqlDbType.Integer);
                    await writer.WriteAsync(SanitizeString(record.EventCode), NpgsqlDbType.Text);
                    await writer.WriteAsync(SanitizeString(record.EventName), NpgsqlDbType.Text);
                    await writer.WriteAsync(SanitizeString(record.EventDescription), NpgsqlDbType.Text);
                    await writer.WriteAsync(record.Round, NpgsqlDbType.Integer);
                    await writer.WriteAsync(SanitizeString(record.EventType), NpgsqlDbType.Text);
                    await writer.WriteAsync(SanitizeString(record.EventStatus), NpgsqlDbType.Text);
                    await writer.WriteAsync(record.ParentId, NpgsqlDbType.Integer);
                    await writer.WriteAsync(SanitizeString(record.PriceBidTemplate), NpgsqlDbType.Text);
                    await writer.WriteAsync(record.IsStandalone, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.PricingStatus, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.EventExtended, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.EventCurrencyId, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.DisableMailInNextRound, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.CompanyId, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.TechnicalApprovalSendDate, NpgsqlDbType.TimestampTz);
                    await writer.WriteAsync(record.TechnicalApprovalApprovedDate, NpgsqlDbType.TimestampTz);
                    await writer.WriteAsync(record.TechnicalApprovalStatus, NpgsqlDbType.Text);
                    await writer.WriteAsync(record.CreatedBy ?? 0, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.CreatedDate ?? now, NpgsqlDbType.TimestampTz);
                }
                
                await writer.CompleteAsync();
            }

            // Insert from temp table with ON CONFLICT DO UPDATE
            var upsertCmd = $@"
                INSERT INTO event_master (
                    event_id, event_code, event_name, event_description, round, event_type, 
                    event_status, parent_id, price_bid_template, is_standalone, pricing_status, 
                    event_extended, event_currency_id, disable_mail_in_next_round, company_id,
                    technical_approval_send_date, technical_approval_approved_date, 
                    technical_approval_status, created_by, created_date
                )
                SELECT 
                    event_id, event_code, event_name, event_description, round, event_type, 
                    event_status, parent_id, price_bid_template, is_standalone, pricing_status, 
                    event_extended, event_currency_id, disable_mail_in_next_round, company_id,
                    technical_approval_send_date, technical_approval_approved_date, 
                    technical_approval_status, created_by, created_date
                FROM {tempTableName}
                ON CONFLICT (event_id) DO UPDATE SET
                    event_code = EXCLUDED.event_code,
                    event_name = EXCLUDED.event_name,
                    event_description = EXCLUDED.event_description,
                    round = EXCLUDED.round,
                    event_type = EXCLUDED.event_type,
                    event_status = EXCLUDED.event_status,
                    parent_id = EXCLUDED.parent_id,
                    price_bid_template = EXCLUDED.price_bid_template,
                    is_standalone = EXCLUDED.is_standalone,
                    pricing_status = EXCLUDED.pricing_status,
                    event_extended = EXCLUDED.event_extended,
                    event_currency_id = EXCLUDED.event_currency_id,
                    disable_mail_in_next_round = EXCLUDED.disable_mail_in_next_round,
                    company_id = EXCLUDED.company_id,
                    technical_approval_send_date = EXCLUDED.technical_approval_send_date,
                    technical_approval_approved_date = EXCLUDED.technical_approval_approved_date,
                    technical_approval_status = EXCLUDED.technical_approval_status,
                    created_by = EXCLUDED.created_by,
                    created_date = EXCLUDED.created_date";
            
            using (var cmd = new NpgsqlCommand(upsertCmd, pgConn))
            {
                cmd.CommandTimeout = 300;
                await cmd.ExecuteNonQueryAsync();
            }
            
            _logger.LogInformation($"✓ Successfully upserted {batch.Count} event_master records via BINARY COPY + ON CONFLICT");
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Failed to insert event_master batch: {ex.Message}");
            if (batch.Count > 0)
            {
                _logger.LogError($"   First record: EventId={batch[0].EventId}, EventCode='{batch[0].EventCode}'");
            }
            throw;
        }
        finally
        {
            // Clean up the temp table
            try
            {
                using var dropCmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {tempTableName}", pgConn);
                await dropCmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"⚠️ Failed to drop temp table {tempTableName}: {ex.Message}");
            }
        }
    }

    /* COMMENTED OUT - Event settings migration moved to separate EventSettingMigrationService
    private async Task InsertEventSettingBatchAsync(NpgsqlConnection pgConn, List<ProcessedEventRecord> batch, DateTime now)
    {
        var tempTableName = $"temp_event_setting_{Guid.NewGuid():N}";
        
        try
        {
            // Create a temporary table for COPY, then use INSERT ... ON CONFLICT
            var createTempTableCmd = $@"
                CREATE TEMP TABLE {tempTableName} (LIKE event_setting INCLUDING DEFAULTS)";
            
            using (var cmd = new NpgsqlCommand(createTempTableCmd, pgConn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            // Use BINARY format to load data into temp table
            var copyCommand = $@"COPY {tempTableName} (
                event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
                auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
                apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
                show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
                min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
                min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
                lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
                gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
                created_by, created_date, lot_level_target_price, max_lot_bid_type, 
                min_lot_bid_type, allow_currency_selection
            ) FROM STDIN (FORMAT BINARY)";

            using (var writer = await pgConn.BeginBinaryImportAsync(copyCommand))
            {
                foreach (var record in batch)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(record.EventId, NpgsqlDbType.Integer);
                    await writer.WriteAsync(SanitizeString(record.EventMode), NpgsqlDbType.Text);
                    await writer.WriteAsync(record.TiePreventLot, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.TiePreventItem, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.TargetPriceApplicable, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.AutoExtendedEnable, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.NoofTimesAutoExtended, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.AutoExtendedMinutes, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.ApplyExtendedTimes, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.GreenPercentage, NpgsqlDbType.Numeric);
                    await writer.WriteAsync(record.YellowPercentage, NpgsqlDbType.Numeric);
                    await writer.WriteAsync(record.ShowItemLevelRank, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.ShowLotLevelRank, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.BasicPriceApplicable, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.BasicPriceValidationMandatory, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.MinMaxBidApplicable, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.ShowLowerBid, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.ApplyAllSettingsInPriceBid, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.MinLotAuctionBidValue, NpgsqlDbType.Numeric);
                    await writer.WriteAsync(record.MaxLotAuctionBidValue, NpgsqlDbType.Numeric);
                    await writer.WriteAsync(record.ConfigureLotLevelAuction, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.LotLevelBasicPrice, NpgsqlDbType.Numeric);
                    await writer.WriteAsync(record.PriceBidAttachmentMandatory, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.DiscountApplicable, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.GstMandatory, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.TechnicalAttachmentMandatory, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.ProposedQty, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.ReadyStockMandatory, NpgsqlDbType.Boolean);
                    await writer.WriteAsync(record.CreatedBy ?? 0, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.CreatedDate ?? now, NpgsqlDbType.TimestampTz);
                    await writer.WriteAsync(0m, NpgsqlDbType.Numeric); // lot_level_target_price (numeric, not integer)
                    await writer.WriteAsync(record.MaxLotBidType, NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.MinLotBidType, NpgsqlDbType.Integer);
                    await writer.WriteAsync(false, NpgsqlDbType.Boolean); // allow_currency_selection
                }
                
                await writer.CompleteAsync();
            }

            // Insert from temp table with ON CONFLICT DO UPDATE
            var upsertCmd = $@"
                INSERT INTO event_setting (
                    event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
                    auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
                    apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
                    show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
                    min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
                    min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
                    lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
                    gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
                    created_by, created_date, lot_level_target_price, max_lot_bid_type, 
                    min_lot_bid_type, allow_currency_selection
                )
                SELECT 
                    event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
                    auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
                    apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
                    show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
                    min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
                    min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
                    lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
                    gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
                    created_by, created_date, lot_level_target_price, max_lot_bid_type, 
                    min_lot_bid_type, allow_currency_selection
                FROM {tempTableName}
                ON CONFLICT (event_id) DO UPDATE SET
                    event_mode = EXCLUDED.event_mode,
                    tie_prevent_lot = EXCLUDED.tie_prevent_lot,
                    tie_prevent_item = EXCLUDED.tie_prevent_item,
                    target_price_applicable = EXCLUDED.target_price_applicable,
                    auto_extended_enable = EXCLUDED.auto_extended_enable,
                    no_of_times_auto_extended = EXCLUDED.no_of_times_auto_extended,
                    auto_extended_minutes = EXCLUDED.auto_extended_minutes,
                    apply_extended_times = EXCLUDED.apply_extended_times,
                    green_percentage = EXCLUDED.green_percentage,
                    yellow_percentage = EXCLUDED.yellow_percentage,
                    show_item_level_rank = EXCLUDED.show_item_level_rank,
                    show_lot_level_rank = EXCLUDED.show_lot_level_rank,
                    basic_price_applicable = EXCLUDED.basic_price_applicable,
                    basic_price_validation_mandatory = EXCLUDED.basic_price_validation_mandatory,
                    min_max_bid_applicable = EXCLUDED.min_max_bid_applicable,
                    show_lower_bid = EXCLUDED.show_lower_bid,
                    apply_all_settings_in_price_bid = EXCLUDED.apply_all_settings_in_price_bid,
                    min_lot_auction_bid_value = EXCLUDED.min_lot_auction_bid_value,
                    max_lot_auction_bid_value = EXCLUDED.max_lot_auction_bid_value,
                    configure_lot_level_auction = EXCLUDED.configure_lot_level_auction,
                    lot_level_basic_price = EXCLUDED.lot_level_basic_price,
                    price_bid_attachment_mandatory = EXCLUDED.price_bid_attachment_mandatory,
                    discount_applicable = EXCLUDED.discount_applicable,
                    gst_mandatory = EXCLUDED.gst_mandatory,
                    technical_attachment_mandatory = EXCLUDED.technical_attachment_mandatory,
                    proposed_qty = EXCLUDED.proposed_qty,
                    ready_stock_mandatory = EXCLUDED.ready_stock_mandatory,
                    created_by = EXCLUDED.created_by,
                    created_date = EXCLUDED.created_date,
                    lot_level_target_price = EXCLUDED.lot_level_target_price,
                    max_lot_bid_type = EXCLUDED.max_lot_bid_type,
                    min_lot_bid_type = EXCLUDED.min_lot_bid_type,
                    allow_currency_selection = EXCLUDED.allow_currency_selection";
            
            using (var cmd = new NpgsqlCommand(upsertCmd, pgConn))
            {
                cmd.CommandTimeout = 300;
                await cmd.ExecuteNonQueryAsync();
            }
            
            _logger.LogInformation($"✓ Successfully upserted {batch.Count} event_setting records via BINARY COPY + ON CONFLICT");
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Failed to insert event_setting batch: {ex.Message}");
            if (batch.Count > 0)
            {
                _logger.LogError($"   First record: EventId={batch[0].EventId}");
            }
            throw;
        }
        finally
        {
            // Clean up the temp table
            try
            {
                using var dropCmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {tempTableName}", pgConn);
                await dropCmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"⚠️ Failed to drop temp table {tempTableName}: {ex.Message}");
            }
        }
    }
    */
    // END COMMENTED OUT - Event settings migration

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "event_master");
        _migrationLogger.LogInfo("Starting migration");

        // For EventMasterMigration, we use the optimized bulk logic
        // This is a wrapper to satisfy the base class interface
        var (successCount, failedCount, errors) = await MigrateAsync();
        return successCount;
    }
}

