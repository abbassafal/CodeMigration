using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class AuctionMinMaxTargetPriceMigration
    {
        private readonly ILogger<AuctionMinMaxTargetPriceMigration> _logger;
        private readonly IConfiguration _configuration;

        public AuctionMinMaxTargetPriceMigration(IConfiguration configuration, ILogger<AuctionMinMaxTargetPriceMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Id", target = "auction_min_max_target_price_id", type = "int -> integer" },
                new { source = "EventId", target = "event_id", type = "numeric -> integer (FK to event_master)" },
                new { source = "PRtransId", target = "event_item_id", type = "int -> integer (FK via event_items.erp_pr_lines_id)" },
                new { source = "Target", target = "item_target_price", type = "numeric -> numeric" },
                new { source = "TBL_PB_BUYER.BasicPrice", target = "item_basic_price", type = "decimal -> numeric (from TBL_PB_BUYER)" },
                new { source = "MinBid", target = "item_minimum_bid_price", type = "decimal -> numeric" },
                new { source = "MaxBid", target = "item_maximum_bid_price", type = "decimal -> numeric" },
                new { source = "MinBidMode", target = "min_bid_type", type = "int -> text (1='Value', 2='Percentage')" },
                new { source = "MaxBidMode", target = "max_bid_type", type = "int -> text (1='Value', 2='Percentage')" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;
            var errors = new List<string>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting AuctionMinMaxTargetPrice migration...");

                // Get valid event_ids from PostgreSQL (remove is_deleted filter to include all events)
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_id FROM event_master", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }

                _logger.LogInformation($"Found {validEventIds.Count} valid event_ids (including deleted)");

                // Get event_item_id mapping from erp_pr_lines_id (PRtransId) - remove is_deleted filter
                var erp_pr_lines_idToEventItemIdMap = new Dictionary<int, int>();
                using (var cmd = new NpgsqlCommand("SELECT event_item_id, erp_pr_lines_id FROM event_items WHERE erp_pr_lines_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var eventItemId = reader.GetInt32(0);
                        var erp_pr_lines_id = reader.GetInt32(1);
                        erp_pr_lines_idToEventItemIdMap[erp_pr_lines_id] = eventItemId;
                    }
                }

                _logger.LogInformation($"Found {erp_pr_lines_idToEventItemIdMap.Count} event_item mappings from erp_pr_lines_id (including deleted)");

                // Fetch data from SQL Server with JOIN to TBL_PB_BUYER for BasicPrice
                var sourceData = new List<(int Id, int? EventId, int PRtransId, decimal? Target, decimal? BasicPrice, decimal? MinBid, decimal? MaxBid, int? MinBidMode, int? MaxBidMode)>();
                
                using (var cmd = new SqlCommand(@"SELECT 
                    itp.Id,
                    itp.EventId,
                    itp.PRtransId,
                    itp.Target,
                    pb.BasicPrice,
                    itp.MinBid,
                    itp.MaxBid,
                    itp.MinBidMode,
                    itp.MaxBidMode
                FROM TBL_ItemTargetPrice itp
                LEFT JOIN TBL_PB_BUYER pb ON itp.PRtransId = pb.PRtransId AND itp.EventId = pb.EventId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add((
                            reader.GetInt32(0),                                      // Id
                            reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetValue(1)), // EventId (numeric to int)
                            reader.GetInt32(2),                                      // PRtransId
                            reader.IsDBNull(3) ? null : reader.GetDecimal(3),       // Target
                            reader.IsDBNull(4) ? null : reader.GetDecimal(4),       // BasicPrice
                            reader.IsDBNull(5) ? null : reader.GetDecimal(5),       // MinBid
                            reader.IsDBNull(6) ? null : reader.GetDecimal(6),       // MaxBid
                            reader.IsDBNull(7) ? null : reader.GetInt32(7),         // MinBidMode
                            reader.IsDBNull(8) ? null : reader.GetInt32(8)          // MaxBidMode
                        ));
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id exists (FK constraint)
                        if (!record.EventId.HasValue || !validEventIds.Contains(record.EventId.Value))
                        {
                            var errorMsg = $"Id {record.Id}: event_id {record.EventId} not found in event_master (FK constraint violation)";
                            _logger.LogWarning(errorMsg);
                            errors.Add(errorMsg);
                            skippedRecords++;
                            continue;
                        }

                        // Get event_item_id from PRtransId mapping
                        if (!erp_pr_lines_idToEventItemIdMap.TryGetValue(record.PRtransId, out var eventItemId))
                        {
                            var errorMsg = $"Id {record.Id}: PRtransId {record.PRtransId} not found in event_items.erp_pr_lines_id mapping (FK constraint violation)";
                            _logger.LogWarning(errorMsg);
                            errors.Add(errorMsg);
                            skippedRecords++;
                            continue;
                        }

                        // Check if record already exists
                        int? existingRecordId = null;
                        using (var checkCmd = new NpgsqlCommand(
                            "SELECT auction_min_max_target_price_id FROM auction_min_max_target_price WHERE auction_min_max_target_price_id = @Id",
                            pgConnection))
                        {
                            checkCmd.Parameters.AddWithValue("@Id", record.Id);
                            var result = await checkCmd.ExecuteScalarAsync();
                            if (result != null && result != DBNull.Value)
                            {
                                existingRecordId = Convert.ToInt32(result);
                            }
                        }

                        // Prepare values with defaults and conversions
                        var itemTargetPrice = record.Target ?? 0.0m;
                        var itemBasicPrice = record.BasicPrice ?? 0.0m;
                        var itemMinimumBidPrice = record.MinBid ?? 0.0m;
                        var itemMaximumBidPrice = record.MaxBid ?? 0.0m;
                        var minBidType = ConvertBidModeToType(record.MinBidMode);
                        var maxBidType = ConvertBidModeToType(record.MaxBidMode);

                        if (existingRecordId.HasValue)
                        {
                            // Update existing record
                            using var updateCmd = new NpgsqlCommand(
                                @"UPDATE auction_min_max_target_price SET
                                    event_id = @EventId,
                                    event_item_id = @EventItemId,
                                    item_target_price = @ItemTargetPrice,
                                    item_basic_price = @ItemBasicPrice,
                                    item_minimum_bid_price = @ItemMinimumBidPrice,
                                    item_maximum_bid_price = @ItemMaximumBidPrice,
                                    min_bid_type = @MinBidType,
                                    max_bid_type = @MaxBidType,
                                    modified_by = NULL,
                                    modified_date = CURRENT_TIMESTAMP
                                WHERE auction_min_max_target_price_id = @Id",
                                pgConnection);

                            updateCmd.Parameters.AddWithValue("@Id", record.Id);
                            updateCmd.Parameters.AddWithValue("@EventId", record.EventId.Value);
                            updateCmd.Parameters.AddWithValue("@EventItemId", eventItemId);
                            updateCmd.Parameters.AddWithValue("@ItemTargetPrice", itemTargetPrice);
                            updateCmd.Parameters.AddWithValue("@ItemBasicPrice", itemBasicPrice);
                            updateCmd.Parameters.AddWithValue("@ItemMinimumBidPrice", itemMinimumBidPrice);
                            updateCmd.Parameters.AddWithValue("@ItemMaximumBidPrice", itemMaximumBidPrice);
                            updateCmd.Parameters.AddWithValue("@MinBidType", minBidType);
                            updateCmd.Parameters.AddWithValue("@MaxBidType", maxBidType);

                            await updateCmd.ExecuteNonQueryAsync();
                            _logger.LogDebug($"Updated record with Id: {record.Id}");
                        }
                        else
                        {
                            // Insert new record
                            using var insertCmd = new NpgsqlCommand(
                                @"INSERT INTO auction_min_max_target_price (
                                    auction_min_max_target_price_id,
                                    event_id,
                                    event_item_id,
                                    item_target_price,
                                    item_basic_price,
                                    item_minimum_bid_price,
                                    item_maximum_bid_price,
                                    min_bid_type,
                                    max_bid_type,
                                    created_by,
                                    created_date,
                                    modified_by,
                                    modified_date,
                                    is_deleted,
                                    deleted_by,
                                    deleted_date
                                ) VALUES (
                                    @Id,
                                    @EventId,
                                    @EventItemId,
                                    @ItemTargetPrice,
                                    @ItemBasicPrice,
                                    @ItemMinimumBidPrice,
                                    @ItemMaximumBidPrice,
                                    @MinBidType,
                                    @MaxBidType,
                                    NULL,
                                    CURRENT_TIMESTAMP,
                                    NULL,
                                    NULL,
                                    false,
                                    NULL,
                                    NULL
                                )",
                                pgConnection);

                            insertCmd.Parameters.AddWithValue("@Id", record.Id);
                            insertCmd.Parameters.AddWithValue("@EventId", record.EventId.Value);
                            insertCmd.Parameters.AddWithValue("@EventItemId", eventItemId);
                            insertCmd.Parameters.AddWithValue("@ItemTargetPrice", itemTargetPrice);
                            insertCmd.Parameters.AddWithValue("@ItemBasicPrice", itemBasicPrice);
                            insertCmd.Parameters.AddWithValue("@ItemMinimumBidPrice", itemMinimumBidPrice);
                            insertCmd.Parameters.AddWithValue("@ItemMaximumBidPrice", itemMaximumBidPrice);
                            insertCmd.Parameters.AddWithValue("@MinBidType", minBidType);
                            insertCmd.Parameters.AddWithValue("@MaxBidType", maxBidType);

                            await insertCmd.ExecuteNonQueryAsync();
                            _logger.LogDebug($"Inserted new record with Id: {record.Id}");
                        }

                        migratedRecords++;
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"Id {record.Id}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                    }
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
                
                if (errors.Any())
                {
                    _logger.LogWarning($"Encountered {errors.Count} errors during migration");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            return migratedRecords;
        }

        private string ConvertBidModeToType(int? bidMode)
        {
            // Convert bid mode: 1 = Value, 2 = Percentage
            // Default to "Value" if null or unknown
            return bidMode switch
            {
                1 => "Value",
                2 => "Percentage",
                _ => "Value"
            };
        }
    }
}
