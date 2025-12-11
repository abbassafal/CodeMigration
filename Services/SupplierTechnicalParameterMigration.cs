using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class SupplierTechnicalParameterMigration : MigrationService
{
    private const int BATCH_SIZE = 5000; // Increased for COPY operations
    private readonly ILogger<SupplierTechnicalParameterMigration> _logger;
    private MigrationLogger? _migrationLogger;
    
    // Track skipped records for detailed reporting
    private List<(string RecordId, string Reason)> _skippedRecords = new List<(string, string)>();
    
    // Strongly-typed record class for batch processing with COPY
    private class SupplierTechnicalParameterRecord
    {
        public long SupplierTechnicalParameterId { get; set; }
        public long UserTechnicalParameterId { get; set; }
        public string? SupplierTechnicalParameterResponse { get; set; }
        public int SupplierId { get; set; }
        public int? EventItemId { get; set; }
        public int? EventId { get; set; }
        public int? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public int? ModifiedBy { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public bool IsDeleted { get; set; }
        public int? DeletedBy { get; set; }
        public DateTime? DeletedDate { get; set; }
    }

    protected override string SelectQuery => @"
        SELECT
            TBL_VendorTechItemTerm.VendorTechItemTermId,
            TBL_VendorTechItemTerm.TechItemTermId AS User_technical_Parameter_id,
            TBL_VendorTechItemTerm.Value AS Supplier_technical_Parameter_Response,
            TBL_VendorTechItemTerm.VendorId AS Supplier_id,
            TBL_VendorTechItemTerm.PBID AS event_Item_Id,
            TBL_PB_BUYER.EVENTID
        FROM TBL_VendorTechItemTerm
        INNER JOIN TBL_PB_BUYER ON TBL_PB_BUYER.PBID = TBL_VendorTechItemTerm.PBID
        ORDER BY TBL_VendorTechItemTerm.VendorTechItemTermId";

    protected override string InsertQuery => ""; // Not used with COPY

    public SupplierTechnicalParameterMigration(IConfiguration configuration, ILogger<SupplierTechnicalParameterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // supplier_technical_parameter_id
            "Direct",  // user_technical_parameter_id
            "Direct",  // supplier_technical_parameter_response
            "Direct",  // supplier_id
            "Direct",  // event_item_id (from PBID)
            "Direct",  // event_id (from TBL_PB_BUYER.EVENTID via INNER JOIN)
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
            new { source = "VendorTechItemTermId", logic = "VendorTechItemTermId -> supplier_technical_parameter_id (Primary key, autoincrement - SupplierTechnicalParameterId)", target = "supplier_technical_parameter_id" },
            new { source = "TechItemTermId", logic = "TechItemTermId -> user_technical_parameter_id (Foreign key to user_technical_parameter - UserTechnicalParameterId)", target = "user_technical_parameter_id" },
            new { source = "Value", logic = "Value -> supplier_technical_parameter_response (SupplierTechnicalParameterResponse)", target = "supplier_technical_parameter_response" },
            new { source = "VendorId", logic = "VendorId -> supplier_id (Foreign key to supplier_master - SupplierId)", target = "supplier_id" },
            new { source = "PBID", logic = "PBID -> event_item_id (Direct from TBL_VendorTechItemTerm.PBID)", target = "event_item_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Direct from TBL_PB_BUYER via INNER JOIN on PBID)", target = "event_id" },
            new { source = "-", logic = "created_by -> NULL (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: false);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "supplier_technical_parameter");
        _migrationLogger.LogInfo("Starting optimized migration with COPY");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load validation data
            var validUserTechParamIds = await LoadValidUserTechnicalParameterIdsAsync(pgConn);
            var validSupplierIds = await LoadValidSupplierIdsAsync(pgConn);
            _logger.LogInformation($"Loaded validation data: {validUserTechParamIds.Count} user_technical_parameters, {validSupplierIds.Count} suppliers");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;
            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<SupplierTechnicalParameterRecord>();
            var processedIds = new HashSet<long>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                // Fast field access by ordinal
                var vendorTechItemTermId = reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0);
                var techItemTermId = reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1);
                var value = reader.IsDBNull(2) ? null : reader.GetString(2);
                var vendorId = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
                var pbId = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);
                var eventId = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5);

                // Skip if primary key is NULL
                if (!vendorTechItemTermId.HasValue)
                {
                    skippedRecords++;
                    _skippedRecords.Add(("NULL", "VendorTechItemTermId is NULL"));
                    continue;
                }

                long id = vendorTechItemTermId.Value;

                // Skip duplicates
                if (processedIds.Contains(id))
                {
                    skippedRecords++;
                    _skippedRecords.Add((id.ToString(), "Duplicate record"));
                    continue;
                }

                // Skip if user_technical_parameter_id is NULL
                if (!techItemTermId.HasValue)
                {
                    skippedRecords++;
                    _skippedRecords.Add((id.ToString(), "user_technical_parameter_id is NULL"));
                    continue;
                }

                // Validate user_technical_parameter_id
                if (!validUserTechParamIds.Contains(techItemTermId.Value))
                {
                    skippedRecords++;
                    _skippedRecords.Add((id.ToString(), $"Invalid user_technical_parameter_id: {techItemTermId.Value}"));
                    continue;
                }

                // Skip if supplier_id is NULL
                if (!vendorId.HasValue)
                {
                    skippedRecords++;
                    _skippedRecords.Add((id.ToString(), "supplier_id is NULL"));
                    continue;
                }

                // Validate supplier_id
                if (!validSupplierIds.Contains(vendorId.Value))
                {
                    skippedRecords++;
                    _skippedRecords.Add((id.ToString(), $"Invalid supplier_id: {vendorId.Value}"));
                    continue;
                }

                var record = new SupplierTechnicalParameterRecord
                {
                    SupplierTechnicalParameterId = id,
                    UserTechnicalParameterId = techItemTermId.Value,
                    SupplierTechnicalParameterResponse = value,
                    SupplierId = vendorId.Value,
                    EventItemId = pbId,
                    EventId = eventId
                };

                batch.Add(record);
                processedIds.Add(id);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchWithCopyAsync(batch, pgConn);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                    
                    if (totalRecords % 10000 == 0)
                    {
                        var elapsed = stopwatch.Elapsed;
                        var rate = totalRecords / elapsed.TotalSeconds;
                        _logger.LogInformation($"Progress: {totalRecords:N0} processed, {migratedRecords:N0} inserted, {rate:F1} records/sec");
                    }
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithCopyAsync(batch, pgConn);
                migratedRecords += batchMigrated;
            }

            stopwatch.Stop();
            var totalRate = migratedRecords / stopwatch.Elapsed.TotalSeconds;
            _logger.LogInformation($"Supplier Technical Parameter migration completed in {stopwatch.Elapsed:mm\\:ss}. Total: {totalRecords:N0}, Migrated: {migratedRecords:N0}, Skipped: {skippedRecords:N0}, Rate: {totalRate:F1} records/sec");

            // Export migration statistics
            MigrationStatsExporter.ExportToExcel(
                "SupplierTechnicalParameter_migration_stats.xlsx",
                totalRecords,
                migratedRecords,
                skippedRecords,
                _logger,
                _skippedRecords
            );

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Technical Parameter migration");
            throw;
        }
    }

    private async Task<HashSet<long>> LoadValidUserTechnicalParameterIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<long>();

        try
        {
            var query = "SELECT user_technical_parameter_id FROM user_technical_parameter WHERE user_technical_parameter_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt64(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid user_technical_parameter IDs from user_technical_parameter");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid user_technical_parameter IDs");
        }

        return validIds;
    }

    private async Task<HashSet<int>> LoadValidSupplierIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT supplier_id FROM supplier_master WHERE supplier_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid supplier IDs from supplier_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid supplier IDs");
        }

        return validIds;
    }



    private async Task<int> InsertBatchWithCopyAsync(List<SupplierTechnicalParameterRecord> batch, NpgsqlConnection pgConn)
    {
        if (batch.Count == 0) return 0;

        int insertedCount = 0;

        try
        {
            // Use PostgreSQL COPY for ultra-fast bulk insert
            var copyCommand = @"COPY supplier_technical_parameter (
                supplier_technical_parameter_id,
                user_technical_parameter_id,
                supplier_technical_parameter_response,
                supplier_id,
                event_item_id,
                event_id,
                created_by,
                created_date,
                modified_by,
                modified_date,
                is_deleted,
                deleted_by,
                deleted_date
            ) FROM STDIN (FORMAT BINARY)";

            using (var writer = await pgConn.BeginBinaryImportAsync(copyCommand))
            {
                foreach (var record in batch)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(record.SupplierTechnicalParameterId, NpgsqlTypes.NpgsqlDbType.Bigint);
                    await writer.WriteAsync(record.UserTechnicalParameterId, NpgsqlTypes.NpgsqlDbType.Bigint);
                    await writer.WriteAsync(record.SupplierTechnicalParameterResponse, NpgsqlTypes.NpgsqlDbType.Text);
                    await writer.WriteAsync(record.SupplierId, NpgsqlTypes.NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.EventItemId, NpgsqlTypes.NpgsqlDbType.Integer);
                    await writer.WriteAsync(record.EventId, NpgsqlTypes.NpgsqlDbType.Integer);
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // created_by
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // created_date
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // modified_by
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // modified_date
                    await writer.WriteAsync(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_deleted
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // deleted_by
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // deleted_date
                    
                    insertedCount++;
                }

                await writer.CompleteAsync();
            }

            _logger.LogDebug($"Inserted {insertedCount} records using COPY");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records with COPY");
            // Fall back to individual inserts on error
            insertedCount = await InsertBatchWithFallbackAsync(batch, pgConn);
        }

        return insertedCount;
    }

    private async Task<int> InsertBatchWithFallbackAsync(List<SupplierTechnicalParameterRecord> batch, NpgsqlConnection pgConn)
    {
        int insertedCount = 0;
        var insertQuery = @"INSERT INTO supplier_technical_parameter (
            supplier_technical_parameter_id,
            user_technical_parameter_id,
            supplier_technical_parameter_response,
            supplier_id,
            event_item_id,
            event_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_technical_parameter_id,
            @user_technical_parameter_id,
            @supplier_technical_parameter_response,
            @supplier_id,
            @event_item_id,
            @event_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        ) ON CONFLICT (supplier_technical_parameter_id) DO NOTHING";

        foreach (var record in batch)
        {
            try
            {
                using var cmd = new NpgsqlCommand(insertQuery, pgConn);
                cmd.Parameters.AddWithValue("@supplier_technical_parameter_id", record.SupplierTechnicalParameterId);
                cmd.Parameters.AddWithValue("@user_technical_parameter_id", record.UserTechnicalParameterId);
                cmd.Parameters.AddWithValue("@supplier_technical_parameter_response", (object?)record.SupplierTechnicalParameterResponse ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@supplier_id", record.SupplierId);
                cmd.Parameters.AddWithValue("@event_item_id", (object?)record.EventItemId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@event_id", (object?)record.EventId ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@created_by", DBNull.Value);
                cmd.Parameters.AddWithValue("@created_date", DBNull.Value);
                cmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                cmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                cmd.Parameters.AddWithValue("@is_deleted", false);
                cmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                cmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                await cmd.ExecuteNonQueryAsync();
                insertedCount++;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Failed to insert record with ID {record.SupplierTechnicalParameterId}");
                _skippedRecords.Add((record.SupplierTechnicalParameterId.ToString(), $"Insert failed: {ex.Message}"));
            }
        }

        return insertedCount;
    }
}
