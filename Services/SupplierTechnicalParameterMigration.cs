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
    
    // Strongly-typed record class for batch processing with COPY
    private class SupplierTechnicalParameterRecord
    {
        public int SupplierTechnicalParameterId { get; set; }  // Changed from long to int
        public int UserTechnicalParameterId { get; set; }      // Changed from long to int
        public string? SupplierTechnicalParameterResponse { get; set; }
        public int SupplierId { get; set; }
        public int EventItemId { get; set; }  // Changed from nullable to NOT NULL (matches schema)
        public int EventId { get; set; }      // Changed from nullable to NOT NULL (matches schema)
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
            new { source = "VendorTechItemTermId", target = "supplier_technical_parameter_id" },
            new { source = "TechItemTermId", target = "user_technical_parameter_id" },
            new { source = "Value", target = "supplier_technical_parameter_response" },
            new { source = "VendorId", target = "supplier_id" },
            new { source = "PBID", target = "event_item_id" },
            new { source = "EVENTID", target = "event_id" },
            new { source = "Fixed:NULL", target = "created_by" },
            new { source = "Fixed:NULL", target = "created_date" },
            new { source = "Fixed:NULL", target = "modified_by" },
            new { source = "Fixed:NULL", target = "modified_date" },
            new { source = "Fixed:false", target = "is_deleted" },
            new { source = "Fixed:NULL", target = "deleted_by" },
            new { source = "Fixed:NULL", target = "deleted_date" }
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

            // Load all existing IDs from PostgreSQL to skip duplicates
            var existingTargetIds = new HashSet<int>();
            try
            {
                var idQuery = "SELECT supplier_technical_parameter_id FROM supplier_technical_parameter";
                using var idCmd = new NpgsqlCommand(idQuery, pgConn);
                using var idReader = await idCmd.ExecuteReaderAsync();
                while (await idReader.ReadAsync())
                {
                    existingTargetIds.Add(idReader.GetInt32(0));
                }
                _logger.LogInformation($"Loaded {existingTargetIds.Count} existing supplier_technical_parameter_id values from target");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading existing supplier_technical_parameter_id values from target");
            }

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;
            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<SupplierTechnicalParameterRecord>();
            var processedIds = new HashSet<int>();  // Changed from long to int

            while (await reader.ReadAsync())
            {
                totalRecords++;

                // Dynamic type checking for SQL Server (can be BIGINT or INT)
                var vendorTechItemTermId = reader.IsDBNull(0) ? (int?)null : 
                    reader.GetFieldType(0) == typeof(long) ? Convert.ToInt32(reader.GetInt64(0)) : reader.GetInt32(0);
                var techItemTermId = reader.IsDBNull(1) ? (int?)null : 
                    reader.GetFieldType(1) == typeof(long) ? Convert.ToInt32(reader.GetInt64(1)) : reader.GetInt32(1);
                var value = reader.IsDBNull(2) ? null : reader.GetString(2);
                var vendorId = reader.IsDBNull(3) ? (int?)null : 
                    reader.GetFieldType(3) == typeof(long) ? Convert.ToInt32(reader.GetInt64(3)) : reader.GetInt32(3);
                var pbId = reader.IsDBNull(4) ? (int?)null : 
                    reader.GetFieldType(4) == typeof(long) ? Convert.ToInt32(reader.GetInt64(4)) : reader.GetInt32(4);
                var eventId = reader.IsDBNull(5) ? (int?)null : 
                    reader.GetFieldType(5) == typeof(long) ? Convert.ToInt32(reader.GetInt64(5)) : reader.GetInt32(5);

                // Skip if primary key is NULL
                if (!vendorTechItemTermId.HasValue)
                {
                    _migrationLogger.LogSkipped("VendorTechItemTermId is NULL", "NULL");
                    skippedRecords++;
                    continue;
                }

                int id = vendorTechItemTermId.Value;  // Changed from long to int

                // Skip if this ID already exists in the target
                if (existingTargetIds.Contains(id))
                {
                    _migrationLogger.LogSkipped("Duplicate in target: supplier_technical_parameter_id already exists", id.ToString());
                    skippedRecords++;
                    continue;
                }

                // Skip if user_technical_parameter_id is NULL
                if (!techItemTermId.HasValue)
                {
                    _migrationLogger.LogSkipped("user_technical_parameter_id is NULL", id.ToString());
                    skippedRecords++;
                    continue;
                }

                // Validate user_technical_parameter_id
                if (!validUserTechParamIds.Contains(techItemTermId.Value))
                {
                    _migrationLogger.LogSkipped($"Invalid user_technical_parameter_id: {techItemTermId.Value}", 
                        id.ToString(), 
                        new Dictionary<string, object> { { "user_technical_parameter_id", techItemTermId.Value } });
                    skippedRecords++;
                    continue;
                }

                // Skip if supplier_id is NULL
                if (!vendorId.HasValue)
                {
                    _migrationLogger.LogSkipped("supplier_id is NULL", id.ToString());
                    skippedRecords++;
                    continue;
                }

                // Validate supplier_id
                if (!validSupplierIds.Contains(vendorId.Value))
                {
                    _migrationLogger.LogSkipped($"Invalid supplier_id: {vendorId.Value}", 
                        id.ToString(), 
                        new Dictionary<string, object> { { "supplier_id", vendorId.Value } });
                    skippedRecords++;
                    continue;
                }

                // Skip if event_item_id or event_id is NULL (both are NOT NULL in PostgreSQL)
                if (!pbId.HasValue)
                {
                    _migrationLogger.LogSkipped("event_item_id is NULL", id.ToString());
                    skippedRecords++;
                    continue;
                }

                if (!eventId.HasValue)
                {
                    _migrationLogger.LogSkipped("event_id is NULL", id.ToString());
                    skippedRecords++;
                    continue;
                }

                var record = new SupplierTechnicalParameterRecord
                {
                    SupplierTechnicalParameterId = id,
                    UserTechnicalParameterId = techItemTermId.Value,
                    SupplierTechnicalParameterResponse = value,
                    SupplierId = vendorId.Value,
                    EventItemId = pbId.Value,    // Now non-nullable
                    EventId = eventId.Value       // Now non-nullable
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

            // Export migration statistics using MigrationLogger
            if (_migrationLogger != null)
            {
                var skippedLogEntries = _migrationLogger.GetSkippedRecords();
                var skippedRecordsList = skippedLogEntries.Select(e => (e.RecordIdentifier, e.Message)).ToList();
                var excelPath = Path.Combine("migration_outputs", $"SupplierTechnicalParameter_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
                MigrationStatsExporter.ExportToExcel(
                    excelPath,
                    totalRecords,
                    migratedRecords,
                    skippedRecords,
                    _logger,
                    skippedRecordsList
                );
                _migrationLogger.LogInfo($"Migration stats exported to {excelPath}");
            }

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Technical Parameter migration");
            throw;
        }
    }

    private async Task<HashSet<int>> LoadValidUserTechnicalParameterIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();  // Changed from long to int

        try
        {
            var query = "SELECT user_technical_parameter_id FROM user_technical_parameter WHERE user_technical_parameter_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));  // Changed from GetInt64 to GetInt32
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
            // First, check the actual column types from PostgreSQL
            var schemaQuery = @"SELECT column_name, data_type, udt_name 
                               FROM information_schema.columns 
                               WHERE table_name = 'supplier_technical_parameter' 
                               ORDER BY ordinal_position";
            
            using (var schemaCmd = new NpgsqlCommand(schemaQuery, pgConn))
            using (var schemaReader = await schemaCmd.ExecuteReaderAsync())
            {
                _logger.LogDebug("=== supplier_technical_parameter table schema ===");
                while (await schemaReader.ReadAsync())
                {
                    var colName = schemaReader.GetString(0);
                    var dataType = schemaReader.GetString(1);
                    var udtName = schemaReader.GetString(2);
                    _logger.LogDebug($"  {colName}: {dataType} ({udtName})");
                }
            }

            // Log first record for debugging
            if (batch.Count > 0)
            {
                var firstRecord = batch[0];
                _logger.LogDebug($"First record: ID={firstRecord.SupplierTechnicalParameterId}, UserTechParamId={firstRecord.UserTechnicalParameterId}, SupplierId={firstRecord.SupplierId}");
            }

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
                    try
                    {
                        await writer.StartRowAsync();
                        
                        // supplier_technical_parameter_id (INTEGER, NOT NULL) - FIXED: Was using Bigint
                        await writer.WriteAsync(record.SupplierTechnicalParameterId, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // user_technical_parameter_id (INTEGER, NOT NULL) - FIXED: Was using Bigint
                        await writer.WriteAsync(record.UserTechnicalParameterId, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // supplier_technical_parameter_response (TEXT, nullable)
                        if (record.SupplierTechnicalParameterResponse != null)
                            await writer.WriteAsync(record.SupplierTechnicalParameterResponse, NpgsqlTypes.NpgsqlDbType.Text);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text);
                        
                        // supplier_id (INTEGER, NOT NULL)
                        await writer.WriteAsync(record.SupplierId, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // event_item_id (INTEGER, NOT NULL) - FIXED: Was nullable
                        await writer.WriteAsync(record.EventItemId, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // event_id (INTEGER, NOT NULL) - FIXED: Was nullable
                        await writer.WriteAsync(record.EventId, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // created_by (INTEGER, nullable)
                        if (record.CreatedBy.HasValue)
                            await writer.WriteAsync(record.CreatedBy.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // created_date (TIMESTAMP, nullable)
                        if (record.CreatedDate.HasValue)
                            await writer.WriteAsync(record.CreatedDate.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        
                        // modified_by (INTEGER, nullable)
                        if (record.ModifiedBy.HasValue)
                            await writer.WriteAsync(record.ModifiedBy.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // modified_date (TIMESTAMP, nullable)
                        if (record.ModifiedDate.HasValue)
                            await writer.WriteAsync(record.ModifiedDate.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        
                        // is_deleted (BOOLEAN, NOT NULL)
                        await writer.WriteAsync(record.IsDeleted, NpgsqlTypes.NpgsqlDbType.Boolean);
                        
                        // deleted_by (INTEGER, nullable)
                        if (record.DeletedBy.HasValue)
                            await writer.WriteAsync(record.DeletedBy.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                        
                        // deleted_date (TIMESTAMP, nullable)
                        if (record.DeletedDate.HasValue)
                            await writer.WriteAsync(record.DeletedDate.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        else
                            await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                        
                        insertedCount++;
                    }
                    catch (Exception recordEx)
                    {
                        _logger.LogError(recordEx, $"Error writing record ID {record.SupplierTechnicalParameterId} to COPY stream");
                        throw;
                    }
                }
                await writer.CompleteAsync();
            }
            _logger.LogDebug($"Inserted {insertedCount} records using COPY");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records with COPY. First ID: {(batch.Count > 0 ? batch[0].SupplierTechnicalParameterId : 0)}");
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
                cmd.Parameters.AddWithValue("@event_item_id", record.EventItemId);  // Now non-nullable
                cmd.Parameters.AddWithValue("@event_id", record.EventId);            // Now non-nullable
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
                _migrationLogger?.LogSkipped($"Insert failed: {ex.Message}", 
                    record.SupplierTechnicalParameterId.ToString(), 
                    new Dictionary<string, object> { { "Exception", ex.GetType().Name } });
            }
        }

        return insertedCount;
    }
}
