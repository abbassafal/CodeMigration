using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.Diagnostics;
using System.Threading;
using DataMigration.Services;
using Microsoft.Extensions.Logging;

public class MaterialMasterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000; // Process in batches of 1000 records
    private const int PROGRESS_UPDATE_INTERVAL = 100; // Update progress every 100 records
    
    private MigrationLogger? _migrationLogger;
    private readonly ILogger<MaterialMasterMigration> _logger;

    protected override string SelectQuery => @"
        SELECT 
            ITEMID, 
            ISNULL(ITEMCODE, '') AS ITEMCODE, 
            ISNULL(ITEMNAME, '') AS ITEMNAME, 
            ITEMDESCRIPTION, 
            ISNULL(UOMId, 0) AS UOMId, 
            ISNULL(MaterialGroupId, 0) AS MaterialGroupId, 
            ISNULL(ClientSAPId, 0) AS ClientSAPId 
        FROM TBL_ITEMMASTER 
        WITH (NOLOCK)
        WHERE ITEMID IS NOT NULL
        ORDER BY ITEMID";
    protected override string InsertQuery => @"INSERT INTO material_master (material_id, material_code, material_name, material_description, uom_id, material_group_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@material_id, @material_code, @material_name, @material_description, @uom_id, @material_group_id, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    // Optimized batch insert query
    private readonly string BatchInsertQuery = @"
        INSERT INTO material_master (material_id, material_code, material_name, material_description, uom_id, material_group_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
        VALUES ";

    public MaterialMasterMigration(IConfiguration configuration, ILogger<MaterialMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "ITEMID -> material_id (Direct)",
            "ITEMCODE -> material_code (Direct)",
            "ITEMNAME -> material_name (Direct)",
            "ITEMDESCRIPTION -> material_description (Direct)",
            "UOMId -> uom_id (FK to uom_master) - Uses default 'EACH' if missing",
            "MaterialGroupId -> material_group_id (FK to material_group_master)",
            "ClientSAPId -> company_id (FK to company)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ITEMID", logic = "ITEMID -> material_id (Direct)", target = "material_id" },
            new { source = "ITEMCODE", logic = "ITEMCODE -> material_code (Direct)", target = "material_code" },
            new { source = "ITEMNAME", logic = "ITEMNAME -> material_name (Direct)", target = "material_name" },
            new { source = "ITEMDESCRIPTION", logic = "ITEMDESCRIPTION -> material_description (Direct)", target = "material_description" },
            new { source = "UOMId", logic = "UOMId -> uom_id (FK to uom_master) - Uses default if missing", target = "uom_id" },
            new { source = "MaterialGroupId", logic = "MaterialGroupId -> material_group_id (FK to material_group_master)", target = "material_group_id" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (FK to company)", target = "company_id" },
            new { source = "-", logic = "created_by -> 0 (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await MigrateAsync(useTransaction: true);
    }

    public async Task<int> MigrateAsync(IMigrationProgress? progress = null)
    {
        return await MigrateAsync(useTransaction: true, progress);
    }

    public async Task<int> MigrateAsync(bool useTransaction, IMigrationProgress? progress = null)
    {
        progress ??= new ConsoleMigrationProgress();
        
        SqlConnection? sqlConn = null;
        NpgsqlConnection? pgConn = null;
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            progress.ReportProgress(0, 0, "Initializing database connections...", stopwatch.Elapsed);
            
            sqlConn = GetSqlServerConnection();
            pgConn = GetPostgreSqlConnection();
            
            // Configure SQL connection for large datasets with extended timeouts
            var sqlConnString = sqlConn.ConnectionString;
            if (!sqlConnString.Contains("Connection Timeout"))
            {
                sqlConnString += ";Connection Timeout=600"; // 10 minutes
            }
            if (!sqlConnString.Contains("Command Timeout"))
            {
                sqlConnString += ";Command Timeout=600"; // 10 minutes
            }
            sqlConn.ConnectionString = sqlConnString;
            
            progress.ReportProgress(0, 0, "Opening SQL Server connection...", stopwatch.Elapsed);
            await sqlConn.OpenAsync();
            progress.ReportProgress(0, 0, "SQL Server connection opened successfully", stopwatch.Elapsed);
            
            progress.ReportProgress(0, 0, "Opening PostgreSQL connection...", stopwatch.Elapsed);
            await pgConn.OpenAsync();
            progress.ReportProgress(0, 0, "PostgreSQL connection opened successfully", stopwatch.Elapsed);

            progress.ReportProgress(0, 0, "Estimating total records...", stopwatch.Elapsed);
            
            // Get total count first for progress reporting
            int totalRecords = await GetTotalRecordsAsync(sqlConn);
            
            if (useTransaction)
            {
                using var transaction = await pgConn.BeginTransactionAsync();
                try
                {
                    int result = await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
                    await transaction.CommitAsync();
                    return result;
                }
                catch (Exception)
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            else
            {
                return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch);
            }
        }
        finally
        {
            sqlConn?.Dispose();
            pgConn?.Dispose();
        }
    }

    private async Task<int> GetTotalRecordsAsync(SqlConnection sqlConn)
    {
        try
        {
            using var cmd = new SqlCommand("SELECT COUNT(*) FROM TBL_ITEMMASTER", sqlConn);
            cmd.CommandTimeout = 600; // 10 minutes
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
        catch (Exception ex)
        {
            throw new Exception($"Error getting total record count from TBL_ITEMMASTER: {ex.Message}", ex);
        }
    }

    private async Task<int> GetOrCreateDefaultUomAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        try
        {
            // First, try to find an existing "EACH" or "EA" UOM
            var checkQuery = @"SELECT uom_id FROM uom_master WHERE LOWER(uom_code) IN ('each', 'ea', 'nos', 'pcs') ORDER BY uom_id LIMIT 1";
            using var checkCmd = new NpgsqlCommand(checkQuery, pgConn);
            if (transaction != null) checkCmd.Transaction = transaction;
            
            var existingUomId = await checkCmd.ExecuteScalarAsync();
            if (existingUomId != null)
            {
                return Convert.ToInt32(existingUomId);
            }

            // If no default UOM exists, create one
            var insertQuery = @"
                INSERT INTO uom_master (uom_code, uom_name, created_by, created_date, is_deleted) 
                VALUES (@uom_code, @uom_name, @created_by, @created_date, @is_deleted) 
                RETURNING uom_id";
                
            using var insertCmd = new NpgsqlCommand(insertQuery, pgConn);
            if (transaction != null) insertCmd.Transaction = transaction;
            
            insertCmd.Parameters.AddWithValue("@uom_code", "EACH");
            insertCmd.Parameters.AddWithValue("@uom_name", "Each (Default for migration)");
            insertCmd.Parameters.AddWithValue("@created_by", 0);
            insertCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            insertCmd.Parameters.AddWithValue("@is_deleted", false);
            
            var newUomId = await insertCmd.ExecuteScalarAsync();
            return Convert.ToInt32(newUomId);
        }
        catch (Exception ex)
        {
            throw new Exception($"Error getting or creating default UOM: {ex.Message}", ex);
        }
    }

    private async Task<HashSet<int>> LoadValidUomIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        var validIds = new HashSet<int>();
        try
        {
            var query = "SELECT uom_id FROM uom_master WHERE is_deleted = false OR is_deleted IS NULL";
            using var cmd = new NpgsqlCommand(query, pgConn);
            if (transaction != null) cmd.Transaction = transaction;
            
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }
            
            Console.WriteLine($"✓ Loaded {validIds.Count:N0} valid UOM IDs from uom_master");
        }
        catch (Exception ex)
        {
            throw new Exception($"Error loading valid UOM IDs: {ex.Message}", ex);
        }
        return validIds;
    }

    private async Task<int> ExecuteOptimizedMigrationAsync(
        SqlConnection sqlConn, 
        NpgsqlConnection pgConn, 
        int totalRecords, 
        IMigrationProgress progress, 
        Stopwatch stopwatch, 
        NpgsqlTransaction? transaction = null)
    {
        var insertedCount = 0;
        var processedCount = 0;
        var skippedCount = 0;
        var batch = new List<MaterialRecord>();
        var skippedRecords = new List<(string RecordId, string Reason)>();

        progress.ReportProgress(0, totalRecords, "Starting migration...", stopwatch.Elapsed);

        // Load valid UOM IDs from uom_master
        var validUomIds = await LoadValidUomIdsAsync(pgConn, transaction);
        progress.ReportProgress(0, totalRecords, $"Loaded {validUomIds.Count:N0} valid UOM IDs", stopwatch.Elapsed);

        // Get or create default UOM for missing/invalid UOM cases
        var defaultUomId = await GetOrCreateDefaultUomAsync(pgConn, transaction);
        progress.ReportProgress(0, totalRecords, $"Default UOM ID: {defaultUomId}", stopwatch.Elapsed);

        // Use streaming reader for memory efficiency
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        sqlCmd.CommandTimeout = 600; // 10 minutes timeout
        
        progress.ReportProgress(0, totalRecords, "Executing SQL query to read data...", stopwatch.Elapsed);
        using var reader = await sqlCmd.ExecuteReaderAsync();
        progress.ReportProgress(0, totalRecords, "SQL query executed, reading data stream...", stopwatch.Elapsed);

        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                MaterialRecord? record = null;
                try
                {
                    record = ReadMaterialRecord(reader, processedCount, defaultUomId, validUomIds);
                }
                catch (Exception ex)
                {
                    skippedCount++;
                    var itemId = reader["ITEMID"]?.ToString() ?? "";
                    skippedRecords.Add((itemId, $"Error reading record {processedCount}: {ex.Message}"));
                    continue;
                }
                if (record == null)
                {
                    skippedCount++;
                    var itemId = reader["ITEMID"]?.ToString() ?? "";
                    skippedRecords.Add((itemId, $"Skipped record {processedCount}: missing MaterialGroupId or ITEMCODE"));
                    continue;
                }
                batch.Add(record);
                // Process batch when it reaches the batch size or it's the last record
                if (batch.Count >= BATCH_SIZE || processedCount == totalRecords)
                {
                    if (batch.Count > 0)
                    {
                        progress.ReportProgress(processedCount, totalRecords, 
                            $"Processing batch of {batch.Count} records (Total processed: {processedCount}, Inserted: {insertedCount})...", 
                            stopwatch.Elapsed);
                        int batchInserted = await InsertBatchAsync(pgConn, batch, transaction);
                        insertedCount += batchInserted;
                        progress.ReportProgress(processedCount, totalRecords, 
                            $"Batch completed: {batchInserted} records inserted", stopwatch.Elapsed);
                        batch.Clear();
                    }
                }

                // Update progress periodically
                if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
                {
                    var recordsPerSecond = processedCount / stopwatch.Elapsed.TotalSeconds;
                    var eta = TimeSpan.FromSeconds((totalRecords - processedCount) / recordsPerSecond);
                    progress.ReportProgress(processedCount, totalRecords, 
                        $"Processed: {processedCount:N0}/{totalRecords:N0} ({(processedCount * 100.0 / totalRecords):F1}%) | " +
                        $"Inserted: {insertedCount:N0} | Skipped: {skippedCount:N0} | " +
                        $"Speed: {recordsPerSecond:F0} rec/s | ETA: {eta:hh\\:mm\\:ss}", 
                        stopwatch.Elapsed);
                }
            }
        }
        catch (SqlException sqlEx) when (sqlEx.Number == -2) // Timeout
        {
            progress.ReportError($"SQL Server timeout after processing {processedCount} records. Connection may be unstable or query is too complex.", processedCount);
            throw new Exception($"SQL Server timeout during Material Master Migration after processing {processedCount} records. " +
                              $"Possible causes: 1) Network issues, 2) SQL Server overload, 3) Query complexity. " +
                              $"Original error: {sqlEx.Message}", sqlEx);
        }
        catch (Exception ex)
        {
            progress.ReportError($"Migration failed after processing {processedCount} records: {ex.Message}", processedCount);
            if (ex.Message.Contains("reading from stream") || ex.Message.Contains("connection") || ex.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during Material Master Migration after processing {processedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {ex.Message}", ex);
            }
            else if (ex.Message.Contains("constraint") || ex.Message.Contains("foreign key") || ex.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during Material Master Migration at record {processedCount}. " +
                                  $"This could be due to: 1) Missing reference data in UOM or MaterialGroup tables, " +
                                  $"2) Duplicate primary keys, 3) Invalid foreign key values. Original error: {ex.Message}", ex);
            }
            else
            {
                throw new Exception($"Unexpected error during Material Master Migration at record {processedCount}: {ex.Message}", ex);
            }
        }

        stopwatch.Stop();
        progress.ReportCompleted(processedCount, insertedCount, stopwatch.Elapsed);

        // Export migration stats and skipped records to Excel
        MigrationStatsExporter.ExportToExcel(
            "MaterialMasterMigration_Stats.xlsx",
            totalRecords,
            insertedCount,
            skippedCount,
            _logger,
            skippedRecords
        );

        return insertedCount;
    }

    private MaterialRecord? ReadMaterialRecord(SqlDataReader reader, int recordNumber, int defaultUomId, HashSet<int> validUomIds)
    {
        try
        {
            var itemId = reader.IsDBNull(reader.GetOrdinal("ITEMID")) ? 0 : Convert.ToInt32(reader["ITEMID"]);
            var itemCode = reader.IsDBNull(reader.GetOrdinal("ITEMCODE")) ? "" : reader["ITEMCODE"].ToString();
            var itemName = reader.IsDBNull(reader.GetOrdinal("ITEMNAME")) ? "" : reader["ITEMNAME"].ToString();
            var itemDescription = reader.IsDBNull(reader.GetOrdinal("ITEMDESCRIPTION")) ? "" : reader["ITEMDESCRIPTION"].ToString();
            var uomId = reader.IsDBNull(reader.GetOrdinal("UOMId")) ? 0 : Convert.ToInt32(reader["UOMId"]);
            var materialGroupId = reader.IsDBNull(reader.GetOrdinal("MaterialGroupId")) ? 0 : Convert.ToInt32(reader["MaterialGroupId"]);
            var clientSapId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(reader["ClientSAPId"]);

            // Validate and handle UOM ID
            if (uomId == 0 || !validUomIds.Contains(uomId))
            {
                var originalUomId = uomId;
                uomId = defaultUomId;
                Console.WriteLine($"Record {recordNumber} (ITEMID: {itemId}) - Invalid/Missing UOMId ({originalUomId}), using default UOM (ID: {defaultUomId})");
            }

            // Skip if MaterialGroup is missing (this is more critical)
            if (materialGroupId == 0)
            {
                Console.WriteLine($"Skipping record {recordNumber} (ITEMID: {itemId}) - Missing required MaterialGroupId");
                return null;
            }

            // Validate required fields
            if (string.IsNullOrWhiteSpace(itemCode))
            {
                throw new Exception($"Invalid ITEMCODE at record {recordNumber} (ITEMID: {itemId}) - Cannot be null or empty");
            }

            return new MaterialRecord
            {
                MaterialId = itemId,
                MaterialCode = itemCode ?? "",
                MaterialName = itemName ?? "",
                MaterialDescription = itemDescription ?? "",
                UomId = uomId,
                MaterialGroupId = materialGroupId,
                CompanyId = clientSapId
            };
        }
        catch (Exception ex)
        {
            throw new Exception($"Error reading material record {recordNumber}: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<MaterialRecord> batch, NpgsqlTransaction? transaction = null)
    {
        if (batch.Count == 0) return 0;

        try
        {
            // Use COPY for maximum performance with large batches
            if (batch.Count >= 100) // Use COPY for larger batches
            {
                return await InsertBatchWithCopyAsync(pgConn, batch, transaction);
            }
            else
            {
                return await InsertBatchWithParametersAsync(pgConn, batch, transaction);
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Error inserting batch of {batch.Count} records: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchWithCopyAsync(NpgsqlConnection pgConn, List<MaterialRecord> batch, NpgsqlTransaction? transaction = null)
    {
        var copyCommand = $"COPY material_master (material_id, material_code, material_name, material_description, uom_id, material_group_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) FROM STDIN (FORMAT BINARY)";
        
        using var writer = transaction != null ? 
            await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None) : 
            await pgConn.BeginBinaryImportAsync(copyCommand);
        
        var now = DateTime.UtcNow;
        foreach (var record in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(record.MaterialId);
            await writer.WriteAsync(record.MaterialCode);
            await writer.WriteAsync(record.MaterialName);
            await writer.WriteAsync(record.MaterialDescription);
            await writer.WriteAsync(record.UomId);
            await writer.WriteAsync(record.MaterialGroupId);
            await writer.WriteAsync(record.CompanyId);
            await writer.WriteAsync(0); // created_by
            await writer.WriteAsync(now); // created_date
            await writer.WriteAsync(DBNull.Value); // modified_by
            await writer.WriteAsync(DBNull.Value); // modified_date
            await writer.WriteAsync(false); // is_deleted
            await writer.WriteAsync(DBNull.Value); // deleted_by
            await writer.WriteAsync(DBNull.Value); // deleted_date
        }
        
        await writer.CompleteAsync();
        return batch.Count;
    }

    private async Task<int> InsertBatchWithParametersAsync(NpgsqlConnection pgConn, List<MaterialRecord> batch, NpgsqlTransaction? transaction = null)
    {
        // Build multi-row insert statement
        var values = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        var now = DateTime.UtcNow;

        for (int i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            var paramPrefix = $"p{i}";
            
            values.Add($"(@{paramPrefix}_material_id, @{paramPrefix}_material_code, @{paramPrefix}_material_name, @{paramPrefix}_material_description, @{paramPrefix}_uom_id, @{paramPrefix}_material_group_id, @{paramPrefix}_company_id, @{paramPrefix}_created_by, @{paramPrefix}_created_date, @{paramPrefix}_modified_by, @{paramPrefix}_modified_date, @{paramPrefix}_is_deleted, @{paramPrefix}_deleted_by, @{paramPrefix}_deleted_date)");
            
            parameters.AddRange(new[]
            {
                new NpgsqlParameter($"@{paramPrefix}_material_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.MaterialId },
                new NpgsqlParameter($"@{paramPrefix}_material_code", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.MaterialCode },
                new NpgsqlParameter($"@{paramPrefix}_material_name", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.MaterialName },
                new NpgsqlParameter($"@{paramPrefix}_material_description", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.MaterialDescription },
                new NpgsqlParameter($"@{paramPrefix}_uom_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.UomId },
                new NpgsqlParameter($"@{paramPrefix}_material_group_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.MaterialGroupId },
                new NpgsqlParameter($"@{paramPrefix}_company_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.CompanyId },
                new NpgsqlParameter($"@{paramPrefix}_created_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = 0 },
                new NpgsqlParameter($"@{paramPrefix}_created_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = now },
                new NpgsqlParameter($"@{paramPrefix}_modified_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_modified_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_is_deleted", NpgsqlTypes.NpgsqlDbType.Boolean) { Value = false },
                new NpgsqlParameter($"@{paramPrefix}_deleted_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_deleted_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = DBNull.Value }
            });
        }

        var query = BatchInsertQuery + string.Join(", ", values);
        
        using var cmd = new NpgsqlCommand(query, pgConn);
        if (transaction != null)
        {
            cmd.Transaction = transaction;
        }
        
        cmd.Parameters.AddRange(parameters.ToArray());
        cmd.CommandTimeout = 300; // 5 minutes timeout
        
        return await cmd.ExecuteNonQueryAsync();
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    // Keep the original method for backward compatibility
    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // Use the injected logger instead of ConsoleLogger
        _migrationLogger = new MigrationLogger(_logger, "material_master");
        var progress = new ConsoleMigrationProgress();
        var stopwatch = Stopwatch.StartNew();
        int totalRecords = await GetTotalRecordsAsync(sqlConn);
        int insertedCount = 0;
        int skippedCount = 0;
        int processedCount = 0;
        var batch = new List<MaterialRecord>();
        var validUomIds = await LoadValidUomIdsAsync(pgConn, transaction);
        var defaultUomId = await GetOrCreateDefaultUomAsync(pgConn, transaction);
        var skippedRecords = new List<(string RecordId, string Reason)>();
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        sqlCmd.CommandTimeout = 600;
        using var reader = await sqlCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            processedCount++;
            MaterialRecord? record = null;
            try
            {
                record = ReadMaterialRecord(reader, processedCount, defaultUomId, validUomIds);
            }
            catch (Exception ex)
            {
                var itemId = reader["ITEMID"]?.ToString() ?? "";
                _migrationLogger.LogSkipped($"Error reading record {processedCount}: {ex.Message}", $"ITEMID={itemId}");
                skippedRecords.Add((itemId, $"Error reading record: {ex.Message}"));
                skippedCount++;
                continue;
            }
            if (record == null)
            {
                var itemId = reader["ITEMID"]?.ToString() ?? "";
                _migrationLogger.LogSkipped($"Skipped record {processedCount}: missing MaterialGroupId or ITEMCODE", $"ITEMID={itemId}");
                skippedRecords.Add((itemId, "Missing MaterialGroupId or ITEMCODE"));
                skippedCount++;
                continue;
            }
            batch.Add(record);
            if (batch.Count >= BATCH_SIZE || processedCount == totalRecords)
            {
                if (batch.Count > 0)
                {
                    int batchInserted = await InsertBatchAsync(pgConn, batch, transaction);
                    insertedCount += batchInserted;
                    foreach (var rec in batch)
                    {
                        _migrationLogger.LogInserted($"ITEMID={rec.MaterialId}");
                    }
                    batch.Clear();
                }
            }
            if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
            {
                _migrationLogger.LogInfo($"Processed: {processedCount}/{totalRecords} | Inserted: {insertedCount} | Skipped: {skippedCount}");
            }
        }
        stopwatch.Stop();
        _migrationLogger.LogInfo($"Migration completed. Total processed: {processedCount}, Inserted: {insertedCount}, Skipped: {skippedCount}");

        // Export migration stats and skipped records to Excel
        MigrationStatsExporter.ExportToExcel(
            "MaterialMasterMigration_Stats.xlsx",
            totalRecords,
            insertedCount,
            skippedCount,
            _logger,
            skippedRecords
        );
        return insertedCount;
    }

    private class MaterialRecord
    {
        public int MaterialId { get; set; }
        public string MaterialCode { get; set; } = "";
        public string MaterialName { get; set; } = "";
        public string MaterialDescription { get; set; } = "";
        public int UomId { get; set; }
        public int MaterialGroupId { get; set; }
        public int CompanyId { get; set; }
    }
}
