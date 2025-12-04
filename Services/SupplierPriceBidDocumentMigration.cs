using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SupplierPriceBidDocumentMigration
    {
        private readonly ILogger<SupplierPriceBidDocumentMigration> _logger;
        private readonly IConfiguration _configuration;

        public SupplierPriceBidDocumentMigration(IConfiguration configuration, ILogger<SupplierPriceBidDocumentMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "supplier_price_bid_document_id", type = "PostgreSQL auto-increment" },
                new { source = "EVENTID", target = "event_id", type = "numeric -> integer" },
                new { source = "SUPPLIERID", target = "supplier_id", type = "numeric -> integer" },
                new { source = "ATTACHMENTNAME", target = "file_name", type = "nvarchar -> varchar" },
                new { source = "SUPPLIER_ATTACHMENT", target = "file_path", type = "nvarchar -> text" },
                new { source = "Default: NULL", target = "created_by", type = "NULL" },
                new { source = "ENTDATE", target = "created_date", type = "datetime -> timestamp" },
                new { source = "Default: NULL", target = "modified_by", type = "NULL" },
                new { source = "Default: NULL", target = "modified_date", type = "NULL" },
                new { source = "Default: false", target = "is_deleted", type = "boolean" },
                new { source = "Default: NULL", target = "deleted_by", type = "NULL" },
                new { source = "Default: NULL", target = "deleted_date", type = "NULL" }
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

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SupplierPriceBidDocument migration...");

                // Truncate table and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE supplier_price_bid_document RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset supplier_price_bid_document table and restarted identity sequence");
                }

                // Build lookup for valid event_ids from PostgreSQL
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_id 
                    FROM event_master 
                    WHERE event_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_id lookup with {validEventIds.Count} entries");

                // Build lookup for valid supplier_ids from PostgreSQL
                var validSupplierIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT supplier_id 
                    FROM supplier_master 
                    WHERE supplier_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validSupplierIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built supplier_id lookup with {validSupplierIds.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        SUPATTACHEMNTID,
                        EVENTID,
                        SUPPLIERID,
                        ATTACHMENTNAME,
                        ENTDATE
                    FROM TBL_PB_SUPPLIER_ATTACHMENT
                    WHERE SUPATTACHEMNTID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            SUPATTACHEMNTID = reader.GetDecimal(0),
                            EVENTID = reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetDecimal(1)),
                            SUPPLIERID = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetDecimal(2)),
                            ATTACHMENTNAME = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ENTDATE = reader.IsDBNull(4) ? null : reader.GetDateTime(4)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_PB_SUPPLIER_ATTACHMENT");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id (REQUIRED - NOT NULL constraint)
                        if (!record.EVENTID.HasValue)
                        {
                            _logger.LogWarning($"Skipping SUPATTACHEMNTID {record.SUPATTACHEMNTID}: EVENTID is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEventIds.Contains(record.EVENTID.Value))
                        {
                            _logger.LogWarning($"Skipping SUPATTACHEMNTID {record.SUPATTACHEMNTID}: event_id={record.EVENTID} not found in event_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate supplier_id (REQUIRED - NOT NULL constraint)
                        if (!record.SUPPLIERID.HasValue)
                        {
                            _logger.LogWarning($"Skipping SUPATTACHEMNTID {record.SUPATTACHEMNTID}: SUPPLIERID is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validSupplierIds.Contains(record.SUPPLIERID.Value))
                        {
                            _logger.LogWarning($"Skipping SUPATTACHEMNTID {record.SUPATTACHEMNTID}: supplier_id={record.SUPPLIERID} not found in supplier_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate file_name (REQUIRED - NOT NULL constraint)
                        if (string.IsNullOrWhiteSpace(record.ATTACHMENTNAME))
                        {
                            _logger.LogWarning($"Skipping SUPATTACHEMNTID {record.SUPATTACHEMNTID}: ATTACHMENTNAME is null or empty");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            EventId = record.EVENTID.Value,
                            SupplierId = record.SUPPLIERID.Value,
                            FileName = record.ATTACHMENTNAME,
                            FilePath = "", // Default empty string for NOT NULL
                            CreatedBy = null,
                            CreatedDate = record.ENTDATE,
                            ModifiedBy = null,
                            ModifiedDate = null,
                            IsDeleted = false,
                            DeletedBy = null,
                            DeletedDate = null
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"SUPATTACHEMNTID {record.SUPATTACHEMNTID}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            return migratedRecords;
        }

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO supplier_price_bid_document (");
            sql.AppendLine("    event_id, supplier_id, file_name, file_path,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventId{i}, @SupplierId{i}, @FileName{i}, @FilePath{i}, @CreatedBy{i}, @CreatedDate{i}, @ModifiedBy{i}, @ModifiedDate{i}, @IsDeleted{i}, @DeletedBy{i}, @DeletedDate{i})");
                
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", row.SupplierId);
                cmd.Parameters.AddWithValue($"@FileName{i}", row.FileName);
                cmd.Parameters.AddWithValue($"@FilePath{i}", row.FilePath);
                cmd.Parameters.AddWithValue($"@CreatedBy{i}", (object?)row.CreatedBy ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", (object?)row.CreatedDate ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@ModifiedBy{i}", (object?)row.ModifiedBy ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@ModifiedDate{i}", (object?)row.ModifiedDate ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@IsDeleted{i}", row.IsDeleted);
                cmd.Parameters.AddWithValue($"@DeletedBy{i}", (object?)row.DeletedBy ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@DeletedDate{i}", (object?)row.DeletedDate ?? DBNull.Value);
            }

            sql.AppendLine(string.Join(",\n", values));
            cmd.CommandText = sql.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                _logger.LogDebug($"Batch inserted {rowsAffected} records");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Batch insert failed: {ex.Message}");
                throw;
            }
        }

        private class SourceRow
        {
            public decimal SUPATTACHEMNTID { get; set; }
            public int? EVENTID { get; set; }
            public int? SUPPLIERID { get; set; }
            public string? ATTACHMENTNAME { get; set; }
            public string? SUPPLIER_ATTACHMENT { get; set; }
            public DateTime? ENTDATE { get; set; }
        }

        private class TargetRow
        {
            public int EventId { get; set; }
            public int SupplierId { get; set; }
            public string FileName { get; set; } = "";
            public string FilePath { get; set; } = "";
            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }
            public int? ModifiedBy { get; set; }
            public DateTime? ModifiedDate { get; set; }
            public bool IsDeleted { get; set; }
            public int? DeletedBy { get; set; }
            public DateTime? DeletedDate { get; set; }
        }
    }
}
