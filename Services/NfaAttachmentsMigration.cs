using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class NfaAttachmentsMigration
    {
        private readonly ILogger<NfaAttachmentsMigration> _logger;
    private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public NfaAttachmentsMigration(IConfiguration configuration, ILogger<NfaAttachmentsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

    public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "QCSPOMailAttachmentFileId (Auto-increment)", target = "nfa_attachments_id", type = "int -> integer (NOT NULL, Auto-increment)" },
                new { source = "AWARDEVENTMAINID (from both tables)", target = "nfa_header_id", type = "int -> integer (NOT NULL)" },
                new { source = "AttachmentFile / FILENAME", target = "nfa_attachments_name", type = "nvarchar -> text (NULLABLE)" },
                new { source = "TBL_QCSPOMailAttachmentFile / UPLOADPATH", target = "nfa_attachments_path", type = "nvarchar -> text (NULLABLE)" },
                new { source = "Static: 'NFA_Doc'", target = "nfa_attachments", type = "text (NULLABLE)" },
                new { source = "Static: false", target = "is_push_to_erp", type = "boolean (NOT NULL)" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            _migrationLogger = new MigrationLogger(_logger, "nfa_attachments");
            _migrationLogger.LogInfo("Starting migration");

            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }
            var sourceData = new List<SourceRow>();

            var migratedRecords = 0;
            var skippedRecordsCount = 0;
            var skippedRecords = new List<(string RecordId, string Reason)>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting NfaAttachments migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE nfa_attachments RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset nfa_attachments table and restarted identity sequence");
                }

                // Build lookup for valid nfa_header_id from PostgreSQL
                var validNfaHeaderIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT nfa_header_id 
                    FROM nfa_header 
                    WHERE nfa_header_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validNfaHeaderIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built nfa_header_id lookup with {validNfaHeaderIds.Count} entries");

                // Fetch data from TBL_QCSPOMailAttachmentFile
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        QCSPOMailAttachmentFileId,
                        AWARDEVENTMAINID,
                        AttachmentFile,
                        UPLOADPATH
                    FROM TBL_QCSPOMailAttachmentFile
                    WHERE QCSPOMailAttachmentFileId IS NOT NULL
                    ORDER BY QCSPOMailAttachmentFileId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            Id = reader.GetInt32(0),
                            NfaHeaderId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            FileName = reader.IsDBNull(2) ? null : reader.GetString(2),
                            FilePath = reader.IsDBNull(3) ? null : reader.GetString(3),
                            SourceTable = "TBL_QCSPOMailAttachmentFile"
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_QCSPOMailAttachmentFile");

                // Fetch data from TBL_STANDALONENFAATTACHMENT
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        ROW_NUMBER() OVER (ORDER BY AWARDEVENTMAINID) as Id,
                        AWARDEVENTMAINID,
                        FILENAME,
                        UPLOADPATH
                    FROM TBL_STANDALONENFAATTACHMENT
                    WHERE AWARDEVENTMAINID IS NOT NULL
                    ORDER BY AWARDEVENTMAINID", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    int maxId = sourceData.Any() ? sourceData.Max(x => x.Id) : 0;
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            Id = maxId + Convert.ToInt32(reader.GetInt64(0)),
                            NfaHeaderId = reader.GetInt32(1),
                            FileName = reader.IsDBNull(2) ? null : reader.GetString(2),
                            FilePath = reader.IsDBNull(3) ? null : reader.GetString(3),
                            SourceTable = "TBL_STANDALONENFAATTACHMENT"
                        });
                    }
                }

                _logger.LogInformation($"Total records after fetching from TBL_STANDALONENFAATTACHMENT: {sourceData.Count}");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate nfa_header_id (REQUIRED - NOT NULL)
                        if (!record.NfaHeaderId.HasValue)
                        {
                            _logger.LogWarning($"Skipping record Id={record.Id} from {record.SourceTable}: AWARDEVENTMAINID is null");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.Id.ToString(), "AWARDEVENTMAINID is null"));
                            continue;
                        }

                        // Validate nfa_header_id exists (FK constraint)
                        if (!validNfaHeaderIds.Contains(record.NfaHeaderId.Value))
                        {
                            _logger.LogWarning($"Skipping record Id={record.Id} from {record.SourceTable}: nfa_header_id={record.NfaHeaderId} not found in nfa_header");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.Id.ToString(), $"nfa_header_id={record.NfaHeaderId} not found in nfa_header"));
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            NfaAttachmentsId = record.Id,
                            NfaHeaderId = record.NfaHeaderId.Value,
                            NfaAttachmentsName = record.FileName,
                            NfaAttachmentsPath = record.FilePath,
                            NfaAttachments = "NFA_Doc",
                            IsPushToErp = false
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Error processing record Id={record.Id} from {record.SourceTable}: {ex.Message}");
                        skippedRecordsCount++;
                        skippedRecords.Add((record.Id.ToString(), $"Error: {ex.Message}"));
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecordsCount}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            // Export migration stats and skipped records to Excel
            MigrationStatsExporter.ExportToExcel(
                "NfaAttachmentsMigration_Stats.xlsx",
                sourceData.Count,
                migratedRecords,
                skippedRecordsCount,
                _logger,
                skippedRecords
            );

            return migratedRecords;
        }

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO nfa_attachments (");
            sql.AppendLine("    nfa_attachments_id, nfa_header_id, nfa_attachments_name, nfa_attachments_path,");
            sql.AppendLine("    nfa_attachments, is_push_to_erp,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@NfaAttachmentsId{i}, @NfaHeaderId{i}, @NfaAttachmentsName{i}, @NfaAttachmentsPath{i}, @NfaAttachments{i}, @IsPushToErp{i}, NULL, NULL, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@NfaAttachmentsId{i}", row.NfaAttachmentsId);
                cmd.Parameters.AddWithValue($"@NfaHeaderId{i}", row.NfaHeaderId);
                cmd.Parameters.AddWithValue($"@NfaAttachmentsName{i}", row.NfaAttachmentsName ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue($"@NfaAttachmentsPath{i}", row.NfaAttachmentsPath ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue($"@NfaAttachments{i}", row.NfaAttachments ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue($"@IsPushToErp{i}", row.IsPushToErp);
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
            public int Id { get; set; }
            public int? NfaHeaderId { get; set; }
            public string? FileName { get; set; }
            public string? FilePath { get; set; }
            public string SourceTable { get; set; } = string.Empty;
        }

        private class TargetRow
        {
            public int NfaAttachmentsId { get; set; }
            public int NfaHeaderId { get; set; }
            public string? NfaAttachmentsName { get; set; }
            public string? NfaAttachmentsPath { get; set; }
            public string? NfaAttachments { get; set; }
            public bool IsPushToErp { get; set; }
        }
    }
}
