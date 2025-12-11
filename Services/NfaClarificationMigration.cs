using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class NfaClarificationMigration
    {
        private readonly ILogger<NfaClarificationMigration> _logger;
    private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public NfaClarificationMigration(IConfiguration configuration, ILogger<NfaClarificationMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

    public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "NFAChatId (Auto-increment)", target = "nfa_clarification_id", type = "int -> integer (NOT NULL, Auto-increment)" },
                new { source = "NFAId", target = "nfa_header_id", type = "int -> integer (NOT NULL)" },
                new { source = "ClarificationRemarks", target = "clarification_remarks", type = "nvarchar -> text (NOT NULL)" },
                new { source = "IsSeen", target = "is_seen", type = "int -> boolean (NOT NULL)" },
                new { source = "ToUserID", target = "to_user_id", type = "int -> ARRAY (NOT NULL)" },
                new { source = "MassageSentToAllUser", target = "message_sent_to_all_user", type = "int -> boolean (NOT NULL)" },
                new { source = "ACTIONBY", target = "created_by", type = "int -> integer" },
                new { source = "ACTIONDATE", target = "created_date", type = "datetime -> timestamp with time zone" }
            };
        }

        public async Task<int> MigrateAsync()
        {
        _migrationLogger = new MigrationLogger(_logger, "nfa_clarification");
        _migrationLogger.LogInfo("Starting migration");

            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecordsCount = 0;
            var skippedRecords = new List<(string RecordId, string Reason)>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting NfaClarification migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE nfa_clarification RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset nfa_clarification table and restarted identity sequence");
                }

                // Build lookup for valid user_ids from PostgreSQL
                var validUserIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT user_id 
                    FROM users 
                    WHERE user_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validUserIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built user_id lookup with {validUserIds.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        NFAChatId,
                        NFAId,
                        ClarificationRemarks,
                        USERTYPE,
                        ACTIONBY,
                        ACTIONDATE,
                        IsSeen,
                        ToUserID,
                        MassageSentToAllUser
                    FROM TBL_NFA_Clarification
                    WHERE NFAChatId IS NOT NULL
                    ORDER BY NFAChatId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            NFAChatId = reader.GetInt32(0),
                            NFAId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            ClarificationRemarks = reader.IsDBNull(2) ? null : reader.GetString(2),
                            UserType = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ActionBy = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            ActionDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            IsSeen = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                            ToUserID = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            MassageSentToAllUser = reader.IsDBNull(8) ? null : reader.GetInt32(8)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_NFA_Clarification");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate nfa_header_id (REQUIRED - NOT NULL)
                        if (!record.NFAId.HasValue)
                        {
                            _logger.LogWarning($"Skipping NFAChatId {record.NFAChatId}: NFAId is null");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.NFAChatId.ToString(), "NFAId is null"));
                            continue;
                        }

                        // Validate clarification_remarks (REQUIRED - NOT NULL)
                        if (string.IsNullOrWhiteSpace(record.ClarificationRemarks))
                        {
                            _logger.LogWarning($"Skipping NFAChatId {record.NFAChatId}: ClarificationRemarks is null/empty");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.NFAChatId.ToString(), "ClarificationRemarks is null/empty"));
                            continue;
                        }

                        // Convert IsSeen (int) to boolean
                        // Assuming: 0 = false, non-zero = true
                        bool isSeen = record.IsSeen.HasValue && record.IsSeen.Value != 0;

                        // Convert MassageSentToAllUser (int) to boolean
                        // Assuming: 0 = false, non-zero = true
                        bool messageSentToAllUser = record.MassageSentToAllUser.HasValue && record.MassageSentToAllUser.Value != 0;

                        // Build to_user_id array
                        // ToUserID is a single int in source, convert to array
                        var toUserIdArray = new List<int>();
                        if (record.ToUserID.HasValue)
                        {
                            if (validUserIds.Contains(record.ToUserID.Value))
                            {
                                toUserIdArray.Add(record.ToUserID.Value);
                            }
                            else
                            {
                                _logger.LogWarning($"NFAChatId {record.NFAChatId}: ToUserID={record.ToUserID} not found in users, will use empty array");
                            }
                        }

                        // If array is empty and message not sent to all, skip the record
                        if (!toUserIdArray.Any() && !messageSentToAllUser)
                        {
                            _logger.LogWarning($"Skipping NFAChatId {record.NFAChatId}: to_user_id array is empty and message not sent to all");
                            skippedRecordsCount++;
                            skippedRecords.Add((record.NFAChatId.ToString(), "to_user_id array is empty and message not sent to all"));
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            NFAClarificationId = record.NFAChatId,
                            NFAHeaderId = record.NFAId.Value,
                            ClarificationRemarks = record.ClarificationRemarks,
                            IsSeen = isSeen,
                            ToUserId = toUserIdArray.ToArray(),
                            MessageSentToAllUser = messageSentToAllUser,
                            CreatedBy = record.ActionBy,
                            CreatedDate = record.ActionDate
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
                        _logger.LogError($"Error processing NFAChatId {record.NFAChatId}: {ex.Message}");
                        skippedRecordsCount++;
                        skippedRecords.Add((record.NFAChatId.ToString(), $"Error: {ex.Message}"));
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecordsCount}");

                // Export migration stats and skipped records to Excel
                MigrationStatsExporter.ExportToExcel(
                    "NfaClarificationMigration_Stats.xlsx",
                    sourceData.Count,
                    migratedRecords,
                    skippedRecordsCount,
                    _logger,
                    skippedRecords
                );
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
            sql.AppendLine("INSERT INTO nfa_clarification (");
            sql.AppendLine("    nfa_clarification_id, nfa_header_id, clarification_remarks, is_seen,");
            sql.AppendLine("    to_user_id, message_sent_to_all_user,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@NFAClarificationId{i}, @NFAHeaderId{i}, @ClarificationRemarks{i}, @IsSeen{i}, @ToUserId{i}, @MessageSentToAllUser{i}, @CreatedBy{i}, @CreatedDate{i}, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@NFAClarificationId{i}", row.NFAClarificationId);
                cmd.Parameters.AddWithValue($"@NFAHeaderId{i}", row.NFAHeaderId);
                cmd.Parameters.AddWithValue($"@ClarificationRemarks{i}", row.ClarificationRemarks);
                cmd.Parameters.AddWithValue($"@IsSeen{i}", row.IsSeen);
                cmd.Parameters.AddWithValue($"@ToUserId{i}", row.ToUserId);
                cmd.Parameters.AddWithValue($"@MessageSentToAllUser{i}", row.MessageSentToAllUser);
                cmd.Parameters.AddWithValue($"@CreatedBy{i}", row.CreatedBy.HasValue ? (object)row.CreatedBy.Value : DBNull.Value);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", row.CreatedDate.HasValue ? (object)row.CreatedDate.Value : DBNull.Value);
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
            public int NFAChatId { get; set; }
            public int? NFAId { get; set; }
            public string? ClarificationRemarks { get; set; }
            public string? UserType { get; set; }
            public int? ActionBy { get; set; }
            public DateTime? ActionDate { get; set; }
            public int? IsSeen { get; set; }
            public int? ToUserID { get; set; }
            public int? MassageSentToAllUser { get; set; }
        }

        private class TargetRow
        {
            public int NFAClarificationId { get; set; }
            public int NFAHeaderId { get; set; }
            public string ClarificationRemarks { get; set; } = string.Empty;
            public bool IsSeen { get; set; }
            public int[] ToUserId { get; set; } = Array.Empty<int>();
            public bool MessageSentToAllUser { get; set; }
            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }
        }
    }
}
