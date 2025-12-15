using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;
using Microsoft.Extensions.Logging;
namespace DataMigration.Services
{
    public class EventCommunicationReceiverMigration
    {
        private readonly ILogger<EventCommunicationReceiverMigration> _logger;
        private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public EventCommunicationReceiverMigration(IConfiguration configuration, ILogger<EventCommunicationReceiverMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "MailMsgSubId", target = "ec_receiverid", type = "int -> integer" },
                new { source = "MailMsgMainId", target = "ec_senderid", type = "int -> integer (NOT NULL, FK)" },
                new { source = "ToUserId", target = "receiver_userid", type = "int -> integer (NOT NULL)" },
                new { source = "ToUserType", target = "receiver_user_type", type = "nvarchar -> text (NOT NULL)" },
                new { source = "ToMailId", target = "receiver_email_address", type = "nvarchar -> text (NOT NULL)" },
                new { source = "ReadStatus", target = "message_read_status", type = "int -> boolean (NOT NULL)" },
                new { source = "ReadDate", target = "message_read_date", type = "datetime -> timestamp (NULLABLE)" },
                new { source = "Default: true", target = "is_cc_user", type = "boolean (NOT NULL, default: false)" }
            };
        }

        public async Task<int> MigrateAsync()
        {
        _migrationLogger = new MigrationLogger(_logger, "event_communication_receiver");
        _migrationLogger.LogInfo("Starting migration");

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

                _logger.LogInformation("Starting EventCommunicationReceiver migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE event_communication_receiver RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset event_communication_receiver table and restarted identity sequence");
                }

                // Build lookup for valid ec_senderids from PostgreSQL
                var validEcSenderIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT ec_senderid 
                    FROM event_communication_sender 
                    WHERE ec_senderid IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        // Handle both INT and BIGINT from PostgreSQL
                        var fieldType = reader.GetFieldType(0);
                        var value = fieldType == typeof(long) ? (int)reader.GetInt64(0) : reader.GetInt32(0);
                        validEcSenderIds.Add(value);
                    }
                }
                _logger.LogInformation($"Built ec_senderid lookup with {validEcSenderIds.Count} entries");

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
                        // Handle both INT and BIGINT from PostgreSQL
                        var fieldType = reader.GetFieldType(0);
                        var value = fieldType == typeof(long) ? (int)reader.GetInt64(0) : reader.GetInt32(0);
                        validUserIds.Add(value);
                    }
                }
                _logger.LogInformation($"Built user_id lookup with {validUserIds.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        MailMsgSubId,
                        MailMsgMainId,
                        ToUserId,
                        ToUserType,
                        ToMailId,
                        ReadStatus,
                        ReadDate
                    FROM TBL_MAILMSGSUB
                    WHERE MailMsgSubId IS NOT NULL
                    ORDER BY MailMsgSubId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            // Handle both int and bigint from SQL Server
                            MailMsgSubId = reader.GetFieldType(0) == typeof(long) ? (int)reader.GetInt64(0) : reader.GetInt32(0),
                            MailMsgMainId = reader.IsDBNull(1) ? null : (reader.GetFieldType(1) == typeof(long) ? (int?)reader.GetInt64(1) : reader.GetInt32(1)),
                            ToUserId = reader.IsDBNull(2) ? null : (reader.GetFieldType(2) == typeof(long) ? (int?)reader.GetInt64(2) : reader.GetInt32(2)),
                            ToUserType = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ToMailId = reader.IsDBNull(4) ? null : reader.GetString(4),
                            ReadStatus = reader.IsDBNull(5) ? null : (reader.GetFieldType(5) == typeof(long) ? (int?)reader.GetInt64(5) : reader.GetInt32(5)),
                            ReadDate = reader.IsDBNull(6) ? null : reader.GetDateTime(6)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_MAILMSGSUB");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate ec_senderid (REQUIRED - NOT NULL, FK)
                        if (!record.MailMsgMainId.HasValue)
                        {
                            _migrationLogger.LogSkipped("MailMsgMainId is null", $"MailMsgSubId={record.MailMsgSubId}");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEcSenderIds.Contains(record.MailMsgMainId.Value))
                        {
                            _migrationLogger.LogSkipped($"ec_senderid={record.MailMsgMainId} not found in event_communication_sender", 
                                $"MailMsgSubId={record.MailMsgSubId}", 
                                new Dictionary<string, object> { { "ec_senderid", record.MailMsgMainId.Value } });
                            skippedRecords++;
                            continue;
                        }

                        // Validate receiver_userid (REQUIRED - NOT NULL)
                        if (!record.ToUserId.HasValue)
                        {
                            _migrationLogger.LogSkipped("ToUserId is null", $"MailMsgSubId={record.MailMsgSubId}");
                            skippedRecords++;
                            continue;
                        }

                        // Validate required text fields (NOT NULL constraints)
                        if (string.IsNullOrWhiteSpace(record.ToUserType))
                        {
                            _migrationLogger.LogSkipped("ToUserType is null/empty", $"MailMsgSubId={record.MailMsgSubId}");
                            skippedRecords++;
                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(record.ToMailId))
                        {
                            _migrationLogger.LogSkipped("ToMailId is null/empty", $"MailMsgSubId={record.MailMsgSubId}");
                            skippedRecords++;
                            continue;
                        }

                        // Convert ReadStatus (int) to boolean
                        // Assuming: 0 = false (unread), non-zero = true (read)
                        bool messageReadStatus = record.ReadStatus.HasValue && record.ReadStatus.Value != 0;

                        var targetRow = new TargetRow
                        {
                            EcReceiverId = record.MailMsgSubId,
                            EcSenderId = record.MailMsgMainId.Value,
                            ReceiverUserId = record.ToUserId.Value,
                            ReceiverUserType = record.ToUserType,
                            ReceiverEmailAddress = record.ToMailId,
                            MessageReadStatus = messageReadStatus,
                            MessageReadDate = record.ReadDate,
                            IsCcUser = false // Default value as per schema
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
                        _migrationLogger.LogSkipped($"Error processing record: {ex.Message}", 
                            $"MailMsgSubId={record.MailMsgSubId}", 
                            new Dictionary<string, object> { { "Exception", ex.GetType().Name } });
                        skippedRecords++;
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");

                // Export migration stats to Excel
                if (_migrationLogger != null)
                {
                    var skippedLogEntries = _migrationLogger.GetSkippedRecords();
                    var skippedRecordsList = skippedLogEntries.Select(e => (e.RecordIdentifier, e.Message)).ToList();
                    var excelPath = Path.Combine("migration_outputs", $"EventCommunicationReceiverMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
                    MigrationStatsExporter.ExportToExcel(
                        excelPath,
                        migratedRecords + skippedRecords,
                        migratedRecords,
                        skippedRecords,
                        _logger,
                        skippedRecordsList
                    );
                    _migrationLogger.LogInfo($"Migration stats exported to {excelPath}");
                }
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
            sql.AppendLine("INSERT INTO event_communication_receiver (");
            sql.AppendLine("    ec_receiverid, ec_senderid, receiver_userid, receiver_user_type,");
            sql.AppendLine("    receiver_email_address, message_read_status, message_read_date, is_cc_user,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EcReceiverId{i}, @EcSenderId{i}, @ReceiverUserId{i}, @ReceiverUserType{i}, @ReceiverEmailAddress{i}, @MessageReadStatus{i}, @MessageReadDate{i}, @IsCcUser{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EcReceiverId{i}", row.EcReceiverId);
                cmd.Parameters.AddWithValue($"@EcSenderId{i}", row.EcSenderId);
                cmd.Parameters.AddWithValue($"@ReceiverUserId{i}", row.ReceiverUserId);
                cmd.Parameters.AddWithValue($"@ReceiverUserType{i}", row.ReceiverUserType);
                cmd.Parameters.AddWithValue($"@ReceiverEmailAddress{i}", row.ReceiverEmailAddress);
                cmd.Parameters.AddWithValue($"@MessageReadStatus{i}", row.MessageReadStatus);
                cmd.Parameters.AddWithValue($"@MessageReadDate{i}", row.MessageReadDate.HasValue ? (object)row.MessageReadDate.Value : DBNull.Value);
                cmd.Parameters.AddWithValue($"@IsCcUser{i}", row.IsCcUser);
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
            public int MailMsgSubId { get; set; }
            public int? MailMsgMainId { get; set; }
            public int? ToUserId { get; set; }
            public string? ToUserType { get; set; }
            public string? ToMailId { get; set; }
            public int? ReadStatus { get; set; }
            public DateTime? ReadDate { get; set; }
        }

        private class TargetRow
        {
            public int EcReceiverId { get; set; }
            public int EcSenderId { get; set; }
            public int ReceiverUserId { get; set; }
            public string ReceiverUserType { get; set; } = string.Empty;
            public string ReceiverEmailAddress { get; set; } = string.Empty;
            public bool MessageReadStatus { get; set; }
            public DateTime? MessageReadDate { get; set; }
            public bool IsCcUser { get; set; }
        }
    }
}
