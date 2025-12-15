using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class EventCommunicationSenderMigration
    {
        private readonly ILogger<EventCommunicationSenderMigration> _logger;
        private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public EventCommunicationSenderMigration(IConfiguration configuration, ILogger<EventCommunicationSenderMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "MailMsgMainId", target = "ec_senderid", type = "int -> integer" },
                new { source = "EventId", target = "event_id", type = "int -> integer (NOT NULL)" },
                new { source = "FromUserId", target = "sender_userid", type = "int -> integer (NOT NULL)" },
                new { source = "FromUserType", target = "sender_user_type", type = "nvarchar -> text (NOT NULL)" },
                new { source = "FromMailId", target = "send_email_address", type = "nvarchar -> text (NOT NULL)" },
                new { source = "CommunicationType", target = "communication_type", type = "nvarchar -> text (NOT NULL)" },
                new { source = "Subject", target = "subject", type = "nvarchar -> text (NOT NULL)" },
                new { source = "BodyText", target = "message", type = "nvarchar -> text (NOT NULL)" },
                new { source = "PARENTID", target = "replyid", type = "int -> integer (NULLABLE)" },
                new { source = "SequenceID", target = "sequence_number", type = "int -> integer (NULLABLE)" }
            };
        }

        public async Task<int> MigrateAsync()
        {
        _migrationLogger = new MigrationLogger(_logger, "event_communication_sender");
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

                _logger.LogInformation("Starting EventCommunicationSender migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE event_communication_sender RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset event_communication_sender table and restarted identity sequence");
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

                // Build lookup for valid parent ec_senderid (for replyid FK)
                // We'll build this after first pass or allow NULL
                var validEcSenderIds = new HashSet<int>();

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        MailMsgMainId,
                        EventId,
                        FromUserId,
                        FromUserType,
                        FromMailId,
                        CommunicationType,
                        Subject,
                        BodyText,
                        PARENTID,
                        SequenceID,
                        CreatedDate
                    FROM TBL_MAILMSGMAIN
                    WHERE MailMsgMainId IS NOT NULL
                    ORDER BY MailMsgMainId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            MailMsgMainId = reader.GetInt32(0),
                            EventId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            FromUserId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            FromUserType = reader.IsDBNull(3) ? null : reader.GetString(3),
                            FromMailId = reader.IsDBNull(4) ? null : reader.GetString(4),
                            CommunicationType = reader.IsDBNull(5) ? null : reader.GetString(5),
                            Subject = reader.IsDBNull(6) ? null : reader.GetString(6),
                            BodyText = reader.IsDBNull(7) ? null : reader.GetString(7),
                            ParentId = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                            SequenceId = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            CreatedDate = reader.IsDBNull(10) ? null : reader.GetDateTime(10)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_MAILMSGMAIN");

                // First pass: Insert all records WITHOUT replyid to avoid FK constraint issues
                // We'll update replyid in a second pass after all records are inserted
                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();
                var recordsWithParent = new List<(int MailMsgMainId, int ParentId)>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id (REQUIRED - NOT NULL)
                        if (!record.EventId.HasValue)
                        {
                            _migrationLogger.LogSkipped("event_id is null", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEventIds.Contains(record.EventId.Value))
                        {
                            _migrationLogger.LogSkipped($"event_id={record.EventId} not found in event_master", 
                                $"MailMsgMainId={record.MailMsgMainId}", 
                                new Dictionary<string, object> { { "event_id", record.EventId.Value } });
                            skippedRecords++;
                            continue;
                        }

                        // Validate sender_userid (REQUIRED - NOT NULL)
                        if (!record.FromUserId.HasValue)
                        {
                            _migrationLogger.LogSkipped("FromUserId is null", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        // Validate required text fields (NOT NULL constraints)
                        if (string.IsNullOrWhiteSpace(record.FromUserType))
                        {
                            _migrationLogger.LogSkipped("FromUserType is null/empty", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(record.FromMailId))
                        {
                            _migrationLogger.LogSkipped("FromMailId is null/empty", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(record.CommunicationType))
                        {
                            _migrationLogger.LogSkipped("CommunicationType is null/empty", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(record.Subject))
                        {
                            _migrationLogger.LogSkipped("Subject is null/empty", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(record.BodyText))
                        {
                            _migrationLogger.LogSkipped("BodyText is null/empty", $"MailMsgMainId={record.MailMsgMainId}");
                            skippedRecords++;
                            continue;
                        }

                        // Track records that have a parent for second pass update
                        if (record.ParentId.HasValue && record.ParentId.Value != 0)
                        {
                            recordsWithParent.Add((record.MailMsgMainId, record.ParentId.Value));
                        }

                        var targetRow = new TargetRow
                        {
                            EcSenderId = record.MailMsgMainId,
                            EventId = record.EventId.Value,
                            SenderUserId = record.FromUserId.Value,
                            SenderUserType = record.FromUserType,
                            SendEmailAddress = record.FromMailId,
                            CommunicationType = record.CommunicationType,
                            Subject = record.Subject,
                            Message = record.BodyText,
                            ReplyId = null, // Will be updated in second pass
                            SequenceNumber = record.SequenceId,
                            CreatedDate = record.CreatedDate
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
                            $"MailMsgMainId={record.MailMsgMainId}", 
                            new Dictionary<string, object> { { "Exception", ex.GetType().Name } });
                        skippedRecords++;
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"First pass completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");

                // Second pass: Update replyid for records that have a parent
                if (recordsWithParent.Any())
                {
                    _logger.LogInformation($"Starting second pass to update replyid for {recordsWithParent.Count} records...");
                    // Build lookup of valid ec_senderids that were just inserted
                    var insertedEcSenderIds = new HashSet<int>();
                    using (var cmd = new NpgsqlCommand(@"
                        SELECT ec_senderid 
                        FROM event_communication_sender 
                        WHERE ec_senderid IS NOT NULL", pgConnection))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            insertedEcSenderIds.Add(reader.GetInt32(0));
                        }
                    }
                    int updatedCount = 0;
                    int skippedUpdateCount = 0;
                    // Batch update for better performance
                    const int updateBatchSize = 1000;
                    var updateBatch = new List<(int MailMsgMainId, int ParentId)>();
                    foreach (var (mailMsgMainId, parentId) in recordsWithParent)
                    {
                        if (insertedEcSenderIds.Contains(parentId))
                        {
                            updateBatch.Add((mailMsgMainId, parentId));
                            if (updateBatch.Count >= updateBatchSize)
                            {
                                await BatchUpdateReplyIds(pgConnection, updateBatch);
                                updatedCount += updateBatch.Count;
                                updateBatch.Clear();
                            }
                        }
                        else
                        {
                            _migrationLogger.LogSkipped($"Cannot update replyid: parent {parentId} not found in event_communication_sender", 
                                $"ec_senderid={mailMsgMainId}", 
                                new Dictionary<string, object> { { "ParentId", parentId } });
                            skippedUpdateCount++;
                        }
                    }
                    if (updateBatch.Count > 0)
                    {
                        await BatchUpdateReplyIds(pgConnection, updateBatch);
                        updatedCount += updateBatch.Count;
                    }
                    _logger.LogInformation($"Second pass completed. Updated replyid for {updatedCount} records, skipped {skippedUpdateCount}");
                }

                _logger.LogInformation($"Migration completed. Total migrated: {migratedRecords}, Total skipped: {skippedRecords}");

                // Export migration stats to Excel
                if (_migrationLogger != null)
                {
                    var skippedLogEntries = _migrationLogger.GetSkippedRecords();
                    var skippedRecordsList = skippedLogEntries.Select(e => (e.RecordIdentifier, e.Message)).ToList();
                    var excelPath = Path.Combine("migration_outputs", $"EventCommunicationSenderMigration_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
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
            sql.AppendLine("INSERT INTO event_communication_sender (");
            sql.AppendLine("    ec_senderid, event_id, sender_userid, sender_user_type,");
            sql.AppendLine("    send_email_address, communication_type, subject, message,");
            sql.AppendLine("    replyid, sequence_number,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EcSenderId{i}, @EventId{i}, @SenderUserId{i}, @SenderUserType{i}, @SendEmailAddress{i}, @CommunicationType{i}, @Subject{i}, @Message{i}, @ReplyId{i}, @SequenceNumber{i}, NULL, @CreatedDate{i}, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EcSenderId{i}", row.EcSenderId);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@SenderUserId{i}", row.SenderUserId);
                cmd.Parameters.AddWithValue($"@SenderUserType{i}", row.SenderUserType);
                cmd.Parameters.AddWithValue($"@SendEmailAddress{i}", row.SendEmailAddress);
                cmd.Parameters.AddWithValue($"@CommunicationType{i}", row.CommunicationType);
                cmd.Parameters.AddWithValue($"@Subject{i}", row.Subject);
                cmd.Parameters.AddWithValue($"@Message{i}", row.Message);
                cmd.Parameters.AddWithValue($"@ReplyId{i}", (object?)row.ReplyId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SequenceNumber{i}", (object?)row.SequenceNumber ?? DBNull.Value);
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

        // Add this helper method to the class:
        private async Task BatchUpdateReplyIds(NpgsqlConnection pgConnection, List<(int mailMsgMainId, int parentId)> batch)
        {
            if (batch == null || batch.Count == 0) return;
            // Use CASE WHEN for batch update
            var ids = batch.Select(x => x.mailMsgMainId).ToList();
            var caseStatements = new System.Text.StringBuilder();
            var parameters = new List<NpgsqlParameter>();
            for (int i = 0; i < batch.Count; i++)
            {
                caseStatements.Append($"WHEN ec_senderid = @id{i} THEN @parent{i} ");
                parameters.Add(new NpgsqlParameter($"@id{i}", batch[i].mailMsgMainId));
                parameters.Add(new NpgsqlParameter($"@parent{i}", batch[i].parentId));
            }
            var inClause = string.Join(",", ids.Select((id, i) => $"@id{i}"));
            var sql = $@"UPDATE event_communication_sender SET replyid = CASE {caseStatements} END WHERE ec_senderid IN ({inClause})";
            using (var cmd = new NpgsqlCommand(sql, pgConnection))
            {
                cmd.Parameters.AddRange(parameters.ToArray());
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private class SourceRow
        {
            public int MailMsgMainId { get; set; }
            public int? EventId { get; set; }
            public int? FromUserId { get; set; }
            public string? FromUserType { get; set; }
            public string? FromMailId { get; set; }
            public string? CommunicationType { get; set; }
            public string? Subject { get; set; }
            public string? BodyText { get; set; }
            public int? ParentId { get; set; }
            public int? SequenceId { get; set; }
            public DateTime? CreatedDate { get; set; }
        }

        private class TargetRow
        {
            public int EcSenderId { get; set; }
            public int EventId { get; set; }
            public int SenderUserId { get; set; }
            public string SenderUserType { get; set; } = string.Empty;
            public string SendEmailAddress { get; set; } = string.Empty;
            public string CommunicationType { get; set; } = string.Empty;
            public string Subject { get; set; } = string.Empty;
            public string Message { get; set; } = string.Empty;
            public int? ReplyId { get; set; }
            public int? SequenceNumber { get; set; }
            public DateTime? CreatedDate { get; set; }
        }
    }
}
