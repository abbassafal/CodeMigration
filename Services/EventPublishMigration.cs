using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class EventPublishMigration
    {
        private readonly ILogger<EventPublishMigration> _logger;
        private readonly IConfiguration _configuration;

        public EventPublishMigration(IConfiguration configuration, ILogger<EventPublishMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "PublishEventAutoMailSendId", target = "event_publish_id", type = "int -> integer" },
                new { source = "EVENTID", target = "event_id", type = "int -> integer (FK to event_master)" },
                new { source = "SENDMAIL", target = "event_round", type = "int -> integer (lookup from event_master)" },
                new { source = "N/A", target = "event_name", type = "Lookup from event_master by event_id" },
                new { source = "N/A", target = "event_status", type = "Lookup from event_master by event_id" },
                new { source = "MSG", target = "to", type = "Leave blank (character varying)" },
                new { source = "Subject", target = "cc", type = "Leave blank (character varying)" },
                new { source = "Subject", target = "subject", type = "nvarchar -> character varying" },
                new { source = "MSG", target = "message", type = "nvarchar -> text" },
                new { source = "CreatedDate", target = "created_date", type = "datetime -> timestamp" },
                new { source = "CreatedId", target = "created_by", type = "int -> integer" }
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

                _logger.LogInformation("Starting EventPublish migration...");

                // Get event master data from PostgreSQL for lookup
                var eventMasterData = new Dictionary<int, EventMasterInfo>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_id, round, event_name, event_status 
                    FROM event_master", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var eventId = reader.GetInt32(0);
                        eventMasterData[eventId] = new EventMasterInfo
                        {
                            EventId = eventId,
                            Round = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            EventName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            EventStatus = reader.IsDBNull(3) ? string.Empty : reader.GetString(3)
                        };
                    }
                }

                _logger.LogInformation($"Found {eventMasterData.Count} events in event_master");

                // Load existing IDs into memory for fast lookup
                var existingIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_publish_id FROM event_publish", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        existingIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {existingIds.Count} existing records in target table");

                // Fetch data from SQL Server
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PublishEventAutoMailSendId,
                        EVENTID,
                        SENDMAIL,
                        MSG,
                        Subject,
                        CreatedDate,
                        CreatedId
                    FROM TBL_PublishEventAutoMailSend", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PublishEventAutoMailSendId = reader.IsDBNull(0) ? null : reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            SENDMAIL = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            MSG = reader.IsDBNull(3) ? null : reader.GetString(3),
                            Subject = reader.IsDBNull(4) ? null : reader.GetString(4),
                            CreatedDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            CreatedId = reader.IsDBNull(6) ? null : reader.GetInt32(6)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();
                var updateBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate required fields
                        if (!record.PublishEventAutoMailSendId.HasValue)
                        {
                            _logger.LogWarning($"Record skipped: PublishEventAutoMailSendId is NULL");
                            skippedRecords++;
                            continue;
                        }

                        // Validate event_id exists (FK constraint)
                        if (!record.EVENTID.HasValue || !eventMasterData.ContainsKey(record.EVENTID.Value))
                        {
                            _logger.LogDebug($"PublishEventAutoMailSendId {record.PublishEventAutoMailSendId}: event_id {record.EVENTID} not found in event_master (FK constraint violation)");
                            skippedRecords++;
                            continue;
                        }

                        // Get event master info for this event
                        var eventInfo = eventMasterData[record.EVENTID.Value];

                        var targetRow = new TargetRow
                        {
                            EventPublishId = record.PublishEventAutoMailSendId.Value,
                            EventId = record.EVENTID.Value,
                            EventRound = eventInfo.Round,
                            EventName = eventInfo.EventName,
                            EventStatus = eventInfo.EventStatus,
                            To = string.Empty, // Leave blank as per requirement
                            Cc = string.Empty, // Leave blank as per requirement
                            Subject = string.IsNullOrWhiteSpace(record.Subject) ? "Event Notification" : record.Subject.Trim(),
                            Message = string.IsNullOrWhiteSpace(record.MSG) ? string.Empty : record.MSG.Trim(),
                            CreatedDate = record.CreatedDate,
                            CreatedBy = record.CreatedId
                        };

                        if (existingIds.Contains(targetRow.EventPublishId))
                        {
                            updateBatch.Add(targetRow);
                        }
                        else
                        {
                            insertBatch.Add(targetRow);
                        }

                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }

                        if (updateBatch.Count >= batchSize)
                        {
                            await ExecuteUpdateBatch(pgConnection, updateBatch);
                            updateBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"PublishEventAutoMailSendId {record.PublishEventAutoMailSendId}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                    }
                }

                // Execute remaining batches
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                if (updateBatch.Any())
                {
                    await ExecuteUpdateBatch(pgConnection, updateBatch);
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

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO event_publish (");
            sql.AppendLine("    event_publish_id, event_id, event_round, event_name, event_status,");
            sql.AppendLine("    \"to\", cc, subject, message,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@Id{i}, @EventId{i}, @EventRound{i}, @EventName{i}, @EventStatus{i}, @To{i}, @Cc{i}, @Subject{i}, @Message{i}, @CreatedBy{i}, @CreatedDate{i}, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@Id{i}", row.EventPublishId);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@EventRound{i}", row.EventRound);
                cmd.Parameters.AddWithValue($"@EventName{i}", row.EventName);
                cmd.Parameters.AddWithValue($"@EventStatus{i}", row.EventStatus);
                cmd.Parameters.AddWithValue($"@To{i}", row.To);
                cmd.Parameters.AddWithValue($"@Cc{i}", row.Cc);
                cmd.Parameters.AddWithValue($"@Subject{i}", row.Subject);
                cmd.Parameters.AddWithValue($"@Message{i}", row.Message);
                cmd.Parameters.AddWithValue($"@CreatedBy{i}", (object?)row.CreatedBy ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", row.CreatedDate.HasValue ? row.CreatedDate.Value : DateTime.UtcNow);
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

        private async Task ExecuteUpdateBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            foreach (var row in batch)
            {
                using var cmd = new NpgsqlCommand(@"
                    UPDATE event_publish SET
                        event_id = @EventId,
                        event_round = @EventRound,
                        event_name = @EventName,
                        event_status = @EventStatus,
                        ""to"" = @To,
                        cc = @Cc,
                        subject = @Subject,
                        message = @Message,
                        modified_date = CURRENT_TIMESTAMP
                    WHERE event_publish_id = @Id", connection);

                cmd.Parameters.AddWithValue("@Id", row.EventPublishId);
                cmd.Parameters.AddWithValue("@EventId", row.EventId);
                cmd.Parameters.AddWithValue("@EventRound", row.EventRound);
                cmd.Parameters.AddWithValue("@EventName", row.EventName);
                cmd.Parameters.AddWithValue("@EventStatus", row.EventStatus);
                cmd.Parameters.AddWithValue("@To", row.To);
                cmd.Parameters.AddWithValue("@Cc", row.Cc);
                cmd.Parameters.AddWithValue("@Subject", row.Subject);
                cmd.Parameters.AddWithValue("@Message", row.Message);

                await cmd.ExecuteNonQueryAsync();
            }

            _logger.LogDebug($"Batch updated {batch.Count} records");
        }

        private class SourceRow
        {
            public int? PublishEventAutoMailSendId { get; set; }
            public int? EVENTID { get; set; }
            public int? SENDMAIL { get; set; }
            public string? MSG { get; set; }
            public string? Subject { get; set; }
            public DateTime? CreatedDate { get; set; }
            public int? CreatedId { get; set; }
        }

        private class TargetRow
        {
            public int EventPublishId { get; set; }
            public int EventId { get; set; }
            public int EventRound { get; set; }
            public string EventName { get; set; } = string.Empty;
            public string EventStatus { get; set; } = string.Empty;
            public string To { get; set; } = string.Empty;
            public string Cc { get; set; } = string.Empty;
            public string Subject { get; set; } = string.Empty;
            public string Message { get; set; } = string.Empty;
            public DateTime? CreatedDate { get; set; }
            public int? CreatedBy { get; set; }
        }

        private class EventMasterInfo
        {
            public int EventId { get; set; }
            public int Round { get; set; }
            public string EventName { get; set; } = string.Empty;
            public string EventStatus { get; set; } = string.Empty;
        }
    }
}
