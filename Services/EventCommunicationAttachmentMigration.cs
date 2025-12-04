using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class EventCommunicationAttachmentMigration
    {
        private readonly ILogger<EventCommunicationAttachmentMigration> _logger;
        private readonly IConfiguration _configuration;

        public EventCommunicationAttachmentMigration(IConfiguration configuration, ILogger<EventCommunicationAttachmentMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "ec_attachmentid", type = "PostgreSQL auto-increment" },
                new { source = "Lookup: TBL_MAILMSGMAIN.EventId via DocumentId", target = "event_id", type = "Lookup -> integer (NOT NULL, FK)" },
                new { source = "Lookup: TBL_MAILMSGMAIN.MailMsgMainId via DocumentId", target = "ec_senderid", type = "Lookup -> integer (NOT NULL, FK)" },
                new { source = "UPLOADPATH", target = "upload_path", type = "nvarchar -> text (default blank)" },
                new { source = "FILENAME", target = "file_name", type = "nvarchar -> text (NOT NULL)" },
                new { source = "Type", target = "file_type", type = "nvarchar -> text (NOT NULL)" },
                new { source = "Lookup: TBL_MAILMSGMAIN.FromUserId via DocumentId", target = "uploaded_by", type = "Lookup -> integer (NULLABLE, FK)" }
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

                _logger.LogInformation("Starting EventCommunicationAttachment migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE event_communication_attachment RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset event_communication_attachment table and restarted identity sequence");
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

                // Build lookup for valid ec_senderids from PostgreSQL
                // ec_senderid preserves the original MailMsgMainId from SQL Server
                var validEcSenderIds = new HashSet<int>();
                int minEcSenderId = int.MaxValue;
                int maxEcSenderId = int.MinValue;
                using (var cmd = new NpgsqlCommand(@"
                    SELECT ec_senderid 
                    FROM event_communication_sender 
                    WHERE ec_senderid IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        validEcSenderIds.Add(id);
                        if (id < minEcSenderId) minEcSenderId = id;
                        if (id > maxEcSenderId) maxEcSenderId = id;
                    }
                }
                _logger.LogInformation($"Built ec_senderid lookup with {validEcSenderIds.Count} entries from event_communication_sender (range: {minEcSenderId} to {maxEcSenderId})");
                
                // Log some sample ec_senderids for debugging
                if (validEcSenderIds.Any())
                {
                    var sampleIds = validEcSenderIds.Take(10).OrderBy(x => x);
                    _logger.LogInformation($"Sample ec_senderid values in PostgreSQL: {string.Join(", ", sampleIds)}");
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

                // Build lookup dictionary from TBL_MAILMSGMAIN for DocumentId -> (EventId, MailMsgMainId, FromUserId)
                var documentLookup = new Dictionary<string, DocumentInfo>();
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        DocumentId,
                        EventId,
                        MailMsgMainId,
                        FromUserId
                    FROM TBL_MAILMSGMAIN
                    WHERE DocumentId IS NOT NULL 
                      AND DocumentId != ''", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var docId = reader.GetString(0).Trim();
                        var eventId = reader.IsDBNull(1) ? null : (int?)reader.GetInt32(1);
                        var mailMsgMainId = reader.GetInt32(2);
                        var fromUserId = reader.IsDBNull(3) ? null : (int?)reader.GetInt32(3);

                        if (!documentLookup.ContainsKey(docId))
                        {
                            documentLookup[docId] = new DocumentInfo
                            {
                                EventId = eventId,
                                MailMsgMainId = mailMsgMainId,
                                FromUserId = fromUserId
                            };
                        }
                    }
                }
                _logger.LogInformation($"Built document lookup with {documentLookup.Count} entries from TBL_MAILMSGMAIN");
                
                // Log first few entries for debugging
                if (documentLookup.Any())
                {
                    var sample = documentLookup.Take(3).Select(kvp => $"'{kvp.Key}' -> MailMsgMainId:{kvp.Value.MailMsgMainId}");
                    _logger.LogInformation($"Sample DocumentId mappings: {string.Join(", ", sample)}");
                    
                    // Check how many MailMsgMainIds from TBL_MAILMSGMAIN exist in event_communication_sender
                    var mailMsgMainIds = documentLookup.Values.Select(v => v.MailMsgMainId).Distinct().ToList();
                    var foundInPg = mailMsgMainIds.Count(id => validEcSenderIds.Contains(id));
                    var missingInPg = mailMsgMainIds.Count - foundInPg;
                    _logger.LogInformation($"TBL_MAILMSGMAIN analysis: {mailMsgMainIds.Count} unique MailMsgMainIds, {foundInPg} found in PostgreSQL, {missingInPg} missing");
                    
                    if (missingInPg > 0)
                    {
                        var missingIds = mailMsgMainIds.Where(id => !validEcSenderIds.Contains(id)).Take(10);
                        _logger.LogWarning($"Sample missing MailMsgMainIds (not in event_communication_sender): {string.Join(", ", missingIds)}");
                    }
                }

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        MailDocId,
                        EventId,
                        Type,
                        UPLOADPATH,
                        FILENAME,
                        DocNo
                    FROM TBL_MailAttachment
                    WHERE MailDocId IS NOT NULL
                    ORDER BY MailDocId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            MailDocId = reader.GetInt32(0),
                            EventId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            Type = reader.IsDBNull(2) ? null : reader.GetString(2),
                            UploadPath = reader.IsDBNull(3) ? null : reader.GetString(3),
                            FileName = reader.IsDBNull(4) ? null : reader.GetString(4),
                            DocNo = reader.IsDBNull(5) ? null : reader.GetString(5)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_MailAttachment");
                
                // Log first few DocNo values for debugging
                if (sourceData.Any())
                {
                    var sampleDocNos = sourceData.Take(5).Where(s => !string.IsNullOrWhiteSpace(s.DocNo))
                        .Select(s => $"MailDocId:{s.MailDocId} -> DocNo:'{s.DocNo}'");
                    _logger.LogInformation($"Sample DocNo values: {string.Join(", ", sampleDocNos)}");
                }
                
                // Count how many attachments have DocNo that maps to DocumentId
                var attachmentsWithValidDocNo = sourceData.Count(r => !string.IsNullOrWhiteSpace(r.DocNo) && documentLookup.ContainsKey(r.DocNo.Trim()));
                _logger.LogInformation($"Attachments with valid DocNo mapping: {attachmentsWithValidDocNo} out of {sourceData.Count}");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Lookup event_id and ec_senderid using DocNo
                        int? eventId = null;
                        int? ecSenderId = null;
                        int? uploadedBy = null;

                        if (!string.IsNullOrWhiteSpace(record.DocNo))
                        {
                            var trimmedDocNo = record.DocNo.Trim();
                            if (documentLookup.TryGetValue(trimmedDocNo, out var docInfo))
                            {
                                eventId = docInfo.EventId;
                                ecSenderId = docInfo.MailMsgMainId;
                                uploadedBy = docInfo.FromUserId;
                            }
                            else
                            {
                                _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: DocNo='{trimmedDocNo}' (length:{trimmedDocNo.Length}) not found in TBL_MAILMSGMAIN DocumentId lookup");
                                skippedRecords++;
                                continue;
                            }
                        }
                        else
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: DocNo is null/empty");
                            skippedRecords++;
                            continue;
                        }

                        // Validate event_id (REQUIRED - NOT NULL)
                        if (!eventId.HasValue)
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: event_id lookup returned null for DocNo={record.DocNo}");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEventIds.Contains(eventId.Value))
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: event_id={eventId} not found in event_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate ec_senderid (REQUIRED - NOT NULL)
                        if (!ecSenderId.HasValue)
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: ec_senderid lookup returned null for DocNo={record.DocNo}");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEcSenderIds.Contains(ecSenderId.Value))
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: ec_senderid={ecSenderId} not found in event_communication_sender");
                            skippedRecords++;
                            continue;
                        }

                        // Validate uploaded_by (OPTIONAL - can be NULL)
                        if (uploadedBy.HasValue && !validUserIds.Contains(uploadedBy.Value))
                        {
                            _logger.LogWarning($"MailDocId {record.MailDocId}: uploaded_by={uploadedBy} not found in users, setting to NULL");
                            uploadedBy = null;
                        }

                        // Validate required text fields (NOT NULL constraints)
                        if (string.IsNullOrWhiteSpace(record.FileName))
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: FileName is null/empty");
                            skippedRecords++;
                            continue;
                        }

                        // Type becomes file_type (NOT NULL)
                        if (string.IsNullOrWhiteSpace(record.Type))
                        {
                            _logger.LogWarning($"Skipping MailDocId {record.MailDocId}: Type is null/empty");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            EventId = eventId.Value,
                            EcSenderId = ecSenderId.Value,
                            UploadPath = string.IsNullOrWhiteSpace(record.UploadPath) ? "" : record.UploadPath, // Default blank
                            FileName = record.FileName,
                            FileType = record.Type,
                            UploadedBy = uploadedBy
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
                        _logger.LogError($"Error processing MailDocId {record.MailDocId}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
                
                // Log skip summary if all records were skipped
                if (migratedRecords == 0 && skippedRecords > 0)
                {
                    _logger.LogError($"CRITICAL: All {skippedRecords} attachment records were skipped! Common reasons:");
                    _logger.LogError("1. DocNo values don't match DocumentId in TBL_MAILMSGMAIN");
                    _logger.LogError("2. Parent sender records (MailMsgMainId) were not migrated to event_communication_sender");
                    _logger.LogError("3. Referenced event_ids or user_ids don't exist in PostgreSQL");
                    _logger.LogError("ACTION: Review EventCommunicationSenderMigration logs to see why sender records were skipped");
                    _logger.LogError($"ACTION: Run this SQL query in MSSQL to check data: SELECT COUNT(*) FROM TBL_MailAttachment WHERE DocNo IN (SELECT DocumentId FROM TBL_MAILMSGMAIN)");
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
            sql.AppendLine("INSERT INTO event_communication_attachment (");
            sql.AppendLine("    event_id, ec_senderid, upload_path, file_name, file_type,");
            sql.AppendLine("    uploaded_by, created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventId{i}, @EcSenderId{i}, @UploadPath{i}, @FileName{i}, @FileType{i}, @UploadedBy{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@EcSenderId{i}", row.EcSenderId);
                cmd.Parameters.AddWithValue($"@UploadPath{i}", row.UploadPath);
                cmd.Parameters.AddWithValue($"@FileName{i}", row.FileName);
                cmd.Parameters.AddWithValue($"@FileType{i}", row.FileType);
                cmd.Parameters.AddWithValue($"@UploadedBy{i}", (object?)row.UploadedBy ?? DBNull.Value);
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

        private class DocumentInfo
        {
            public int? EventId { get; set; }
            public int MailMsgMainId { get; set; }
            public int? FromUserId { get; set; }
        }

        private class SourceRow
        {
            public int MailDocId { get; set; }
            public int? EventId { get; set; }
            public string? Type { get; set; }
            public string? UploadPath { get; set; }
            public string? FileName { get; set; }
            public string? DocNo { get; set; }
        }

        private class TargetRow
        {
            public int EventId { get; set; }
            public int EcSenderId { get; set; }
            public string UploadPath { get; set; } = string.Empty;
            public string FileName { get; set; } = string.Empty;
            public string FileType { get; set; } = string.Empty;
            public int? UploadedBy { get; set; }
        }
    }
}
