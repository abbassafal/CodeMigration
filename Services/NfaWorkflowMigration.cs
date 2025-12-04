using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class NfaWorkflowMigration
    {
        private readonly ILogger<NfaWorkflowMigration> _logger;
        private readonly IConfiguration _configuration;

        public NfaWorkflowMigration(IConfiguration configuration, ILogger<NfaWorkflowMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "TechApprovalId", target = "nfa_workflow_id", type = "int -> integer (NOT NULL, Auto-increment)" },
                new { source = "AWARDEVENTMAINID", target = "nfa_header_id", type = "int -> integer (NOT NULL, FK to nfa_header)" },
                new { source = "ApprovedBy", target = "approved_by", type = "nvarchar -> integer (NULLABLE, FK to users_master)" },
                new { source = "CreateDate", target = "assign_date", type = "datetime -> timestamp (NULLABLE)" },
                new { source = "Level", target = "level", type = "int -> integer (NULLABLE)" },
                new { source = "CreatedBy", target = "created_by", type = "int -> integer (NULLABLE)" },
                new { source = "CreateDate", target = "created_date", type = "datetime -> timestamp (NULLABLE)" }
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

                _logger.LogInformation("Starting NfaWorkflow migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE nfa_workflow RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset nfa_workflow table and restarted identity sequence");
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

                // Build lookup for valid user_id from PostgreSQL (for approved_by validation)
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
                        TechApprovalId,
                        AWARDEVENTMAINID,
                        ApprovedBy,
                        Level,
                        CreatedBy,
                        CreateDate
                    FROM TBL_StandAloneQCSApprovalAuthority
                    WHERE TechApprovalId IS NOT NULL
                    ORDER BY TechApprovalId", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            TechApprovalId = reader.GetInt32(0),
                            AwardEventMainId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            ApprovedBy = reader.IsDBNull(2) ? null : reader.GetString(2),
                            Level = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            CreatedBy = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            CreateDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_StandAloneQCSApprovalAuthority");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate nfa_header_id (REQUIRED - NOT NULL, FK)
                        if (!record.AwardEventMainId.HasValue)
                        {
                            _logger.LogWarning($"Skipping TechApprovalId {record.TechApprovalId}: AWARDEVENTMAINID is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validNfaHeaderIds.Contains(record.AwardEventMainId.Value))
                        {
                            _logger.LogWarning($"Skipping TechApprovalId {record.TechApprovalId}: AWARDEVENTMAINID={record.AwardEventMainId} not found in nfa_header");
                            skippedRecords++;
                            continue;
                        }

                        // Parse and validate approved_by (OPTIONAL, but if present must be valid FK)
                        int? approvedBy = null;
                        if (!string.IsNullOrWhiteSpace(record.ApprovedBy))
                        {
                            if (int.TryParse(record.ApprovedBy, out var approvedByValue))
                            {
                                // Validate FK only if it's a valid integer
                                if (validUserIds.Contains(approvedByValue))
                                {
                                    approvedBy = approvedByValue;
                                }
                                else
                                {
                                    _logger.LogWarning($"TechApprovalId {record.TechApprovalId}: ApprovedBy={approvedByValue} not found in users_master, setting to null");
                                }
                            }
                            else
                            {
                                _logger.LogWarning($"TechApprovalId {record.TechApprovalId}: ApprovedBy='{record.ApprovedBy}' is not a valid integer, setting to null");
                            }
                        }

                        // Convert CreateDate to timestamp for both assign_date and created_date
                        DateTime? createdDate = record.CreateDate;
                        DateTime? assignDate = record.CreateDate; // Same source for both fields

                        var targetRow = new TargetRow
                        {
                            NfaWorkflowId = record.TechApprovalId,
                            NfaHeaderId = record.AwardEventMainId.Value,
                            ApprovedBy = approvedBy,
                            AssignDate = assignDate,
                            Level = record.Level,
                            CreatedBy = record.CreatedBy,
                            CreatedDate = createdDate
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
                        _logger.LogError($"Error processing TechApprovalId {record.TechApprovalId}: {ex.Message}");
                        skippedRecords++;
                    }
                }

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
            sql.AppendLine("INSERT INTO nfa_workflow (");
            sql.AppendLine("    nfa_workflow_id, nfa_header_id, approved_by, assign_date, level,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@NfaWorkflowId{i}, @NfaHeaderId{i}, @ApprovedBy{i}, @AssignDate{i}, @Level{i}, @CreatedBy{i}, @CreatedDate{i}, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@NfaWorkflowId{i}", row.NfaWorkflowId);
                cmd.Parameters.AddWithValue($"@NfaHeaderId{i}", row.NfaHeaderId);
                cmd.Parameters.AddWithValue($"@ApprovedBy{i}", row.ApprovedBy.HasValue ? row.ApprovedBy.Value : DBNull.Value);
                cmd.Parameters.AddWithValue($"@AssignDate{i}", row.AssignDate.HasValue ? row.AssignDate.Value : DBNull.Value);
                cmd.Parameters.AddWithValue($"@Level{i}", row.Level.HasValue ? row.Level.Value : DBNull.Value);
                cmd.Parameters.AddWithValue($"@CreatedBy{i}", row.CreatedBy.HasValue ? row.CreatedBy.Value : DBNull.Value);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", row.CreatedDate.HasValue ? row.CreatedDate.Value : DBNull.Value);
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
            public int TechApprovalId { get; set; }
            public int? AwardEventMainId { get; set; }
            public string? ApprovedBy { get; set; }
            public int? Level { get; set; }
            public int? CreatedBy { get; set; }
            public DateTime? CreateDate { get; set; }
        }

        private class TargetRow
        {
            public int NfaWorkflowId { get; set; }
            public int NfaHeaderId { get; set; }
            public int? ApprovedBy { get; set; }
            public DateTime? AssignDate { get; set; }
            public int? Level { get; set; }
            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }
        }
    }
}
