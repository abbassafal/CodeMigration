using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using DataMigration.Hubs;

namespace DataMigration.Services
{
    /// <summary>
    /// Background service that handles binary attachment migration independently of the main migration flow.
    /// Provides real-time progress updates via SignalR.
    /// </summary>
    public class BinaryAttachmentBackgroundService : BackgroundService
    {
        private readonly ILogger<BinaryAttachmentBackgroundService> _logger;
        private readonly IConfiguration _configuration;
        private readonly IHubContext<MigrationProgressHub> _hubContext;
        private readonly ConcurrentQueue<BinaryMigrationJob> _jobQueue = new();
        private readonly ConcurrentDictionary<string, BinaryMigrationStatus> _jobStatuses = new();

        private const int BINARY_COPY_BATCH = 50; // Increased for better performance
        private const long MAX_BINARY_SIZE = 250 * 1024 * 1024; // 250 MB
        private const int MAX_PARALLEL_BATCHES = 4; // Process 4 batches concurrently

        public BinaryAttachmentBackgroundService(
            IConfiguration configuration,
            ILogger<BinaryAttachmentBackgroundService> logger,
            IHubContext<MigrationProgressHub> hubContext)
        {
            _configuration = configuration;
            _logger = logger;
            _hubContext = hubContext;
        }

        /// <summary>
        /// Enqueue a new binary migration job
        /// </summary>
        public string EnqueueJob(string tableName, string migrationId)
        {
            var jobId = Guid.NewGuid().ToString();
            var job = new BinaryMigrationJob
            {
                JobId = jobId,
                TableName = tableName,
                MigrationId = migrationId,
                EnqueuedAt = DateTime.UtcNow
            };

            _jobQueue.Enqueue(job);
            
            var status = new BinaryMigrationStatus
            {
                JobId = jobId,
                MigrationId = migrationId,
                TableName = tableName,
                State = BinaryMigrationState.Queued,
                TotalFiles = 0,
                ProcessedFiles = 0,
                EnqueuedAt = DateTime.UtcNow
            };
            
            _jobStatuses[jobId] = status;

            _logger.LogInformation($"Binary migration job enqueued: JobId={jobId}, MigrationId={migrationId}, Table={tableName}");
            
            // Notify via SignalR
            _ = NotifyStatusUpdate(status);

            return jobId;
        }

        /// <summary>
        /// Get the status of a specific job
        /// </summary>
        public BinaryMigrationStatus? GetJobStatus(string jobId)
        {
            return _jobStatuses.TryGetValue(jobId, out var status) ? status : null;
        }
        /// <summary>
        /// Get the status of all jobs
        /// </summary>
        public IEnumerable<BinaryMigrationStatus> GetAllJobStatuses()
        {
            return _jobStatuses.Values;
        }
        /// <summary>
        /// Background execution loop
        /// </summary>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("BinaryAttachmentBackgroundService started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (_jobQueue.TryDequeue(out var job))
                    {
                        await ProcessJobAsync(job, stoppingToken);
                    }
                    else
                    {
                        // No jobs in queue, wait a bit
                        await Task.Delay(1000, stoppingToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("BinaryAttachmentBackgroundService is stopping");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in BinaryAttachmentBackgroundService execution loop");
                    await Task.Delay(5000, stoppingToken); // Wait before retrying
                }
            }

            _logger.LogInformation("BinaryAttachmentBackgroundService stopped");
        }

        private async Task ProcessJobAsync(BinaryMigrationJob job, CancellationToken cancellationToken)
        {
            var status = _jobStatuses[job.JobId];
            status.State = BinaryMigrationState.Running;
            status.StartedAt = DateTime.UtcNow;
            
            await NotifyStatusUpdate(status);

            try
            {
                _logger.LogInformation($"Processing binary migration job: JobId={job.JobId}, Table={job.TableName}");

                // Route to appropriate migration handler based on table name
                if (job.TableName.ToLower() == "prattachment")
                {
                    await MigratePRAttachmentBinariesAsync(job, status, cancellationToken);
                }
                // Add more table handlers here as needed
                else
                {
                    throw new NotSupportedException($"Binary migration not supported for table: {job.TableName}");
                }

                status.State = BinaryMigrationState.Completed;
                status.CompletedAt = DateTime.UtcNow;
                status.ProgressPercentage = 100;
                
                _logger.LogInformation($"Binary migration job completed successfully: JobId={job.JobId}, ProcessedFiles={status.ProcessedFiles}");
            }
            catch (Exception ex)
            {
                status.State = BinaryMigrationState.Failed;
                status.ErrorMessage = ex.Message;
                status.CompletedAt = DateTime.UtcNow;
                
                _logger.LogError(ex, $"Binary migration job failed: JobId={job.JobId}");
            }

            await NotifyStatusUpdate(status);
        }

        private async Task MigratePRAttachmentBinariesAsync(BinaryMigrationJob job, BinaryMigrationStatus status, CancellationToken cancellationToken)
        {
            var sqlConnString = _configuration.GetConnectionString("SqlServer");
            var pgConnString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnString) || string.IsNullOrEmpty(pgConnString))
                throw new InvalidOperationException("Connection strings not configured");

            await using var sqlConn = new SqlConnection(sqlConnString);
            await using var pgConn = new NpgsqlConnection(pgConnString);

            await sqlConn.OpenAsync(cancellationToken);
            await pgConn.OpenAsync(cancellationToken);

            // Get total count of binary records
            var countCmd = new SqlCommand(
                "SELECT COUNT(*) FROM TBL_PRATTACHMENT WHERE PRATTACHMENTDATA IS NOT NULL",
                sqlConn);
            status.TotalFiles = (int)await countCmd.ExecuteScalarAsync(cancellationToken);
            
            _logger.LogInformation($"Total binary files to migrate: {status.TotalFiles:N0}");
            await NotifyStatusUpdate(status);

            if (status.TotalFiles == 0)
            {
                _logger.LogInformation("No binary files found to migrate");
                return;
            }

            // Select binary records
            var selectSql = @"
                SELECT PRATTACHMENTID, PRATTACHMENTDATA 
                FROM TBL_PRATTACHMENT 
                WHERE PRATTACHMENTDATA IS NOT NULL 
                ORDER BY PRATTACHMENTID";

            using var selectCmd = new SqlCommand(selectSql, sqlConn);
            selectCmd.CommandTimeout = 0;

            using var reader = await selectCmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, cancellationToken);

            var allBatches = new List<List<(long id, byte[] data)>>();
            var currentBatch = new List<(long id, byte[] data)>();
            int skippedLarge = 0;
            int skippedEmpty = 0;
            var skippedRecords = new List<(string RecordId, string Reason)>();
            int readCount = 0;

            // Phase 1: Read all data into batches first
            _logger.LogInformation("Phase 1: Reading binary data from SQL Server...");
            while (await reader.ReadAsync(cancellationToken))
            {
                readCount++;
                var id = reader.IsDBNull(0) ? throw new InvalidOperationException("Null PRATTACHMENTID") : Convert.ToInt64(reader.GetValue(0));
                long size = reader.IsDBNull(1) ? 0 : reader.GetBytes(1, 0, null, 0, 0);

                if (size == 0)
                {
                    skippedEmpty++;
                    skippedRecords.Add((id.ToString(), "Empty binary data"));
                    continue;
                }

                if (size > MAX_BINARY_SIZE)
                {
                    _logger.LogWarning($"Skipping large binary for ID {id} (size {size:N0} bytes)");
                    skippedLarge++;
                    skippedRecords.Add((id.ToString(), $"Binary too large: {size:N0} bytes"));
                    continue;
                }

                // Read binary data directly into byte array
                var buffer = new byte[size];
                reader.GetBytes(1, 0, buffer, 0, (int)size);
                currentBatch.Add((id, buffer));

                // Log progress every 100 files
                if (readCount % 100 == 0)
                {
                    _logger.LogInformation($"Read {readCount:N0} / {status.TotalFiles:N0} files from SQL Server ({readCount * 100.0 / status.TotalFiles:F1}%)");
                }

                // Create new batch when threshold reached
                if (currentBatch.Count >= BINARY_COPY_BATCH)
                {
                    allBatches.Add(currentBatch);
                    currentBatch = new List<(long id, byte[] data)>();
                }
            }

            // Add remaining batch
            if (currentBatch.Count > 0)
            {
                allBatches.Add(currentBatch);
            }

            _logger.LogInformation($"Phase 1 complete: Read {readCount:N0} files into {allBatches.Count} batches");

            // Phase 2: Process batches in parallel
            _logger.LogInformation($"Phase 2: Processing {allBatches.Count} batches in parallel (max {MAX_PARALLEL_BATCHES} concurrent)...");
            
            using var semaphore = new SemaphoreSlim(MAX_PARALLEL_BATCHES);
            var processingTasks = new List<Task>();

            for (int i = 0; i < allBatches.Count; i++)
            {
                var batchIndex = i;
                var batch = allBatches[i];

                var task = Task.Run(async () =>
                {
                    await semaphore.WaitAsync(cancellationToken);
                    try
                    {
                        // Each task gets its own PostgreSQL connection
                        await using var pgConnParallel = new NpgsqlConnection(pgConnString);
                        await pgConnParallel.OpenAsync(cancellationToken);

                        await ProcessBinaryBatchParallelAsync(
                            pgConnParallel, 
                            batch, 
                            batchIndex, 
                            allBatches.Count, 
                            status, 
                            cancellationToken);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cancellationToken);

                processingTasks.Add(task);
            }

            // Wait for all batches to complete
            await Task.WhenAll(processingTasks);

            // Clear memory
            allBatches.Clear();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            status.SkippedFiles = skippedEmpty + skippedLarge;
            _logger.LogInformation($"Binary migration complete: Processed={status.ProcessedFiles}, Skipped={status.SkippedFiles}");

            // Export migration stats to Excel
            var excelPath = Path.Combine("migration_outputs", $"BinaryMigration_{job.TableName}_{job.MigrationId}.xlsx");
            MigrationStatsExporter.ExportToExcel(
                excelPath,
                status.TotalFiles,
                status.ProcessedFiles,
                status.SkippedFiles,
                _logger,
                skippedRecords
            );
            _logger.LogInformation($"Migration stats exported to {excelPath}");
        }

        private async Task ProcessBinaryBatchAsync(
            NpgsqlConnection pgConn,
            List<(long id, byte[] data)> batch,
            BinaryMigrationStatus status,
            CancellationToken cancellationToken)
        {
            var batchStart = DateTime.UtcNow;
            _logger.LogInformation($"Processing batch of {batch.Count} files...");

            // Create temp table for this batch
            var createTempSql = @"
                DROP TABLE IF EXISTS tmp_pr_attachments_bin;
                CREATE TEMP TABLE tmp_pr_attachments_bin (
                    pr_attachment_id bigint,
                    pr_attachment_data bytea
                );";

            await using (var createCmd = new NpgsqlCommand(createTempSql, pgConn))
                await createCmd.ExecuteNonQueryAsync(cancellationToken);

            // Binary import - directly write byte arrays (MUCH faster than streams)
            await using (var importer = pgConn.BeginBinaryImport(
                "COPY tmp_pr_attachments_bin (pr_attachment_id, pr_attachment_data) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var item in batch)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    importer.StartRow();
                    importer.Write(item.id, NpgsqlDbType.Bigint);
                    importer.Write(item.data, NpgsqlDbType.Bytea);

                    status.ProcessedFiles++;
                    status.ProgressPercentage = status.TotalFiles > 0
                        ? (int)((double)status.ProcessedFiles / status.TotalFiles * 100)
                        : 0;
                }

                await importer.CompleteAsync(cancellationToken);
            }

            // Merge into target table
            var mergeSql = @"
                UPDATE pr_attachments t
                SET pr_attachment_data = b.pr_attachment_data,
                    modified_date = CURRENT_TIMESTAMP
                FROM tmp_pr_attachments_bin b
                WHERE t.pr_attachment_id = b.pr_attachment_id;";

            await using var mergeCmd = new NpgsqlCommand(mergeSql, pgConn);
            await mergeCmd.ExecuteNonQueryAsync(cancellationToken);

            var batchDuration = (DateTime.UtcNow - batchStart).TotalSeconds;
            var throughput = batch.Count / batchDuration;
            status.CurrentOperation = $"Batch complete: {batch.Count} files in {batchDuration:F1}s ({throughput:F1} files/sec)";
            
            _logger.LogInformation(status.CurrentOperation);
            await NotifyStatusUpdate(status);
        }

        private async Task ProcessBinaryBatchParallelAsync(
            NpgsqlConnection pgConn,
            List<(long id, byte[] data)> batch,
            int batchIndex,
            int totalBatches,
            BinaryMigrationStatus status,
            CancellationToken cancellationToken)
        {
            var batchStart = DateTime.UtcNow;
            _logger.LogInformation($"Processing batch {batchIndex + 1}/{totalBatches} ({batch.Count} files)...");

            // Create temp table with unique name for this batch to avoid conflicts in parallel execution
            var tempTableName = $"tmp_pr_attachments_bin_{Guid.NewGuid():N}";
            var createTempSql = $@"
                CREATE TEMP TABLE {tempTableName} (
                    pr_attachment_id bigint,
                    pr_attachment_data bytea
                );";

            await using (var createCmd = new NpgsqlCommand(createTempSql, pgConn))
                await createCmd.ExecuteNonQueryAsync(cancellationToken);

            // Binary import - directly write byte arrays
            await using (var importer = pgConn.BeginBinaryImport(
                $"COPY {tempTableName} (pr_attachment_id, pr_attachment_data) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var item in batch)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    importer.StartRow();
                    importer.Write(item.id, NpgsqlDbType.Bigint);
                    importer.Write(item.data, NpgsqlDbType.Bytea);

                    // Thread-safe increment
                    var newProcessedCount = Interlocked.Increment(ref status.ProcessedFiles);
                    
                    // Update progress percentage (thread-safe)
                    var newProgress = status.TotalFiles > 0
                        ? (int)((double)newProcessedCount / status.TotalFiles * 100)
                        : 0;
                    
                    if (newProgress != status.ProgressPercentage)
                    {
                        status.ProgressPercentage = newProgress;
                        _ = NotifyStatusUpdate(status);
                    }
                }

                await importer.CompleteAsync(cancellationToken);
            }

            // Merge into target table
            var mergeSql = $@"
                UPDATE pr_attachments t
                SET pr_attachment_data = b.pr_attachment_data,
                    modified_date = CURRENT_TIMESTAMP
                FROM {tempTableName} b
                WHERE t.pr_attachment_id = b.pr_attachment_id;";

            await using var mergeCmd = new NpgsqlCommand(mergeSql, pgConn);
            await mergeCmd.ExecuteNonQueryAsync(cancellationToken);

            // Clean up temp table
            var dropTempSql = $"DROP TABLE IF EXISTS {tempTableName};";
            await using var dropCmd = new NpgsqlCommand(dropTempSql, pgConn);
            await dropCmd.ExecuteNonQueryAsync(cancellationToken);

            var batchDuration = (DateTime.UtcNow - batchStart).TotalSeconds;
            var throughput = batch.Count / batchDuration;
            
            _logger.LogInformation(
                $"✓ Batch {batchIndex + 1}/{totalBatches} complete: {batch.Count} files in {batchDuration:F1}s ({throughput:F1} files/sec)");
        }

        private async Task NotifyStatusUpdate(BinaryMigrationStatus status)
        {
            try
            {
                var progressInfo = new
                {
                    JobId = status.JobId,
                    MigrationId = status.MigrationId,
                    TableName = status.TableName,
                    State = status.State.ToString(),
                    ProcessedFiles = status.ProcessedFiles,
                    TotalFiles = status.TotalFiles,
                    SkippedFiles = status.SkippedFiles,
                    ProgressPercentage = status.ProgressPercentage,
                    CurrentOperation = status.CurrentOperation,
                    ElapsedTime = status.StartedAt.HasValue 
                        ? (DateTime.UtcNow - status.StartedAt.Value).ToString(@"hh\:mm\:ss")
                        : "00:00:00",
                    ErrorMessage = status.ErrorMessage
                };

                await _hubContext.Clients.Group(status.MigrationId)
                    .SendAsync("BinaryMigrationProgress", progressInfo);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending progress update via SignalR");
            }
        }
    }

    public class BinaryMigrationJob
    {
        public string JobId { get; set; } = "";
        public string TableName { get; set; } = "";
        public string MigrationId { get; set; } = "";
        public DateTime EnqueuedAt { get; set; }
    }

    public class BinaryMigrationStatus
    {
        public string JobId { get; set; } = "";
        public string MigrationId { get; set; } = "";
        public string TableName { get; set; } = "";
        public BinaryMigrationState State { get; set; }
        public int TotalFiles { get; set; }
        public int ProcessedFiles;
        public int SkippedFiles { get; set; }
        public int ProgressPercentage { get; set; }
        public string CurrentOperation { get; set; } = "";
        public string ErrorMessage { get; set; } = "";
        public DateTime EnqueuedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
    }

    public enum BinaryMigrationState
    {
        Queued,
        Running,
        Completed,
        Failed
    }
}
