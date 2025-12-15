using Microsoft.AspNetCore.Mvc;
using DataMigration.Services;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace DataMigration.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class BinaryAttachmentMigrationController : ControllerBase
    {
        private readonly BinaryAttachmentBackgroundService _binaryService;
        private readonly ILogger<BinaryAttachmentMigrationController> _logger;

        public BinaryAttachmentMigrationController(
            BinaryAttachmentBackgroundService binaryService,
            ILogger<BinaryAttachmentMigrationController> logger)
        {
            _binaryService = binaryService;
            _logger = logger;
        }

        /// <summary>
        /// Start binary attachment migration for a specific table
        /// </summary>
        /// <param name="tableName">Table name (e.g., "prattachment")</param>
        /// <param name="migrationId">Optional migration ID for tracking</param>
        /// <returns>Job ID for tracking progress</returns>
        [HttpPost("start")]
        public IActionResult StartBinaryMigration([FromQuery] string tableName, [FromQuery] string? migrationId = null)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(tableName))
                {
                    return BadRequest(new { error = "Table name is required" });
                }

                var finalMigrationId = migrationId ?? Guid.NewGuid().ToString();
                var jobId = _binaryService.EnqueueJob(tableName, finalMigrationId);

                _logger.LogInformation($"Binary migration started: JobId={jobId}, Table={tableName}, MigrationId={finalMigrationId}");

                return Ok(new
                {
                    success = true,
                    jobId = jobId,
                    migrationId = finalMigrationId,
                    tableName = tableName,
                    message = "Binary migration job has been queued and will start processing shortly"
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error starting binary migration for table: {tableName}");
                return StatusCode(500, new
                {
                    success = false,
                    error = ex.Message
                });
            }
        }

        /// <summary>
        /// Get the status of a specific binary migration job
        /// </summary>
        /// <param name="jobId">Job ID returned from start endpoint</param>
        [HttpGet("status/{jobId}")]
        public IActionResult GetJobStatus(string jobId)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(jobId))
                {
                    return BadRequest(new { error = "Job ID is required" });
                }

                var status = _binaryService.GetJobStatus(jobId);
                
                if (status == null)
                {
                    return NotFound(new { error = $"Job {jobId} not found" });
                }

                var elapsedTime = status.StartedAt.HasValue
                    ? (status.CompletedAt ?? DateTime.UtcNow) - status.StartedAt.Value
                    : TimeSpan.Zero;

                return Ok(new
                {
                    jobId = status.JobId,
                    migrationId = status.MigrationId,
                    tableName = status.TableName,
                    state = status.State.ToString(),
                    totalFiles = status.TotalFiles,
                    processedFiles = status.ProcessedFiles,
                    skippedFiles = status.SkippedFiles,
                    progressPercentage = status.ProgressPercentage,
                    currentOperation = status.CurrentOperation,
                    errorMessage = status.ErrorMessage,
                    enqueuedAt = status.EnqueuedAt,
                    startedAt = status.StartedAt,
                    completedAt = status.CompletedAt,
                    elapsedTime = elapsedTime.ToString(@"hh\:mm\:ss")
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error getting status for job: {jobId}");
                return StatusCode(500, new
                {
                    success = false,
                    error = ex.Message
                });
            }
        }

        /// <summary>
        /// Get all job statuses
        /// </summary>
        [HttpGet("status/all")]
        public IActionResult GetAllJobStatuses()
        {
            try
            {
                var allStatuses = _binaryService.GetAllJobStatuses();
                
                return Ok(new
                {
                    totalJobs = allStatuses.Count(),
                    jobs = allStatuses.Select(status =>
                    {
                        var elapsedTime = status.StartedAt.HasValue
                            ? (status.CompletedAt ?? DateTime.UtcNow) - status.StartedAt.Value
                            : TimeSpan.Zero;

                        return new
                        {
                            jobId = status.JobId,
                            migrationId = status.MigrationId,
                            tableName = status.TableName,
                            state = status.State.ToString(),
                            totalFiles = status.TotalFiles,
                            processedFiles = status.ProcessedFiles,
                            skippedFiles = status.SkippedFiles,
                            progressPercentage = status.ProgressPercentage,
                            currentOperation = status.CurrentOperation,
                            errorMessage = status.ErrorMessage,
                            enqueuedAt = status.EnqueuedAt,
                            startedAt = status.StartedAt,
                            completedAt = status.CompletedAt,
                            elapsedTime = elapsedTime.ToString(@"hh\:mm\:ss")
                        };
                    }).ToList()
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting all job statuses");
                return StatusCode(500, new
                {
                    success = false,
                    error = ex.Message
                });
            }
        }

        /// <summary>
        /// Quick start for PR Attachment binary migration
        /// </summary>
        [HttpPost("start-prattachment")]
        public IActionResult StartPRAttachmentBinaryMigration()
        {
            return StartBinaryMigration("prattachment", $"PRAttachment-{DateTime.UtcNow:yyyyMMdd-HHmmss}");
        }
    }
}
