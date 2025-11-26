using Microsoft.AspNetCore.SignalR;
using System.Threading.Tasks;
using DataMigration.Services;

namespace DataMigration.Hubs
{
    public class MigrationProgressHub : Hub
    {
        public async Task JoinMigrationGroup(string migrationId)
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, migrationId);
        }

        public async Task LeaveMigrationGroup(string migrationId)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, migrationId);
        }
    }

    public class SignalRMigrationProgress : IMigrationProgress
    {
        private readonly IHubContext<MigrationProgressHub> _hubContext;
        private readonly string _migrationId;
        private readonly object _lock = new object();
        private DateTime _lastUpdate = DateTime.MinValue;
        private readonly TimeSpan _updateInterval = TimeSpan.FromMilliseconds(1000); // Update every 1 second

        public SignalRMigrationProgress(IHubContext<MigrationProgressHub> hubContext, string migrationId)
        {
            _hubContext = hubContext;
            _migrationId = migrationId;
        }

        public void ReportProgress(int processed, int total, string currentOperation, TimeSpan elapsed)
        {
            lock (_lock)
            {
                var now = DateTime.Now;
                if (now - _lastUpdate < _updateInterval && processed < total)
                    return; // Throttle updates

                _lastUpdate = now;

                var progressInfo = new MigrationProgressInfo
                {
                    ProcessedRecords = processed,
                    TotalRecords = total,
                    CurrentOperation = currentOperation,
                    ElapsedTime = elapsed
                };

                _ = Task.Run(async () =>
                {
                    await _hubContext.Clients.Group(_migrationId).SendAsync("ProgressUpdate", progressInfo);
                });
            }
        }

        public void ReportCompleted(int totalProcessed, int totalInserted, TimeSpan totalTime)
        {
            var completionInfo = new
            {
                TotalProcessed = totalProcessed,
                TotalInserted = totalInserted,
                SkippedRecords = totalProcessed - totalInserted,
                TotalTime = totalTime,
                IsCompleted = true
            };

            _ = Task.Run(async () =>
            {
                await _hubContext.Clients.Group(_migrationId).SendAsync("MigrationCompleted", completionInfo);
            });
        }

        public void ReportError(string error, int processedSoFar)
        {
            var errorInfo = new
            {
                Error = error,
                ProcessedSoFar = processedSoFar,
                IsError = true
            };

            _ = Task.Run(async () =>
            {
                await _hubContext.Clients.Group(_migrationId).SendAsync("MigrationError", errorInfo);
            });
        }
    }
}
