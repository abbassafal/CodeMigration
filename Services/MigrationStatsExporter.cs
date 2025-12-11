using ClosedXML.Excel;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace DataMigration.Services
{
    public static class MigrationStatsExporter
    {
        /// <summary>
        /// Exports migration statistics and skipped records to an Excel file.
        /// </summary>
        /// <param name="filePath">Path to the Excel file to create.</param>
        /// <param name="totalCount">Total records processed.</param>
        /// <param name="insertedCount">Records inserted.</param>
        /// <param name="skippedCount">Records skipped.</param>
        /// <param name="logger">Logger for status messages.</param>
        /// <param name="skippedRecords">Optional list of skipped records (RecordId, Reason).</param>
        public static void ExportToExcel(string filePath, int totalCount, int insertedCount, int skippedCount, ILogger logger, List<(string RecordId, string Reason)>? skippedRecords = null)
        {
            using var workbook = new XLWorkbook();
            var worksheet = workbook.Worksheets.Add("MigrationStats");
            worksheet.Cell(1, 1).Value = "Metric";
            worksheet.Cell(1, 2).Value = "Count";
            worksheet.Cell(2, 1).Value = "Total Records";
            worksheet.Cell(2, 2).Value = totalCount;
            worksheet.Cell(3, 1).Value = "Inserted Records";
            worksheet.Cell(3, 2).Value = insertedCount;
            worksheet.Cell(4, 1).Value = "Skipped Records";
            worksheet.Cell(4, 2).Value = skippedCount;

            if (skippedRecords != null && skippedRecords.Count > 0)
            {
                var skippedSheet = workbook.Worksheets.Add("SkippedRecords");
                skippedSheet.Cell(1, 1).Value = "RecordId";
                skippedSheet.Cell(1, 2).Value = "Reason";
                int row = 2;
                foreach (var rec in skippedRecords)
                {
                    skippedSheet.Cell(row, 1).Value = rec.RecordId;
                    skippedSheet.Cell(row, 2).Value = rec.Reason;
                    row++;
                }
            }

            workbook.SaveAs(filePath);
            logger.LogInformation($"Migration stats exported to Excel: {filePath}");
        }
    }
}
