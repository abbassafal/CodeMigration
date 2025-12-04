using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class PoLineMigration
    {
        private readonly ILogger<PoLineMigration> _logger;
        private readonly IConfiguration _configuration;

        public PoLineMigration(IConfiguration configuration, ILogger<PoLineMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "POSubId", target = "po_lines_id", type = "int -> integer" },
                new { source = "POId", target = "po_header_id", type = "int -> integer (FK to po_header, NOT NULL)" },
                new { source = "SAP_Item_Code", target = "po_line_number", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "ItemCode", target = "material_code", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "ItemDescription", target = "material_name", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "ItemLongDescription", target = "material_description", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "UOM", target = "uom_code", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "Qty", target = "qty", type = "decimal -> numeric" },
                new { source = "Rate", target = "unit_price", type = "decimal -> numeric" },
                new { source = "Amount", target = "total", type = "decimal -> numeric" },
                new { source = "OtherCharges", target = "other_charges", type = "decimal -> text (NOT NULL, default '')" },
                new { source = "NetAmount", target = "net_amount", type = "decimal -> numeric" },
                new { source = "PRNo", target = "pr_number", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "PRItemNO", target = "pr_line", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "Plant", target = "plant", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "TaxCode", target = "tax_code", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "DeliveryDate", target = "delivery_date", type = "datetime -> timestamp with time zone" },
                new { source = "POCondition", target = "po_condition", type = "nvarchar -> text (NOT NULL, default '')" }
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

                _logger.LogInformation("Starting PoLine migration...");

                // Build lookup for valid po_header_ids from PostgreSQL
                var validPoHeaderIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT po_header_id FROM po_header", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validPoHeaderIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {validPoHeaderIds.Count} valid po_header_ids");

                // Fetch source data from TBL_PO_Sub
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        POSubId,
                        POId,
                        SAP_Item_Code,
                        ItemCode,
                        ItemDescription,
                        ItemLongDescription,
                        UOM,
                        Qty,
                        Rate,
                        Amount,
                        OtherCharges,
                        NetAmount,
                        PRNo,
                        PRItemNO,
                        Plant,
                        TaxCode,
                        DeliveryDate,
                        POCondition
                    FROM TBL_PO_Sub
                    WHERE POSubId IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            POSubId = reader.GetInt32(0),
                            POId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            SAP_Item_Code = reader.IsDBNull(2) ? null : reader.GetString(2),
                            ItemCode = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ItemDescription = reader.IsDBNull(4) ? null : reader.GetString(4),
                            ItemLongDescription = reader.IsDBNull(5) ? null : reader.GetString(5),
                            UOM = reader.IsDBNull(6) ? null : reader.GetString(6),
                            Qty = reader.IsDBNull(7) ? null : reader.GetDecimal(7),
                            Rate = reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                            Amount = reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                            OtherCharges = reader.IsDBNull(10) ? null : reader.GetDecimal(10),
                            NetAmount = reader.IsDBNull(11) ? null : reader.GetDecimal(11),
                            PRNo = reader.IsDBNull(12) ? null : reader.GetString(12),
                            PRItemNO = reader.IsDBNull(13) ? null : reader.GetString(13),
                            Plant = reader.IsDBNull(14) ? null : reader.GetString(14),
                            TaxCode = reader.IsDBNull(15) ? null : reader.GetString(15),
                            DeliveryDate = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                            POCondition = reader.IsDBNull(17) ? null : reader.GetString(17)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records from TBL_PO_Sub");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate po_header_id (NOT NULL - skip if NULL or not found in FK table)
                        if (!record.POId.HasValue)
                        {
                            _logger.LogDebug($"POSubId {record.POSubId}: po_header_id (POId) is NULL (NOT NULL constraint), skipping record");
                            skippedRecords++;
                            continue;
                        }

                        if (!validPoHeaderIds.Contains(record.POId.Value))
                        {
                            _logger.LogDebug($"POSubId {record.POSubId}: po_header_id {record.POId.Value} not found in po_header (FK constraint), skipping record");
                            skippedRecords++;
                            continue;
                        }

                        // Convert OtherCharges (decimal) to text (NOT NULL)
                        string otherCharges = record.OtherCharges?.ToString() ?? "";

                        var targetRow = new TargetRow
                        {
                            PoLinesId = record.POSubId,
                            PoHeaderId = record.POId.Value, // NOT NULL - validated above
                            PoLineNumber = record.SAP_Item_Code ?? "", // NOT NULL - default to empty string
                            MaterialCode = record.ItemCode ?? "", // NOT NULL - default to empty string
                            MaterialName = record.ItemDescription ?? "", // NOT NULL - default to empty string
                            MaterialDescription = record.ItemLongDescription ?? "", // NOT NULL - default to empty string
                            UomCode = record.UOM ?? "", // NOT NULL - default to empty string
                            Qty = record.Qty,
                            UnitPrice = record.Rate,
                            Total = record.Amount,
                            OtherCharges = otherCharges, // NOT NULL - converted from decimal
                            NetAmount = record.NetAmount,
                            PrNumber = record.PRNo ?? "", // NOT NULL - default to empty string
                            PrLine = record.PRItemNO ?? "", // NOT NULL - default to empty string
                            Plant = record.Plant ?? "", // NOT NULL - default to empty string
                            TaxCode = record.TaxCode ?? "", // NOT NULL - default to empty string
                            DeliveryDate = record.DeliveryDate,
                            PoCondition = record.POCondition ?? "" // NOT NULL - default to empty string
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"POSubId {record.POSubId}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
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
            sql.AppendLine("INSERT INTO po_line (");
            sql.AppendLine("    po_lines_id, po_header_id, po_line_number, material_code,");
            sql.AppendLine("    material_name, material_description, uom_code, qty,");
            sql.AppendLine("    unit_price, total, other_charges, net_amount,");
            sql.AppendLine("    pr_number, pr_line, plant, tax_code,");
            sql.AppendLine("    delivery_date, po_condition,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@PoLinesId{i}, @PoHeaderId{i}, @PoLineNumber{i}, @MaterialCode{i}, @MaterialName{i}, @MaterialDescription{i}, @UomCode{i}, @Qty{i}, @UnitPrice{i}, @Total{i}, @OtherCharges{i}, @NetAmount{i}, @PrNumber{i}, @PrLine{i}, @Plant{i}, @TaxCode{i}, @DeliveryDate{i}, @PoCondition{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@PoLinesId{i}", row.PoLinesId);
                cmd.Parameters.AddWithValue($"@PoHeaderId{i}", row.PoHeaderId);
                cmd.Parameters.AddWithValue($"@PoLineNumber{i}", row.PoLineNumber);
                cmd.Parameters.AddWithValue($"@MaterialCode{i}", row.MaterialCode);
                cmd.Parameters.AddWithValue($"@MaterialName{i}", row.MaterialName);
                cmd.Parameters.AddWithValue($"@MaterialDescription{i}", row.MaterialDescription);
                cmd.Parameters.AddWithValue($"@UomCode{i}", row.UomCode);
                cmd.Parameters.AddWithValue($"@Qty{i}", (object?)row.Qty ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@UnitPrice{i}", (object?)row.UnitPrice ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@Total{i}", (object?)row.Total ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@OtherCharges{i}", row.OtherCharges);
                cmd.Parameters.AddWithValue($"@NetAmount{i}", (object?)row.NetAmount ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@PrNumber{i}", row.PrNumber);
                cmd.Parameters.AddWithValue($"@PrLine{i}", row.PrLine);
                cmd.Parameters.AddWithValue($"@Plant{i}", row.Plant);
                cmd.Parameters.AddWithValue($"@TaxCode{i}", row.TaxCode);
                cmd.Parameters.AddWithValue($"@DeliveryDate{i}", (object?)row.DeliveryDate ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@PoCondition{i}", row.PoCondition);
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
            public int POSubId { get; set; }
            public int? POId { get; set; }
            public string? SAP_Item_Code { get; set; }
            public string? ItemCode { get; set; }
            public string? ItemDescription { get; set; }
            public string? ItemLongDescription { get; set; }
            public string? UOM { get; set; }
            public decimal? Qty { get; set; }
            public decimal? Rate { get; set; }
            public decimal? Amount { get; set; }
            public decimal? OtherCharges { get; set; }
            public decimal? NetAmount { get; set; }
            public string? PRNo { get; set; }
            public string? PRItemNO { get; set; }
            public string? Plant { get; set; }
            public string? TaxCode { get; set; }
            public DateTime? DeliveryDate { get; set; }
            public string? POCondition { get; set; }
        }

        private class TargetRow
        {
            public int PoLinesId { get; set; }
            public int PoHeaderId { get; set; } // NOT NULL
            public string PoLineNumber { get; set; } = ""; // NOT NULL
            public string MaterialCode { get; set; } = ""; // NOT NULL
            public string MaterialName { get; set; } = ""; // NOT NULL
            public string MaterialDescription { get; set; } = ""; // NOT NULL
            public string UomCode { get; set; } = ""; // NOT NULL
            public decimal? Qty { get; set; }
            public decimal? UnitPrice { get; set; }
            public decimal? Total { get; set; }
            public string OtherCharges { get; set; } = ""; // NOT NULL
            public decimal? NetAmount { get; set; }
            public string PrNumber { get; set; } = ""; // NOT NULL
            public string PrLine { get; set; } = ""; // NOT NULL
            public string Plant { get; set; } = ""; // NOT NULL
            public string TaxCode { get; set; } = ""; // NOT NULL
            public DateTime? DeliveryDate { get; set; }
            public string PoCondition { get; set; } = ""; // NOT NULL
        }
    }
}
