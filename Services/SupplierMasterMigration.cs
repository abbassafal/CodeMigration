using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Threading;
using DataMigration.Services;

public class SupplierMasterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000; // Process in batches of 1000 records
    private const int PROGRESS_UPDATE_INTERVAL = 100; // Update progress every 100 records

    // SQL Server: TBL_VENDORMASTERNEW -> PostgreSQL: supplier_master
    protected override string SelectQuery => @"
        SELECT 
            VendorID,
            VendorCode,
            VendorName,
            VendorType,
            Primary_Contact_Person,
            PANNo,
            GSTNo,
            ZipCode,
            OfficeAddress,
            Primary_Phone,
            MobileTelephoneNumber,
            Primary_Email,
            OfficeCountryID,
            OfficeCity,
            StateID,
            ClientSAPId,
            VendorGroupId,
            StatusVendor
        FROM TBL_VENDORMASTERNEW
        ORDER BY VendorID";

    protected override string InsertQuery => @"
        INSERT INTO supplier_master (
            supplier_id,
            supplier_code,
            supplier_name,
            supplier_type,
            contact_name,
            pan_card_number,
            gst_number,
            zip_code,
            officeaddress,
            mobile_number1,
            mobile_number2,
            primary_email_id,
            office_country_id,
            office_city,
            office_state,
            company_id,
            supplier_group_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date,
            status
        ) VALUES (
            @supplier_id,
            @supplier_code,
            @supplier_name,
            @supplier_type,
            @contact_name,
            @pan_card_number,
            @gst_number,
            @zip_code,
            @officeaddress,
            @mobile_number1,
            @mobile_number2,
            @primary_email_id,
            @office_country_id,
            @office_city,
            @office_state,
            @company_id,
            @supplier_group_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date,
            @status
        )";

    // Optimized batch insert query
    private readonly string BatchInsertQuery = @"
        INSERT INTO supplier_master (
            supplier_id,
            supplier_code,
            supplier_name,
            supplier_type,
            contact_name,
            pan_card_number,
            gst_number,
            zip_code,
            officeaddress,
            mobile_number1,
            mobile_number2,
            primary_email_id,
            office_country_id,
            office_city,
            office_state,
            company_id,
            supplier_group_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date,
            status
        ) VALUES ";

    public SupplierMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "VendorID -> supplier_id (Direct)",
            "VendorCode -> supplier_code (Direct)",
            "VendorName -> supplier_name (Direct)",
            "VendorType -> supplier_type (Direct)",
            "Primary_Contact_Person -> contact_name (Direct)",
            "PANNo -> pan_card_number (Direct)",
            "GSTNo -> gst_number (Direct)",
            "ZipCode -> zip_code (Direct)",
            "OfficeAddress -> officeaddress (Direct)",
            "Primary_Phone -> mobile_number1 (Direct)",
            "MobileTelephoneNumber -> mobile_number2 (Direct)",
            "Primary_Email -> primary_email_id (Direct)",
            "OfficeCountryID -> office_country_id (Direct, defaults to 1 if NULL)",
            "OfficeCity -> office_city (Direct)",
            "StateID -> office_state (Convert to text)",
            "ClientSAPId -> company_id (FK to company_master, defaults to 1 if NULL)",
            "VendorGroupId -> supplier_group_id (FK to supplier_groupmaster)",
            "StatusVendor -> status (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "VendorID", logic = "VendorID -> supplier_id (Direct)", target = "supplier_id" },
            new { source = "VendorCode", logic = "VendorCode -> supplier_code (Direct)", target = "supplier_code" },
            new { source = "VendorName", logic = "VendorName -> supplier_name (Direct)", target = "supplier_name" },
            new { source = "VendorType", logic = "VendorType -> supplier_type (Direct)", target = "supplier_type" },
            new { source = "Primary_Contact_Person", logic = "Primary_Contact_Person -> contact_name (Direct)", target = "contact_name" },
            new { source = "PANNo", logic = "PANNo -> pan_card_number (Direct)", target = "pan_card_number" },
            new { source = "GSTNo", logic = "GSTNo -> gst_number (Direct)", target = "gst_number" },
            new { source = "ZipCode", logic = "ZipCode -> zip_code (Direct)", target = "zip_code" },
            new { source = "OfficeAddress", logic = "OfficeAddress -> officeaddress (Direct)", target = "officeaddress" },
            new { source = "Primary_Phone", logic = "Primary_Phone -> mobile_number1 (Direct)", target = "mobile_number1" },
            new { source = "MobileTelephoneNumber", logic = "MobileTelephoneNumber -> mobile_number2 (Direct)", target = "mobile_number2" },
            new { source = "Primary_Email", logic = "Primary_Email -> primary_email_id (Direct)", target = "primary_email_id" },
            new { source = "OfficeCountryID", logic = "OfficeCountryID -> office_country_id (Direct, defaults to 1 if NULL)", target = "office_country_id" },
            new { source = "OfficeCity", logic = "OfficeCity -> office_city (Direct)", target = "office_city" },
            new { source = "StateID", logic = "StateID -> office_state (Convert to text)", target = "office_state" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (FK to company_master, defaults to 1 if NULL)", target = "company_id" },
            new { source = "VendorGroupId", logic = "VendorGroupId -> supplier_group_id (FK to supplier_groupmaster)", target = "supplier_group_id" },
            new { source = "StatusVendor", logic = "StatusVendor -> status (Direct)", target = "status" },
            new { source = "-", logic = "created_by -> 0 (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await MigrateAsync(useTransaction: true);
    }

    public async Task<int> MigrateAsync(IMigrationProgress? progress = null)
    {
        return await MigrateAsync(useTransaction: true, progress);
    }

    public async Task<int> MigrateAsync(bool useTransaction, IMigrationProgress? progress = null)
    {
        progress ??= new ConsoleMigrationProgress();
        
        SqlConnection? sqlConn = null;
        NpgsqlConnection? pgConn = null;
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            sqlConn = GetSqlServerConnection();
            pgConn = GetPostgreSqlConnection();
            
            // Configure SQL connection for large datasets
            sqlConn.ConnectionString = sqlConn.ConnectionString + ";Connection Timeout=300;Command Timeout=300;";
            
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            progress.ReportProgress(0, 0, "Estimating total records...", stopwatch.Elapsed);
            
            // Get total count first for progress reporting
            int totalRecords = await GetTotalRecordsAsync(sqlConn);
            
            if (useTransaction)
            {
                using var transaction = await pgConn.BeginTransactionAsync();
                try
                {
                    int result = await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
                    await transaction.CommitAsync();
                    return result;
                }
                catch (Exception)
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            else
            {
                return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch);
            }
        }
        finally
        {
            sqlConn?.Dispose();
            pgConn?.Dispose();
        }
    }

    private async Task<int> GetTotalRecordsAsync(SqlConnection sqlConn)
    {
        using var cmd = new SqlCommand("SELECT COUNT(*) FROM TBL_VENDORMASTERNEW", sqlConn);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task<int> ExecuteOptimizedMigrationAsync(
        SqlConnection sqlConn, 
        NpgsqlConnection pgConn, 
        int totalRecords, 
        IMigrationProgress progress, 
        Stopwatch stopwatch, 
        NpgsqlTransaction? transaction = null)
    {
        var insertedCount = 0;
        var processedCount = 0;
        var skippedCount = 0;
        var batch = new List<SupplierRecord>();

        progress.ReportProgress(0, totalRecords, "Starting SupplierMaster migration...", stopwatch.Elapsed);

        // Use streaming reader for memory efficiency
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        sqlCmd.CommandTimeout = 300; // 5 minutes timeout
        
        using var reader = await sqlCmd.ExecuteReaderAsync();

        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                
                try
                {
                    var record = ReadSupplierRecord(reader, processedCount);
                    
                    if (record != null)
                    {
                        batch.Add(record);
                    }
                    else
                    {
                        skippedCount++;
                    }

                    // Process batch when it reaches the batch size or it's the last record
                    if (batch.Count >= BATCH_SIZE || processedCount == totalRecords)
                    {
                        if (batch.Count > 0)
                        {
                            progress.ReportProgress(processedCount, totalRecords, 
                                $"Processing batch of {batch.Count} records...", stopwatch.Elapsed);
                            
                            int batchInserted = await InsertBatchAsync(pgConn, batch, transaction);
                            insertedCount += batchInserted;
                            
                            batch.Clear();
                        }
                    }

                    // Update progress periodically
                    if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
                    {
                        progress.ReportProgress(processedCount, totalRecords, 
                            $"Processed: {processedCount:N0}, Inserted: {insertedCount:N0}, Skipped: {skippedCount:N0}", 
                            stopwatch.Elapsed);
                    }
                }
                catch (Exception recordEx)
                {
                    progress.ReportError($"Error processing record {processedCount}: {recordEx.Message}", processedCount);
                    throw new Exception($"Error processing record {processedCount} in SupplierMaster Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception ex)
        {
            progress.ReportError($"Migration failed after processing {processedCount} records: {ex.Message}", processedCount);
            throw;
        }

        stopwatch.Stop();
        progress.ReportCompleted(processedCount, insertedCount, stopwatch.Elapsed);
        
        return insertedCount;
    }

    private SupplierRecord? ReadSupplierRecord(SqlDataReader reader, int recordNumber)
    {
        try
        {
            var vendorId = reader.IsDBNull(reader.GetOrdinal("VendorID")) ? 0 : Convert.ToInt32(reader["VendorID"]);
            var vendorCode = reader.IsDBNull(reader.GetOrdinal("VendorCode")) ? "" : reader["VendorCode"].ToString();
            var vendorName = reader.IsDBNull(reader.GetOrdinal("VendorName")) ? "" : reader["VendorName"].ToString();
            // VendorType should show StatusVendor value
            var vendorType = reader.IsDBNull(reader.GetOrdinal("StatusVendor")) ? "" : reader["StatusVendor"].ToString();
            var contactPerson = reader.IsDBNull(reader.GetOrdinal("Primary_Contact_Person")) ? "" : reader["Primary_Contact_Person"].ToString();
            var panNo = reader.IsDBNull(reader.GetOrdinal("PANNo")) ? "" : reader["PANNo"].ToString();
            var gstNo = reader.IsDBNull(reader.GetOrdinal("GSTNo")) ? "" : reader["GSTNo"].ToString();
            var zipCode = reader.IsDBNull(reader.GetOrdinal("ZipCode")) ? "" : reader["ZipCode"].ToString();
            var officeAddress = reader.IsDBNull(reader.GetOrdinal("OfficeAddress")) ? "" : reader["OfficeAddress"].ToString();
            var primaryPhone = reader.IsDBNull(reader.GetOrdinal("Primary_Phone")) ? "" : reader["Primary_Phone"].ToString();
            var mobilePhone = reader.IsDBNull(reader.GetOrdinal("MobileTelephoneNumber")) ? "" : reader["MobileTelephoneNumber"].ToString();
            var primaryEmail = reader.IsDBNull(reader.GetOrdinal("Primary_Email")) ? "" : reader["Primary_Email"].ToString();
            var officeCountryId = reader.IsDBNull(reader.GetOrdinal("OfficeCountryID")) ? 1 : Convert.ToInt32(reader["OfficeCountryID"]); // Default to India (1)
            var officeCity = reader.IsDBNull(reader.GetOrdinal("OfficeCity")) ? "" : reader["OfficeCity"].ToString();
            var stateId = reader.IsDBNull(reader.GetOrdinal("StateID")) ? "" : reader["StateID"].ToString();
            var clientSapId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 1 : Convert.ToInt32(reader["ClientSAPId"]);
            if (clientSapId == 0)
            {
                clientSapId = 1;
            }
            var vendorGroupId = reader.IsDBNull(reader.GetOrdinal("VendorGroupId")) ? (int?)null : Convert.ToInt32(reader["VendorGroupId"]);
            // Status field should always be 'Active'
            var statusVendor = "Active";

            // Validate required fields
            if (vendorId == 0)
            {
                Console.WriteLine($"Skipping record {recordNumber} - Missing required VendorID");
                return null;
            }

            return new SupplierRecord
            {
                SupplierId = vendorId,
                SupplierCode = vendorCode ?? "",
                SupplierName = vendorName ?? "",
                SupplierType = vendorType ?? "", // Now shows StatusVendor value
                ContactName = contactPerson ?? "",
                PanCardNumber = panNo ?? "",
                GstNumber = gstNo ?? "",
                ZipCode = zipCode ?? "",
                OfficeAddress = officeAddress ?? "",
                MobileNumber1 = primaryPhone ?? "",
                MobileNumber2 = mobilePhone ?? "",
                PrimaryEmailId = primaryEmail ?? "",
                OfficeCountryId = officeCountryId,
                OfficeCity = officeCity ?? "",
                OfficeState = stateId ?? "",
                CompanyId = clientSapId,
                SupplierGroupId = vendorGroupId,
                Status = statusVendor // Always 'Active'
            };
        }
        catch (Exception ex)
        {
            throw new Exception($"Error reading supplier record {recordNumber}: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<SupplierRecord> batch, NpgsqlTransaction? transaction = null)
    {
        if (batch.Count == 0) return 0;

        try
        {
            // Use COPY for maximum performance with large batches
            if (batch.Count >= 100)
            {
                return await InsertBatchWithCopyAsync(pgConn, batch, transaction);
            }
            else
            {
                return await InsertBatchWithParametersAsync(pgConn, batch, transaction);
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Error inserting batch of {batch.Count} records: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchWithCopyAsync(NpgsqlConnection pgConn, List<SupplierRecord> batch, NpgsqlTransaction? transaction = null)
    {
        var copyCommand = $"COPY supplier_master (supplier_id, supplier_code, supplier_name, supplier_type, contact_name, pan_card_number, gst_number, zip_code, officeaddress, mobile_number1, mobile_number2, primary_email_id, office_country_id, office_city, office_state, company_id, supplier_group_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date, status) FROM STDIN (FORMAT BINARY)";
        
        using var writer = transaction != null ? 
            await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None) : 
            await pgConn.BeginBinaryImportAsync(copyCommand);
        
        var now = DateTime.UtcNow;
        foreach (var record in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(record.SupplierId);
            await writer.WriteAsync(record.SupplierCode);
            await writer.WriteAsync(record.SupplierName);
            await writer.WriteAsync(record.SupplierType);
            await writer.WriteAsync(record.ContactName);
            await writer.WriteAsync(record.PanCardNumber);
            await writer.WriteAsync(record.GstNumber);
            await writer.WriteAsync(record.ZipCode);
            await writer.WriteAsync(record.OfficeAddress);
            await writer.WriteAsync(record.MobileNumber1);
            await writer.WriteAsync(record.MobileNumber2);
            await writer.WriteAsync(record.PrimaryEmailId);
            await writer.WriteAsync(record.OfficeCountryId);
            await writer.WriteAsync(record.OfficeCity);
            await writer.WriteAsync(record.OfficeState);
            await writer.WriteAsync(record.CompanyId);
            await writer.WriteAsync(record.SupplierGroupId.HasValue ? (object)record.SupplierGroupId.Value : DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
            await writer.WriteAsync(0); // created_by
            await writer.WriteAsync(now); // created_date
            await writer.WriteAsync(DBNull.Value); // modified_by
            await writer.WriteAsync(DBNull.Value); // modified_date
            await writer.WriteAsync(false); // is_deleted
            await writer.WriteAsync(DBNull.Value); // deleted_by
            await writer.WriteAsync(DBNull.Value); // deleted_date
            await writer.WriteAsync(record.Status);
        }
        
        await writer.CompleteAsync();
        return batch.Count;
    }

    private async Task<int> InsertBatchWithParametersAsync(NpgsqlConnection pgConn, List<SupplierRecord> batch, NpgsqlTransaction? transaction = null)
    {
        // Build multi-row insert statement
        var values = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        var now = DateTime.UtcNow;

        for (int i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            var paramPrefix = $"p{i}";
            
            values.Add($"(@{paramPrefix}_supplier_id, @{paramPrefix}_supplier_code, @{paramPrefix}_supplier_name, @{paramPrefix}_supplier_type, @{paramPrefix}_contact_name, @{paramPrefix}_pan_card_number, @{paramPrefix}_gst_number, @{paramPrefix}_zip_code, @{paramPrefix}_officeaddress, @{paramPrefix}_mobile_number1, @{paramPrefix}_mobile_number2, @{paramPrefix}_primary_email_id, @{paramPrefix}_office_country_id, @{paramPrefix}_office_city, @{paramPrefix}_office_state, @{paramPrefix}_company_id, @{paramPrefix}_supplier_group_id, @{paramPrefix}_created_by, @{paramPrefix}_created_date, @{paramPrefix}_modified_by, @{paramPrefix}_modified_date, @{paramPrefix}_is_deleted, @{paramPrefix}_deleted_by, @{paramPrefix}_deleted_date, @{paramPrefix}_status)");
            
            parameters.AddRange(new[]
            {
                new NpgsqlParameter($"@{paramPrefix}_supplier_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.SupplierId },
                new NpgsqlParameter($"@{paramPrefix}_supplier_code", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.SupplierCode },
                new NpgsqlParameter($"@{paramPrefix}_supplier_name", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.SupplierName },
                new NpgsqlParameter($"@{paramPrefix}_supplier_type", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.Status },
                new NpgsqlParameter($"@{paramPrefix}_contact_name", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.ContactName },
                new NpgsqlParameter($"@{paramPrefix}_pan_card_number", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.PanCardNumber },
                new NpgsqlParameter($"@{paramPrefix}_gst_number", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.GstNumber },
                new NpgsqlParameter($"@{paramPrefix}_zip_code", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.ZipCode },
                new NpgsqlParameter($"@{paramPrefix}_officeaddress", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.OfficeAddress },
                new NpgsqlParameter($"@{paramPrefix}_mobile_number1", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.MobileNumber1 },
                new NpgsqlParameter($"@{paramPrefix}_mobile_number2", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.MobileNumber2 },
                new NpgsqlParameter($"@{paramPrefix}_primary_email_id", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.PrimaryEmailId },
                new NpgsqlParameter($"@{paramPrefix}_office_country_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.OfficeCountryId },
                new NpgsqlParameter($"@{paramPrefix}_office_city", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.OfficeCity },
                new NpgsqlParameter($"@{paramPrefix}_office_state", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.OfficeState },
                new NpgsqlParameter($"@{paramPrefix}_company_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.CompanyId },
                new NpgsqlParameter($"@{paramPrefix}_supplier_group_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.SupplierGroupId.HasValue ? record.SupplierGroupId.Value : DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_created_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = 0 },
                new NpgsqlParameter($"@{paramPrefix}_created_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = now },
                new NpgsqlParameter($"@{paramPrefix}_modified_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_modified_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_is_deleted", NpgsqlTypes.NpgsqlDbType.Boolean) { Value = false },
                new NpgsqlParameter($"@{paramPrefix}_deleted_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_deleted_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_status", NpgsqlTypes.NpgsqlDbType.Text) { Value = "Active" }
            });
        }

        var query = BatchInsertQuery + string.Join(", ", values);
        
        using var cmd = new NpgsqlCommand(query, pgConn);
        if (transaction != null)
        {
            cmd.Transaction = transaction;
        }
        
        cmd.Parameters.AddRange(parameters.ToArray());
        cmd.CommandTimeout = 300; // 5 minutes timeout
        
        return await cmd.ExecuteNonQueryAsync();
    }

    // Keep the original method for backward compatibility
    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        var progress = new ConsoleMigrationProgress();
        var stopwatch = Stopwatch.StartNew();
        
        // Get total records count
        int totalRecords = await GetTotalRecordsAsync(sqlConn);
        
        return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
    }

    private class SupplierRecord
    {
        public int SupplierId { get; set; }
        public string SupplierCode { get; set; } = "";
        public string SupplierName { get; set; } = "";
        public string SupplierType { get; set; } = "";
        public string ContactName { get; set; } = "";
        public string PanCardNumber { get; set; } = "";
        public string GstNumber { get; set; } = "";
        public string ZipCode { get; set; } = "";
        public string OfficeAddress { get; set; } = "";
        public string MobileNumber1 { get; set; } = "";
        public string MobileNumber2 { get; set; } = "";
        public string PrimaryEmailId { get; set; } = "";
        public int OfficeCountryId { get; set; }
        public string OfficeCity { get; set; } = "";
        public string OfficeState { get; set; } = "";
        public int CompanyId { get; set; }
        public int? SupplierGroupId { get; set; }
        public string Status { get; set; } = "";
    }
}
