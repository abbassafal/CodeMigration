// Optimized UsersMasterMigration service

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Helper;
using Helpers;
using System.Diagnostics;
using System.Threading;
using DataMigration.Services;
using System.Data;
using System.Collections.Concurrent;
using NpgsqlTypes;

public class UsersMasterMigration : MigrationService
{
    private readonly AesEncryptionService _aesEncryptionService;
    private readonly IConfiguration _configuration;
    private readonly bool _fastMode;
    private readonly int _hashIterations;
    private readonly int _transformWorkerCount;
    private readonly int _rawQueueCapacity;
    private readonly int _writeQueueCapacity;
    private const int INITIAL_BATCH_SIZE = 1000;

    private const int PROGRESS_UPDATE_INTERVAL = 100;

    // User type to role mapping (SQL Server USERTYPE_ID -> PostgreSQL role_id)
    private static readonly Dictionary<int, int> UserTypeToRoleMapping = new Dictionary<int, int>
    {
        { 1, 1 },  // Admin -> Admin
        { 2, 2 },  // Buyer -> Buyer
        { 3, 3 },  // Supplier -> Supplier
        { 6, 4 },  // HOD -> HOD
        { 5, 5 }   // Technical -> Technical
    };

    // User type ID to text mapping
    private static readonly Dictionary<int, string> UserTypeToTextMapping = new Dictionary<int, string>
    {
        { 1, "Admin" },
        { 2, "Buyer" },
        { 3, "Supplier" },
        { 6, "HOD" },
        { 5, "Technical" }
    };

    // Currency mapping (loaded dynamically from SQL Server)
    private Dictionary<int, string> _currencyMapping = new Dictionary<int, string>();

    private static int MapUserTypeToRoleId(int userTypeId)
    {
        return UserTypeToRoleMapping.TryGetValue(userTypeId, out var roleId) ? roleId : 0;
    }

    private static string MapUserTypeToText(int userTypeId)
    {
        return UserTypeToTextMapping.TryGetValue(userTypeId, out var text) ? text : "Unknown";
    }

    private string MapCurrencyIdToCode(int currencyId)
    {
        return _currencyMapping.TryGetValue(currencyId, out var code) ? code : "";
    }

    private async Task LoadCurrencyMappingAsync(NpgsqlConnection pgConn)
    {
        _currencyMapping.Clear();
        using var cmd = new NpgsqlCommand("SELECT currency_id, currency_code FROM currency_master", pgConn);
        using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32(0);
            var code = reader.GetString(1);
            _currencyMapping[id] = code;
            count++;
        }
        Console.WriteLine($"Loaded {count} currency mappings from PostgreSQL currency_master table");
    }

    protected override string SelectQuery => @"
        SELECT 
            PERSON_ID, USER_ID, USERPASSWORD, FULL_NAME, EMAIL_ADDRESS, MobileNumber, STATUS,REPORTINGTO,
            USERTYPE_ID, CURRENCYID, TIMEZONE, USER_SAP_ID, DEPARTMENTHEAD, DigitalSignature
        FROM TBL_USERMASTERFINAL";

    protected override string InsertQuery => @"
        INSERT INTO users (
            user_id, username, password_hash, full_name, email, mobile_number, status,
            password_salt, masked_email, masked_mobile_number, email_hash, mobile_hash, failed_login_attempts,
            last_failed_login, lockout_end, last_login_date, is_mfa_enabled, mfa_type, mfa_secret, last_mfa_sent_at,
            reporting_to_id, lockout_count, azureoid, user_type, currency, location, client_sap_code,
            digital_signature, last_password_changed, is_active, created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date, erp_username, approval_head, time_zone_country, digital_signature_path
        ) VALUES (
            @user_id, @username, @password_hash, @full_name, @email, @mobile_number, @status,
            @password_salt, @masked_email, @masked_mobile_number, @email_hash, @mobile_hash, @failed_login_attempts,
            @last_failed_login, @lockout_end, @last_login_date, @is_mfa_enabled, @mfa_type, @mfa_secret, @last_mfa_sent_at,
            @reporting_to_id, @lockout_count, @azureoid, @user_type, @currency, @location, @client_sap_code,
            @digital_signature, @last_password_changed, @is_active, @created_by, @created_date, @modified_by, @modified_date,
            @is_deleted, @deleted_by, @deleted_date, @erp_username, @approval_head, @time_zone_country, @digital_signature_path
        )";

    public UsersMasterMigration(IConfiguration configuration) : base(configuration)
    {
        _configuration = configuration;
        _aesEncryptionService = new AesEncryptionService();

        // Read migration tweaks from configuration; use fast defaults for faster migration
        _fastMode = _configuration.GetValue<bool?>("Migration:FastMode") ?? true;
        _hashIterations = _configuration.GetValue<int?>("Migration:Users:HashIterations") ?? (_fastMode ? 10000 : 1500000);
        _transformWorkerCount = _configuration.GetValue<int?>("Migration:Users:TransformWorkerCount") ?? Math.Max(1, Environment.ProcessorCount - 1);
        _rawQueueCapacity = _configuration.GetValue<int?>("Migration:Users:RawQueueCapacity") ?? Math.Max(1000, _transformWorkerCount * 2000);
        _writeQueueCapacity = _configuration.GetValue<int?>("Migration:Users:WriteQueueCapacity") ?? Math.Max(5000, _transformWorkerCount * 2000);
    }

    private async Task<int> GetTotalRecordsAsync(SqlConnection sqlConn)
    {
        using var cmd = new SqlCommand("SELECT COUNT(*) FROM TBL_USERMASTERFINAL", sqlConn);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<int> MigrateAsync(IMigrationProgress? progress = null)
    {
        progress ??= new ConsoleMigrationProgress();
        SqlConnection? sqlConn = null;
        NpgsqlConnection? pgConn = null;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            sqlConn = GetSqlServerConnection();
            pgConn = GetPostgreSqlConnection();
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            progress.ReportProgress(0, 0, "Loading currency mappings from PostgreSQL...", stopwatch.Elapsed);
            await LoadCurrencyMappingAsync(pgConn);

            progress.ReportProgress(0, 0, "Estimating total records...", stopwatch.Elapsed);
            int totalRecords = await GetTotalRecordsAsync(sqlConn);

            using var transaction = await pgConn.BeginTransactionAsync();
            try
            {
                int result = await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
                await transaction.CommitAsync();
                return result;
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                progress.ReportError($"Transaction rolled back due to error: {ex.Message}", 0);
                throw;
            }
        }
        finally
        {
            sqlConn?.Dispose();
            pgConn?.Dispose();
        }
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        var progress = new ConsoleMigrationProgress();
        var stopwatch = Stopwatch.StartNew();
        int totalRecords = await GetTotalRecordsAsync(sqlConn);
        return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
    }

    private class RawUserRow
    {
        public int PersonId { get; set; }
        public string UserId { get; set; } = "";
        public string RawPassword { get; set; } = "";
        public string FullName { get; set; } = "";
        public string Email { get; set; } = "";
        public string MobileNumber { get; set; } = "";
        public string Status { get; set; } = "";
        public string ReportingTo { get; set; } = "";
        public int UserTypeId { get; set; } // Store as int to map to role_id
        public string Currency { get; set; } = "";
        public string Location { get; set; } = "";
        public string ErpUsername { get; set; } = "";
        public string ApprovalHead { get; set; } = "";
        public string DigitalSignature { get; set; } = "";
    }

    private bool ValidateUserRecord(UserRecord record, out string errorMessage)
    {
        errorMessage = "";
        
        if (record == null)
        {
            errorMessage = "Record is null";
            return false;
        }
        
        if (record.UserId <= 0)
        {
            errorMessage = $"Invalid UserId: {record.UserId}";
            return false;
        }
        
        if (record.Username == null)
        {
            errorMessage = "Username is null";
            return false;
        }
        
        if (record.PasswordHash == null || record.PasswordSalt == null)
        {
            errorMessage = $"Password hash or salt is null (hash={record.PasswordHash?.Length ?? -1}, salt={record.PasswordSalt?.Length ?? -1})";
            return false;
        }
        
        // Ensure all string fields are not null (empty string is OK)
        if (record.FullName == null) record.FullName = string.Empty;
        if (record.Email == null) record.Email = string.Empty;
        if (record.MobileNumber == null) record.MobileNumber = string.Empty;
        if (record.Status == null) record.Status = string.Empty;
        if (record.MaskedEmail == null) record.MaskedEmail = string.Empty;
        if (record.MaskedMobileNumber == null) record.MaskedMobileNumber = string.Empty;
        if (record.EmailHash == null) record.EmailHash = string.Empty;
        if (record.MobileHash == null) record.MobileHash = string.Empty;
        if (record.ReportingToId == null) record.ReportingToId = string.Empty;
        if (record.UserType == null) record.UserType = string.Empty;
        if (record.Currency == null) record.Currency = string.Empty;
        if (record.Location == null) record.Location = string.Empty;
        if (record.ErpUsername == null) record.ErpUsername = string.Empty;
        if (record.ApprovalHead == null) record.ApprovalHead = string.Empty;
        if (record.DigitalSignature == null) record.DigitalSignature = string.Empty;
        if (record.DigitalSignaturePath == null) record.DigitalSignaturePath = string.Empty;
        
        return true;
    }

    private UserRecord? BuildUserRecordFromRaw(RawUserRow raw)
    {
        try
        {
            // Validate that raw is not null
            if (raw == null)
            {
                Console.WriteLine("BuildUserRecordFromRaw: raw parameter is null");
                return null;
            }

            // Check if critical services are initialized
            if (_aesEncryptionService == null)
            {
                Console.WriteLine($"BuildUserRecordFromRaw: AES service is null for user {raw.PersonId}");
                return null;
            }

            // Step 1: Hash password
            string passwordHash = "";
            string passwordSalt = "";
            try
            {
                // If password is empty, use a default placeholder to avoid exception
                var passwordToHash = string.IsNullOrEmpty(raw.RawPassword) ? "DefaultPassword123!" : raw.RawPassword;
                (passwordHash, passwordSalt) = PasswordEncryptionHelper.EncryptPassword(passwordToHash, _hashIterations);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error encrypting password for user {raw.PersonId}: {ex.Message}");
                throw;
            }

            // Step 2: Encrypt email
            string encryptedEmail = "";
            try
            {
                encryptedEmail = string.IsNullOrEmpty(raw.Email) ? string.Empty : _aesEncryptionService.Encrypt(raw.Email);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error encrypting email for user {raw.PersonId} (email: '{raw.Email}'): {ex.Message}");
                throw;
            }

            // Step 3: Encrypt mobile number
            string encryptedMobileNumber = "";
            try
            {
                encryptedMobileNumber = string.IsNullOrEmpty(raw.MobileNumber) ? string.Empty : _aesEncryptionService.Encrypt(raw.MobileNumber);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error encrypting mobile for user {raw.PersonId} (mobile: '{raw.MobileNumber}'): {ex.Message}");
                throw;
            }

            // Step 4: Compute hashes
            string emailHash = "";
            string mobileHash = "";
            try
            {
                emailHash = string.IsNullOrEmpty(raw.Email) ? string.Empty : AesEncryptionService.ComputeSha256Hash(raw.Email);
                mobileHash = string.IsNullOrEmpty(raw.MobileNumber) ? string.Empty : AesEncryptionService.ComputeSha256Hash(raw.MobileNumber);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error computing hashes for user {raw.PersonId}: {ex.Message}");
                throw;
            }

            // Step 5: Generate masked values
            string maskedEmail = "";
            string maskedMobile = "";
            try
            {
                maskedEmail = MaskHelper.MaskEmail(raw.Email ?? string.Empty);
                maskedMobile = MaskHelper.MaskPhoneNumber(raw.MobileNumber ?? string.Empty);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error masking fields for user {raw.PersonId}: {ex.Message}");
                throw;
            }

            // Step 6: Create record with explicit null-coalescing for all fields
            return new UserRecord
            {
                UserId = raw.PersonId,
                Username = raw.UserId ?? string.Empty,
                PasswordHash = passwordHash ?? string.Empty,
                PasswordSalt = passwordSalt ?? string.Empty,
                FullName = raw.FullName ?? string.Empty,
                Email = encryptedEmail ?? string.Empty,
                MobileNumber = encryptedMobileNumber ?? string.Empty,
                Status = raw.Status ?? string.Empty,
                MaskedEmail = maskedEmail ?? string.Empty,
                MaskedMobileNumber = maskedMobile ?? string.Empty,
                EmailHash = emailHash ?? string.Empty,
                MobileHash = mobileHash ?? string.Empty,
                ReportingToId = raw.ReportingTo ?? string.Empty,
                UserTypeId = raw.UserTypeId, // Store the original type ID for role mapping
                UserType = MapUserTypeToText(raw.UserTypeId) ?? string.Empty, // Convert to text for users table
                Currency = raw.Currency ?? string.Empty, // Will be mapped in reading phase
                Location = raw.Location ?? string.Empty,
                ErpUsername = raw.ErpUsername ?? string.Empty,
                ApprovalHead = raw.ApprovalHead ?? string.Empty,
                DigitalSignature = raw.DigitalSignature ?? string.Empty,
                DigitalSignaturePath = "/Documents/TechnicalDocuments/" + (raw.DigitalSignature ?? string.Empty)
            };
        }
        catch (Exception ex)
        {
            // log detailed error info
            Console.WriteLine($"Transform error for user {raw?.PersonId ?? -1}: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
            }
            return null;
        }
    }

    private async Task<int> ExecuteOptimizedMigrationAsync(
        SqlConnection sqlConn,
        NpgsqlConnection pgConn,
        int totalRecords,
        IMigrationProgress progress,
        Stopwatch stopwatch,
        NpgsqlTransaction? transaction = null)
    {
        int insertedCount = 0;
        int processedCount = 0;
        int skippedCount = 0;
        int errorCount = 0;

        // Heuristic for worker count
        int transformWorkerCount = Math.Max(1, _transformWorkerCount);
        int rawQueueCapacity = Math.Max(1000, _rawQueueCapacity);
        const int TRANSFORM_BATCH_SIZE = 500; // number of user records each transform worker accumulates
        int writeQueueCapacity = Math.Max(4, rawQueueCapacity / TRANSFORM_BATCH_SIZE);

        var rawQueue = new BlockingCollection<RawUserRow>(rawQueueCapacity);
        var writeQueue = new BlockingCollection<List<UserRecord>>(writeQueueCapacity);
        var userRolesList = new ConcurrentBag<(int userId, int roleId)>(); // Collect user-role mappings
        var listPool = new ConcurrentBag<List<UserRecord>>();
        // Pre-seed pool to avoid allocations
        for (int i = 0; i < Math.Max(4, transformWorkerCount * 2); i++)
        {
            listPool.Add(new List<UserRecord>(TRANSFORM_BATCH_SIZE));
        }
        var cts = new CancellationTokenSource();
        var token = cts.Token;

        // Diagnostic: Query actual table schema
        try
        {
            using var schemaCmd = pgConn.CreateCommand();
            schemaCmd.CommandText = @"
                SELECT column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_name = 'users' AND table_schema = 'public'
                ORDER BY ordinal_position";
            using var schemaReader = await schemaCmd.ExecuteReaderAsync();
            Console.WriteLine("\n=== PostgreSQL 'users' table schema ===");
            int colCount = 0;
            while (await schemaReader.ReadAsync())
            {
                colCount++;
                Console.WriteLine($"{schemaReader.GetInt32(2),3}. {schemaReader.GetString(0),-40} {schemaReader.GetString(1)}");
            }
            Console.WriteLine($"Total columns in PostgreSQL users table: {colCount}\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: Could not query schema: {ex.Message}");
        }

        // COPY command with required NOT NULL columns included
        var copyCommand = @"COPY users (
            user_id, username, password_hash, password_salt, full_name, 
            email, mobile_number, masked_email, masked_mobile_number,
            email_hash, mobile_hash, status, reporting_to_id, user_type,
            currency, erp_username, approval_head, time_zone_country,
            is_active, created_by, created_date
        ) FROM STDIN (FORMAT BINARY)";

        Exception? backgroundException = null;

        // Determine writer count - use single writer when in transaction to avoid connection conflicts
        int writerCount = transaction != null ? 1 : Math.Min(4, Math.Max(1, Environment.ProcessorCount / 2));

        var writerTasks = new List<Task>(writerCount);
        for (int w = 0; w < writerCount; w++)
        {
            writerTasks.Add(Task.Run(async () =>
            {
                NpgsqlConnection? writerConn = null;
                try
                {
                    bool isSharedConnection = transaction != null;
                    if (isSharedConnection)
                    {
                        writerConn = pgConn; // shared connection
                    }
                    else
                    {
                        writerConn = GetPostgreSqlConnection();
                        await writerConn.OpenAsync(token);
                    }

                    using var writer = await writerConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None);
                    var now = DateTime.UtcNow;

                    foreach (var batchList in writeQueue.GetConsumingEnumerable(token))
                    {
                        try
                        {
                            foreach (var record in batchList)
                            {
                                try
                                {
                                    writer.StartRow();
                                    writer.Write(record.UserId, NpgsqlDbType.Integer); // user_id
                                    writer.Write(record.Username ?? string.Empty, NpgsqlDbType.Text); // username
                                    writer.Write(record.PasswordHash ?? string.Empty, NpgsqlDbType.Text); // password_hash
                                    writer.Write(record.PasswordSalt ?? string.Empty, NpgsqlDbType.Text); // password_salt
                                    writer.Write(record.FullName ?? string.Empty, NpgsqlDbType.Text); // full_name
                                    writer.Write(record.Email ?? string.Empty, NpgsqlDbType.Text); // email
                                    writer.Write(record.MobileNumber ?? string.Empty, NpgsqlDbType.Text); // mobile_number
                                    writer.Write(record.MaskedEmail ?? string.Empty, NpgsqlDbType.Text); // masked_email
                                    writer.Write(record.MaskedMobileNumber ?? string.Empty, NpgsqlDbType.Text); // masked_mobile_number
                                    writer.Write(record.EmailHash ?? string.Empty, NpgsqlDbType.Text); // email_hash
                                    writer.Write(record.MobileHash ?? string.Empty, NpgsqlDbType.Text); // mobile_hash
                                    writer.Write(record.Status ?? string.Empty, NpgsqlDbType.Text); // status
                                    // reporting_to_id is INTEGER in PostgreSQL, convert from string
                                    int reportingToId = int.TryParse(record.ReportingToId, out var rtId) ? rtId : 0;
                                    writer.Write(reportingToId, NpgsqlDbType.Integer); // reporting_to_id
                                    writer.Write(record.UserType ?? string.Empty, NpgsqlDbType.Text); // user_type
                                    writer.Write(record.Currency ?? string.Empty, NpgsqlDbType.Text); // currency (mapped from currency_master)
                                    writer.Write(record.ErpUsername ?? string.Empty, NpgsqlDbType.Text); // erp_username
                                    // approval_head is INTEGER in PostgreSQL, convert from string
                                    int approvalHead = int.TryParse(record.ApprovalHead, out var ahId) ? ahId : 0;
                                    writer.Write(approvalHead, NpgsqlDbType.Integer); // approval_head
                                    writer.Write(record.Location ?? string.Empty, NpgsqlDbType.Text); // time_zone_country
                                    writer.Write(true, NpgsqlDbType.Boolean); // is_active
                                    writer.Write(0, NpgsqlDbType.Integer); // created_by
                                    writer.Write(now, NpgsqlDbType.TimestampTz); // created_date

                                    // Collect user-role mapping for later insertion
                                    int roleId = MapUserTypeToRoleId(record.UserTypeId);
                                    if (roleId > 0)
                                    {
                                        userRolesList.Add((record.UserId, roleId));
                                    }
                                }
                                catch (Exception ex)
                                {
                                    // Log which specific record failed
                                    Console.WriteLine($"Error writing user {record.UserId} ({record.Username}): {ex.Message}");
                                    Console.WriteLine($"  UserId: {record.UserId}");
                                    Console.WriteLine($"  Username: {record.Username ?? "NULL"}");
                                    Console.WriteLine($"  PasswordHash length: {record.PasswordHash?.Length ?? 0}");
                                    Console.WriteLine($"  PasswordSalt length: {record.PasswordSalt?.Length ?? 0}");
                                    Console.WriteLine($"  Email length: {record.Email?.Length ?? 0}");
                                    Console.WriteLine($"  MobileNumber length: {record.MobileNumber?.Length ?? 0}");
                                    throw;
                                }
                            }

                            Interlocked.Add(ref insertedCount, batchList.Count);

                            // recycle buffer list back into pool for reuse
                            batchList.Clear();
                            listPool.Add(batchList);
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref errorCount);
                            progress.ReportError($"Error writing record chunk to PostgreSQL: {ex.Message}", processedCount);
                            cts.Cancel();
                            throw;
                        }
                    }

                    await writer.CompleteAsync();
                }
                catch (Exception ex)
                {
                    backgroundException = ex;
                    cts.Cancel();
                }
                finally
                {
                    if (writerConn != null && writerConn != pgConn)
                    {
                        try
                        {
                            writerConn.Close();
                            writerConn.Dispose();
                        }
                        catch { }
                    }
                }
            }, token));
        }

        // Start transformation worker pool
        var transformTasks = new List<Task>();
        for (int w = 0; w < transformWorkerCount; w++)
        {
            transformTasks.Add(Task.Run(() =>
            {
                try
                {
                    var localBatch = listPool.TryTake(out var pooledList) ? pooledList : new List<UserRecord>(TRANSFORM_BATCH_SIZE);
                    foreach (var raw in rawQueue.GetConsumingEnumerable(token))
                    {
                        if (token.IsCancellationRequested) break;
                        try
                        {
                            var userRecord = BuildUserRecordFromRaw(raw);
                            if (userRecord != null)
                            {
                                // Validate the record before adding to batch
                                if (ValidateUserRecord(userRecord, out string validationError))
                                {
                                    localBatch.Add(userRecord);
                                    if (localBatch.Count >= TRANSFORM_BATCH_SIZE)
                                    {
                                        writeQueue.Add(localBatch, token);
                                        localBatch = listPool.TryTake(out pooledList) ? pooledList : new List<UserRecord>(TRANSFORM_BATCH_SIZE);
                                    }
                                }
                                else
                                {
                                    Interlocked.Increment(ref skippedCount);
                                    Console.WriteLine($"Validation failed for user PersonId={raw.PersonId}, UserId={raw.UserId ?? "null"}: {validationError}");
                                }
                            }
                            else
                            {
                                Interlocked.Increment(ref skippedCount);
                                Console.WriteLine($"Skipped user: PersonId={raw.PersonId}, UserId={raw.UserId ?? "null"}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref errorCount);
                            var errorMsg = $"Error transforming user PersonId={raw?.PersonId ?? -1}, UserId={raw?.UserId ?? "null"}: {ex.Message}";
                            Console.WriteLine(errorMsg);
                            progress.ReportError(errorMsg, Interlocked.CompareExchange(ref processedCount, 0, 0));
                        }
                    }

                    if (localBatch.Count > 0)
                    {
                        writeQueue.Add(localBatch, token);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // ignore
                }
                catch (Exception ex)
                {
                    backgroundException = ex;
                    cts.Cancel();
                }
            }, token));
        }

        progress.ReportProgress(0, totalRecords, "Starting migration (parallel transform + single-writer COPY)...", stopwatch.Elapsed);

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        sqlCmd.CommandTimeout = 300;
        using var reader = await sqlCmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        try
        {
            // cache ordinals once for performance
            var ordPersonId = reader.GetOrdinal("PERSON_ID");
            var ordUserId = reader.GetOrdinal("USER_ID");
            var ordPassword = reader.GetOrdinal("USERPASSWORD");
            var ordFullName = reader.GetOrdinal("FULL_NAME");
            var ordEmail = reader.GetOrdinal("EMAIL_ADDRESS");
            var ordMobile = reader.GetOrdinal("MobileNumber");
            var ordStatus = reader.GetOrdinal("STATUS");
            var ordReporting = reader.GetOrdinal("REPORTINGTO");
            var ordUserType = reader.GetOrdinal("USERTYPE_ID");
            var ordCurrency = reader.GetOrdinal("CURRENCYID");
            var ordLocation = reader.GetOrdinal("TIMEZONE");
            var ordErp = reader.GetOrdinal("USER_SAP_ID");
            var ordApproval = reader.GetOrdinal("DEPARTMENTHEAD");
            var ordDigital = reader.GetOrdinal("DigitalSignature");

            while (await reader.ReadAsync())
            {
                if (token.IsCancellationRequested) break;

                Interlocked.Increment(ref processedCount);
                try
                {
                    var raw = new RawUserRow
                    {
                        PersonId = reader.IsDBNull(ordPersonId) ? 0 : reader.GetInt32(ordPersonId),
                        UserId = reader.IsDBNull(ordUserId) ? string.Empty : reader.GetString(ordUserId),
                        RawPassword = reader.IsDBNull(ordPassword) ? string.Empty : reader.GetString(ordPassword),
                        FullName = reader.IsDBNull(ordFullName) ? string.Empty : reader.GetString(ordFullName),
                        Email = reader.IsDBNull(ordEmail) ? string.Empty : reader.GetString(ordEmail),
                        MobileNumber = reader.IsDBNull(ordMobile) ? string.Empty : reader.GetString(ordMobile),
                        Status = reader.IsDBNull(ordStatus) ? string.Empty : reader.GetString(ordStatus),
                        ReportingTo = reader.IsDBNull(ordReporting) ? "0" : reader.GetInt32(ordReporting).ToString(),
                        UserTypeId = reader.IsDBNull(ordUserType) ? 0 : reader.GetInt32(ordUserType),
                        Currency = reader.IsDBNull(ordCurrency) ? "" : MapCurrencyIdToCode(reader.GetInt32(ordCurrency)),
                        Location = reader.IsDBNull(ordLocation) ? string.Empty : reader.GetString(ordLocation),
                        ErpUsername = reader.IsDBNull(ordErp) ? string.Empty : reader.GetString(ordErp),
                        ApprovalHead = reader.IsDBNull(ordApproval) ? "0" : reader.GetInt32(ordApproval).ToString(),
                        DigitalSignature = reader.IsDBNull(ordDigital) ? string.Empty : reader.GetString(ordDigital)
                    };

                    rawQueue.Add(raw, token);

                    if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
                    {
                        progress.ReportProgress(processedCount, totalRecords, $"Queued: {processedCount:N0}, Inserted: {insertedCount:N0}, Skipped: {skippedCount:N0}, Errors: {errorCount:N0}", stopwatch.Elapsed);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref errorCount);
                    progress.ReportError($"Error reading raw record {processedCount}: {ex.Message}", processedCount);
                    // optionally continue
                }
            }

            // Signal no more raw items
            rawQueue.CompleteAdding();

            // Wait for transforms to finish
            await Task.WhenAll(transformTasks);

            // Signal writer no more items
            writeQueue.CompleteAdding();

            // Wait for all writer tasks to finish
            await Task.WhenAll(writerTasks);

            if (backgroundException != null)
            {
                throw backgroundException;
            }

            // After users are migrated, insert user_roles
            progress.ReportProgress(processedCount, totalRecords, "Inserting user roles...", stopwatch.Elapsed);
            int rolesInserted = await InsertUserRolesAsync(pgConn, userRolesList, transaction);
            progress.ReportProgress(processedCount, totalRecords, $"User roles inserted: {rolesInserted}", stopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            cts.Cancel();
            progress.ReportError($"Migration failed after processing {processedCount} records: {ex.Message}", processedCount);
            throw;
        }
        finally
        {
            rawQueue.Dispose();
            writeQueue.Dispose();
            cts.Dispose();
        }

        stopwatch.Stop();
        progress.ReportCompleted(processedCount, insertedCount, stopwatch.Elapsed);
        return insertedCount;
    }

    private int AdjustBatchSize(int currentBatchSize, int recordsInserted)
    {
        if (recordsInserted < currentBatchSize * 0.8)
        {
            return Math.Max(currentBatchSize / 2, 100); // Reduce batch size
        }
        else if (recordsInserted == currentBatchSize)
        {
            return Math.Min(currentBatchSize * 2, 5000); // Increase batch size
        }
        return currentBatchSize; // Keep batch size unchanged
    }

    private async Task<int> InsertUserRolesAsync(
        NpgsqlConnection pgConn,
        ConcurrentBag<(int userId, int roleId)> userRoles,
        NpgsqlTransaction? transaction = null)
    {
        if (userRoles.IsEmpty) return 0;

        var copyCommand = @"COPY user_roles (
            user_id, role_id, created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date
        ) FROM STDIN (FORMAT BINARY)";

        using var writer = await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None);
        var now = DateTime.UtcNow;
        int count = 0;

        foreach (var (userId, roleId) in userRoles)
        {
            writer.StartRow();
            writer.Write(userId, NpgsqlDbType.Integer); // user_id
            writer.Write(roleId, NpgsqlDbType.Integer); // role_id
            writer.Write(0, NpgsqlDbType.Integer); // created_by
            writer.Write(now, NpgsqlDbType.TimestampTz); // created_date
            writer.Write(DBNull.Value, NpgsqlDbType.Integer); // modified_by
            writer.Write(DBNull.Value, NpgsqlDbType.TimestampTz); // modified_date
            writer.Write(false, NpgsqlDbType.Boolean); // is_deleted
            writer.Write(DBNull.Value, NpgsqlDbType.Integer); // deleted_by
            writer.Write(DBNull.Value, NpgsqlDbType.TimestampTz); // deleted_date
            count++;
        }

        await writer.CompleteAsync();
        return count;
    }

    private UserRecord? ReadUserRecord(SqlDataReader reader, int recordNumber)
    {
        try
        {
            var (passwordHash, passwordSalt) = PasswordEncryptionHelper.EncryptPassword(reader["USERPASSWORD"]?.ToString() ?? string.Empty);
            var emailAddress = reader["EMAIL_ADDRESS"].ToString() ?? string.Empty;
            var mobileNumber = reader["MobileNumber"].ToString() ?? string.Empty;
            var encryptedEmail = _aesEncryptionService.Encrypt(emailAddress);
            var encryptedMobileNumber = _aesEncryptionService.Encrypt(mobileNumber);
            var emailHash = AesEncryptionService.ComputeSha256Hash(emailAddress);
            var mobileHash = AesEncryptionService.ComputeSha256Hash(mobileNumber);
            return new UserRecord
            {
                UserId = Convert.ToInt32(reader["PERSON_ID"]),
                Username = reader["USER_ID"].ToString() ?? "",
                PasswordHash = passwordHash,
                PasswordSalt = passwordSalt,
                FullName = reader["FULL_NAME"].ToString() ?? "",
                Email = encryptedEmail,
                MobileNumber = encryptedMobileNumber,
                Status = reader["STATUS"].ToString() ?? "",
                MaskedEmail = MaskHelper.MaskEmail(emailAddress),
                MaskedMobileNumber = MaskHelper.MaskPhoneNumber(mobileNumber),
                EmailHash = emailHash,
                MobileHash = mobileHash,
                ReportingToId = reader["REPORTINGTO"].ToString() ?? "",
                UserType = reader["USERTYPE_ID"].ToString() ?? "",
                Currency = reader["CURRENCYID"].ToString() ?? "",
                Location = reader["TIMEZONE"].ToString() ?? "",
                ErpUsername = reader["USER_SAP_ID"].ToString() ?? "",
                ApprovalHead = reader["DEPARTMENTHEAD"].ToString() ?? "",
                DigitalSignature = reader["DigitalSignature"].ToString() ?? "",
                DigitalSignaturePath = "/Documents/TechnicalDocuments/" + (reader["DigitalSignature"].ToString() ?? "")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading user record {recordNumber}: {ex.Message}");
            return null;
        }
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<UserRecord> batch, NpgsqlTransaction? transaction = null)
    {
        if (batch.Count == 0) return 0;
        try
        {
            return await InsertBatchWithCopyAsync(pgConn, batch, transaction);
        }
        catch (Exception ex)
        {
            throw new Exception($"Error inserting batch of {batch.Count} records: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchWithCopyAsync(NpgsqlConnection pgConn, List<UserRecord> batch, NpgsqlTransaction? transaction = null)
    {
        var copyCommand = @"COPY users (
            user_id, username, password_hash, full_name, email, mobile_number, status,
            password_salt, masked_email, masked_mobile_number, email_hash, mobile_hash, failed_login_attempts,
            last_failed_login, lockout_end, last_login_date, is_mfa_enabled, mfa_type, mfa_secret, last_mfa_sent_at,
            reporting_to_id, lockout_count, azureoid, user_type, currency, location, client_sap_code,
            digital_signature, last_password_changed, is_active, created_by, created_date, modified_by, modified_date,
            is_deleted, deleted_by, deleted_date, erp_username, approval_head, time_zone_country, digital_signature_path
        ) FROM STDIN (FORMAT BINARY)";
        using var writer = await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None);
        var now = DateTime.UtcNow;
        foreach (var record in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(record.UserId);
            await writer.WriteAsync(record.Username);
            await writer.WriteAsync(record.PasswordHash);
            await writer.WriteAsync(record.FullName);
            await writer.WriteAsync(record.Email);
            await writer.WriteAsync(record.MobileNumber);
            await writer.WriteAsync(record.Status);
            await writer.WriteAsync(record.PasswordSalt);
            await writer.WriteAsync(record.MaskedEmail);
            await writer.WriteAsync(record.MaskedMobileNumber);
            await writer.WriteAsync(record.EmailHash);
            await writer.WriteAsync(record.MobileHash);
            await writer.WriteAsync(0); // failed_login_attempts
            await writer.WriteAsync(DBNull.Value); // last_failed_login
            await writer.WriteAsync(DBNull.Value); // lockout_end
            await writer.WriteAsync(DBNull.Value); // last_login_date
            await writer.WriteAsync(false); // is_mfa_enabled
            await writer.WriteAsync(DBNull.Value); // mfa_type
            await writer.WriteAsync(DBNull.Value); // mfa_secret
            await writer.WriteAsync(DBNull.Value); // last_mfa_sent_at
            await writer.WriteAsync(record.ReportingToId);
            await writer.WriteAsync(0); // lockout_count
            await writer.WriteAsync(DBNull.Value); // azureoid
            await writer.WriteAsync(record.UserType);
            await writer.WriteAsync(record.Currency);
            await writer.WriteAsync(record.Location);
            await writer.WriteAsync(DBNull.Value); // client_sap_code
            await writer.WriteAsync(record.DigitalSignature);
            await writer.WriteAsync(DBNull.Value); // last_password_changed
            await writer.WriteAsync(true); // is_active
            await writer.WriteAsync(0); // created_by
            await writer.WriteAsync(now); // created_date
            await writer.WriteAsync(DBNull.Value); // modified_by
            await writer.WriteAsync(DBNull.Value); // modified_date
            await writer.WriteAsync(false); // is_deleted
            await writer.WriteAsync(DBNull.Value); // deleted_by
            await writer.WriteAsync(DBNull.Value); // deleted_date
            await writer.WriteAsync(record.ErpUsername);
            await writer.WriteAsync(record.ApprovalHead);
            await writer.WriteAsync(record.Location); // time_zone_country
            await writer.WriteAsync(record.DigitalSignaturePath);
        }
        await writer.CompleteAsync();
        return batch.Count;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "PERSON_ID -> user_id (Direct)",
            "USER_ID -> username (Direct)",
            "USERPASSWORD -> password_hash (Hashed using PBKDF2 with configurable iterations)",
            "password_salt -> password_salt (Generated during migration)",
            "FULL_NAME -> full_name (Direct)",
            "EMAIL_ADDRESS -> email (AES Encrypted)",
            "MobileNumber -> mobile_number (AES Encrypted)",
            "masked_email -> masked_email (Generated using MaskHelper.MaskEmail)",
            "masked_mobile_number -> masked_mobile_number (Generated using MaskHelper.MaskPhoneNumber)",
            "email_hash -> email_hash (SHA256, Generated during migration)",
            "mobile_hash -> mobile_hash (SHA256, Generated during migration)",
            "STATUS -> status (Direct)",
            "REPORTINGTO -> reporting_to_id (Direct, int to string conversion)",
            "USERTYPE_ID -> user_type (Mapped to text: 1=Admin, 2=Buyer, 3=Supplier, 6=HOD, 5=Technical)",
            "USERTYPE_ID -> user_roles.role_id (Mapped: 1->1, 2->2, 3->3, 6->4, 5->5)",
            "CURRENCYID -> currency (Mapped from PostgreSQL currency_master.currency_code using currency_id)",
            "USER_SAP_ID -> erp_username (Direct)",
            "DEPARTMENTHEAD -> approval_head (Direct, int to string conversion)",
            "TIMEZONE -> time_zone_country (Direct)",
            "DigitalSignature -> digital_signature (Direct)",
            "digital_signature_path -> digital_signature_path (Generated: /Documents/TechnicalDocuments/ + DigitalSignature)",
            "is_active -> is_active (Fixed: true)",
            "created_by -> created_by (Fixed: 0)",
            "created_date -> created_date (Set to migration timestamp)",
            "modified_by -> NULL (Fixed Default)",
            "modified_date -> NULL (Fixed Default)",
            "is_deleted -> false (Fixed Default)",
            "deleted_by -> NULL (Fixed Default)",
            "deleted_date -> NULL (Fixed Default)",
            "",
            "Additional Processing:",
            "- User roles inserted into user_roles table with mapping",
            "- Password hashing uses configurable iterations (default: 10000 in fast mode, 1500000 in secure mode)",
            "- Parallel transform workers for high-performance processing",
            "- PostgreSQL COPY for bulk insert optimization",
            "",
            "Note: Fields not present in minimal COPY are omitted for schema compatibility"
        };
    }

    private class UserRecord
    {
        public int UserId { get; set; }
        public string Username { get; set; } = "";
        public string PasswordHash { get; set; } = "";
        public string PasswordSalt { get; set; } = "";
        public string FullName { get; set; } = "";
        public string Email { get; set; } = "";
        public string MobileNumber { get; set; } = "";
        public string Status { get; set; } = "";
        public string MaskedEmail { get; set; } = "";
        public string MaskedMobileNumber { get; set; } = "";
        public string EmailHash { get; set; } = "";
        public string MobileHash { get; set; } = "";
        public string ReportingToId { get; set; } = "";
        public int UserTypeId { get; set; } // Store original ID for role mapping
        public string UserType { get; set; } = ""; // Text version for users table
        public string Currency { get; set; } = "";
        public string Location { get; set; } = "";
        public string ErpUsername { get; set; } = "";
        public string ApprovalHead { get; set; } = "";
        public string DigitalSignature { get; set; } = "";
        public string DigitalSignaturePath { get; set; } = "";
    }
}