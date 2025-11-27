using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.AspNetCore.SignalR;
using DataMigration.Hubs;
using DataMigration.Services;
using System;
using System.Threading;

[Route("Migration")]
public class MigrationController : Controller
{
    private readonly UOMMasterMigration _uomMigration;
    private readonly PlantMasterMigration _plantMigration;
    private readonly CurrencyMasterMigration _currencyMigration;
    private readonly CountryMasterMigration _countryMigration;
    private readonly MaterialGroupMasterMigration _materialGroupMigration;
    private readonly PurchaseGroupMasterMigration _purchaseGroupMigration;
    private readonly PaymentTermMasterMigration _paymentTermMigration;
    private readonly MaterialMasterMigration _materialMigration;
    private readonly EventMasterMigration _eventMigration;
    private readonly TaxMasterMigration _taxMigration;
    private readonly UsersMasterMigration _usersmasterMigration;
    private readonly ErpPrLinesMigration _erpprlinesMigration;
    private readonly IncotermMasterMigration _incotermMigration;
    private readonly PODocTypeMasterMigration _poDocTypeMigration;
    private readonly POConditionMasterMigration _poConditionMigration;
    private readonly IHubContext<MigrationProgressHub> _hubContext;


    public MigrationController(
        UOMMasterMigration uomMigration, 
        PlantMasterMigration plantMigration,
        CurrencyMasterMigration currencyMigration,
        CountryMasterMigration countryMigration,
        MaterialGroupMasterMigration materialGroupMigration,
        PurchaseGroupMasterMigration purchaseGroupMigration,
        PaymentTermMasterMigration paymentTermMigration,
        MaterialMasterMigration materialMigration,
        EventMasterMigration eventMigration,
        TaxMasterMigration taxMigration,
        UsersMasterMigration usersmasterMigration,
        ErpPrLinesMigration erpprlinesMigration,
        IncotermMasterMigration incotermMigration,
        PODocTypeMasterMigration poDocTypeMigration,
        POConditionMasterMigration poConditionMigration,
        IHubContext<MigrationProgressHub> hubContext)
    {
        _uomMigration = uomMigration;
        _plantMigration = plantMigration;
        _currencyMigration = currencyMigration;
        _countryMigration = countryMigration;
        _materialGroupMigration = materialGroupMigration;
        _purchaseGroupMigration = purchaseGroupMigration;
        _paymentTermMigration = paymentTermMigration;
        _materialMigration = materialMigration;
        _eventMigration = eventMigration;
        _taxMigration = taxMigration;
        _usersmasterMigration = usersmasterMigration;
        _erpprlinesMigration = erpprlinesMigration;
        _incotermMigration = incotermMigration;
        _poDocTypeMigration = poDocTypeMigration;
        _poConditionMigration = poConditionMigration;
        _hubContext = hubContext;
    }

    public IActionResult Index()
    {
        return View();
    }


    [HttpGet("GetTables")]
    public IActionResult GetTables()
    {
        var tables = new List<object>
        {
            new { name = "uom", description = "TBL_UOM_MASTER to uom_master" },
            new { name = "plant", description = "TBL_PlantMaster to plant_master" },
            new { name = "currency", description = "TBL_CURRENCYMASTER to currency_master" },
            new { name = "country", description = "TBL_COUNTRYMASTER to country_master" },
            new { name = "materialgroup", description = "TBL_MaterialGroupMaster to material_group_master" },
            new { name = "purchasegroup", description = "TBL_PurchaseGroupMaster to purchase_group_master" },
            new { name = "paymentterm", description = "TBL_PAYMENTTERMMASTER to payment_term_master" },
            new { name = "incoterm", description = "TBL_IncotermMAST to incoterm_master" },
            new { name = "material", description = "TBL_ITEMMASTER to material_master" },
            new { name = "eventmaster", description = "TBL_EVENTMASTER to event_master + event_setting" },
            new { name = "tax", description = "TBL_TaxMaster to tax_master" },
            new { name = "users", description = "TBL_USERMASTERFINAL to users" },
            new { name = "erpprlines", description = "TBL_PRTRANSACTION to erp_pr_lines" },
            new { name = "podoctype", description = "TBL_PO_DOC_TYPE to po_doc_type_master" },
            new { name = "pocondition", description = "TBL_POConditionTypeMaster to po_condition_master" },
        };
        return Json(tables);
    }

    [HttpGet("GetMappings")]
    public IActionResult GetMappings(string table)
    {
        if (table.ToLower() == "uom")
        {
            var mappings = _uomMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "plant")
        {
            var mappings = _plantMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "currency")
        {
            var mappings = _currencyMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "country")
        {
            var mappings = _countryMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "materialgroup")
        {
            var mappings = _materialGroupMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "purchasegroup")
        {
            var mappings = _purchaseGroupMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "paymentterm")
        {
            var mappings = _paymentTermMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "incoterm")
        {
            var mappings = _incotermMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "material")
        {
            var mappings = _materialMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventmaster")
        {
            var mappings = _eventMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "tax")
        {
            var mappings = _taxMigration.GetMappings();      
            return Json(mappings);
        }
        else if (table.ToLower() == "users")
        {
            var mappings = _usersmasterMigration.GetMappings();       
            return Json(mappings);
        }
        else if (table.ToLower() == "erp_pr_lines")
        {
            var mappings = _erpprlinesMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "podoctype")
        {
            var mappings = _poDocTypeMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "pocondition")
        {
            var mappings = _poConditionMigration.GetMappings();
            return Json(mappings);
        }
        return Json(new List<object>());
    }

    [HttpPost("MigrateAsync")]
    public async Task<IActionResult> MigrateAsync([FromBody] MigrationRequest request)
    {
        try
        {   
            // Handle other migration types (keeping existing logic)
            int recordCount = 0;
            if (request.Table.ToLower() == "uom")
            {
                recordCount = await _uomMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "plant")
            {
                recordCount = await _plantMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "currency")
            {
                recordCount = await _currencyMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "country")
            {
                recordCount = await _countryMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "materialgroup")
            {
                recordCount = await _materialGroupMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "purchasegroup")
            {
                recordCount = await _purchaseGroupMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "paymentterm")
            {
                recordCount = await _paymentTermMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "incoterm")
            {
                recordCount = await _incotermMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "material")
            {
                try
                {
                    recordCount = await _materialMigration.MigrateAsync();
                }
                catch (Exception ex)
                {
                    // Provide specific error information for material migration
                    string detailedError;
                    if (ex.Message.Contains("SQL Server connection issue"))
                    {
                        detailedError = "Database Connection Error: " + ex.Message + 
                                      "\n\nTroubleshooting steps:\n" +
                                      "1. Check if SQL Server is running and accessible\n" +
                                      "2. Verify connection string in appsettings.json\n" +
                                      "3. Check network connectivity between application and SQL Server\n" +
                                      "4. Ensure TBL_ITEMMASTER table exists and has data\n" +
                                      "5. Check if the table has too many records (consider pagination)";
                    }
                    else if (ex.Message.Contains("constraint violation") || ex.Message.Contains("foreign key"))
                    {
                        detailedError = "Database Constraint Error: " + ex.Message + 
                                      "\n\nTroubleshooting steps:\n" +
                                      "1. Ensure UOM Master migration was completed successfully\n" +
                                      "2. Ensure Material Group Master migration was completed successfully\n" +
                                      "3. Check for invalid UOMId or MaterialGroupId values in TBL_ITEMMASTER\n" +
                                      "4. Verify that all foreign key reference tables have the required data";
                    }
                    else
                    {
                        detailedError = "Material Migration Error: " + ex.Message +
                                      "\n\nGeneral troubleshooting:\n" +
                                      "1. Check application logs for more details\n" +
                                      "2. Verify data integrity in source table TBL_ITEMMASTER\n" +
                                      "3. Ensure PostgreSQL connection is stable\n" +
                                      "4. Check for data type mismatches or invalid characters";
                    }
                    
                    return Json(new { 
                        success = false, 
                        error = detailedError,
                        table = "material",
                        timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss UTC")
                    });
                }
            }
            else if (request.Table.ToLower() == "eventmaster")
            {
                var result = await _eventMigration.MigrateAsync();
                var message = $"Migration completed for {request.Table}. Success: {result.SuccessCount}, Failed: {result.FailedCount}";

                if (result.Errors.Any())
                {
                    message += $", Errors: {result.Errors.Count}";
                }

                return Json(new
                {
                    success = true,
                    message = message,
                    details = new
                    {
                        successCount = result.SuccessCount,
                        failedCount = result.FailedCount,
                        errors = result.Errors.Take(5).ToList() // Return first 5 errors
                    }
                });
            }
            else if (request.Table.ToLower() == "tax")
            {
                recordCount = await _taxMigration.MigrateAsync(); // 6. Migrate tax data
            }
            else if (request.Table.ToLower() == "users")
            {
                recordCount = await _usersmasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "erp_pr_lines")
            {
                recordCount = await _erpprlinesMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "podoctype")
            {
                recordCount = await _poDocTypeMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "pocondition")
            {
                recordCount = await _poConditionMigration.MigrateAsync();
            }
            else
            {
                return Json(new { success = false, error = "Unknown table" });
            }
            
            return Json(new { success = true, message = $"Migration completed for {request.Table}. {recordCount} records migrated." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost("migrate-all-with-transaction")]
    public async Task<IActionResult> MigrateAllWithTransaction()
    {
        try
        {
            var migrationServices = new List<MigrationService>
            {
                _uomMigration,
                _currencyMigration,
                _countryMigration,
                _materialGroupMigration,
                _plantMigration,
                _purchaseGroupMigration,
                _paymentTermMigration,
                _incotermMigration,
                _materialMigration,
                _taxMigration,
                _usersmasterMigration,
                _erpprlinesMigration,
                _poDocTypeMigration,
                _poConditionMigration
            };

            var (totalMigrated, results) = await MigrationService.MigrateMultipleAsync(migrationServices, useCommonTransaction: true);

            return Json(new 
            { 
                success = true, 
                message = $"Successfully migrated {totalMigrated} records across all services.",
                results = results,
                timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            return Json(new 
            { 
                success = false, 
                message = "Migration failed and was rolled back.", 
                error = ex.Message,
                timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("migrate-individual-with-transactions")]
    public async Task<IActionResult> MigrateIndividualWithTransactions()
    {
        var results = new Dictionary<string, object>();
        
        try
        {
            // Migrate each service individually with their own transactions
            results["UOM"] = new { count = await _uomMigration.MigrateAsync(), success = true };
            results["Currency"] = new { count = await _currencyMigration.MigrateAsync(), success = true };
            results["Country"] = new { count = await _countryMigration.MigrateAsync(), success = true };
            results["MaterialGroup"] = new { count = await _materialGroupMigration.MigrateAsync(), success = true };
            results["Plant"] = new { count = await _plantMigration.MigrateAsync(), success = true };
            results["PurchaseGroup"] = new { count = await _purchaseGroupMigration.MigrateAsync(), success = true };
            results["PaymentTerm"] = new { count = await _paymentTermMigration.MigrateAsync(), success = true };
            results["Incoterm"] = new { count = await _incotermMigration.MigrateAsync(), success = true };
            results["Material"] = new { count = await _materialMigration.MigrateAsync(), success = true };
            results["Tax"] = new { count = await _taxMigration.MigrateAsync(), success = true };
            results["Users"] = new { count = await _usersmasterMigration.MigrateAsync(), success = true };
            results["ErpPrLines"] = new { count = await _erpprlinesMigration.MigrateAsync(), success = true };

            // Handle EventMaster separately due to its different return type
            var eventResult = await _eventMigration.MigrateAsync();
            results["Event"] = new 
            { 
                count = eventResult.SuccessCount, 
                failed = eventResult.FailedCount, 
                errors = eventResult.Errors, 
                success = eventResult.FailedCount == 0 
            };

            return Json(new 
            { 
                success = true, 
                message = "Individual migrations completed.",
                results = results,
                timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            return Json(new 
            { 
                success = false, 
                message = "One or more migrations failed.", 
                error = ex.Message,
                results = results,
                timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("test-connections")]
    public async Task<IActionResult> TestConnections()
    {
        try
        {
            var diagnostics = await _materialMigration.TestConnectionsAsync();
            return Json(new { success = true, diagnostics = diagnostics });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpGet("validate-source-data/{table}")]
    public async Task<IActionResult> ValidateSourceData(string table)
    {
        try
        {
            var validation = new Dictionary<string, object>();
            
            if (table.ToLower() == "material")
            {
                using var sqlConn = _materialMigration.GetSqlServerConnection();
                await sqlConn.OpenAsync();
                
                // Check if table exists
                var checkTableQuery = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TBL_ITEMMASTER'";
                using var checkCmd = new SqlCommand(checkTableQuery, sqlConn);
                var tableExists = Convert.ToInt32(await checkCmd.ExecuteScalarAsync()) > 0;
                
                validation["TableExists"] = tableExists;
                
                if (tableExists)
                {
                    // Get total record count
                    var countQuery = "SELECT COUNT(*) FROM TBL_ITEMMASTER";
                    using var countCmd = new SqlCommand(countQuery, sqlConn);
                    validation["TotalRecords"] = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
                    
                    // Check for problematic records
                    var problemsQuery = @"
                        SELECT 
                            COUNT(*) as TotalRecords,
                            SUM(CASE WHEN ITEMCODE IS NULL OR ITEMCODE = '' THEN 1 ELSE 0 END) as NullItemCodes,
                            SUM(CASE WHEN UOMId IS NULL OR UOMId = 0 THEN 1 ELSE 0 END) as NullUOMIds,
                            SUM(CASE WHEN MaterialGroupId IS NULL OR MaterialGroupId = 0 THEN 1 ELSE 0 END) as NullMaterialGroupIds,
                            SUM(CASE WHEN ClientSAPId IS NULL THEN 1 ELSE 0 END) as NullClientSAPIds
                        FROM TBL_ITEMMASTER";
                    
                    using var problemsCmd = new SqlCommand(problemsQuery, sqlConn);
                    using var reader = await problemsCmd.ExecuteReaderAsync();
                    
                    if (await reader.ReadAsync())
                    {
                        validation["DataQuality"] = new
                        {
                            TotalRecords = reader["TotalRecords"],
                            Issues = new
                            {
                                NullItemCodes = reader["NullItemCodes"],
                                NullUOMIds = reader["NullUOMIds"],
                                NullMaterialGroupIds = reader["NullMaterialGroupIds"],
                                NullClientSAPIds = reader["NullClientSAPIds"]
                            }
                        };
                    }
                }
                else
                {
                    validation["Error"] = "Source table TBL_ITEMMASTER does not exist";
                }
            }
            else if (table.ToLower() == "country")
            {
                using var sqlConn = _countryMigration.GetSqlServerConnection();
                await sqlConn.OpenAsync();
                
                // Check if table exists
                var checkTableQuery = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TBL_COUNTRYMASTER'";
                using var checkCmd = new SqlCommand(checkTableQuery, sqlConn);
                var tableExists = Convert.ToInt32(await checkCmd.ExecuteScalarAsync()) > 0;
                
                validation["TableExists"] = tableExists;
                
                if (tableExists)
                {
                    // Get total record count
                    var countQuery = "SELECT COUNT(*) FROM TBL_COUNTRYMASTER";
                    using var countCmd = new SqlCommand(countQuery, sqlConn);
                    validation["TotalRecords"] = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
                    
                    // Check for problematic records
                    var problemsQuery = @"
                        SELECT 
                            COUNT(*) as TotalRecords,
                            SUM(CASE WHEN Country_NAME IS NULL OR Country_NAME = '' THEN 1 ELSE 0 END) as NullCountryNames,
                            SUM(CASE WHEN Country_Shname IS NULL OR Country_Shname = '' THEN 1 ELSE 0 END) as NullCountryCodes,
                            SUM(CASE WHEN CountryMasterID IS NULL THEN 1 ELSE 0 END) as NullCountryMasterIds
                        FROM TBL_COUNTRYMASTER";
                    
                    using var problemsCmd = new SqlCommand(problemsQuery, sqlConn);
                    using var reader = await problemsCmd.ExecuteReaderAsync();
                    
                    if (await reader.ReadAsync())
                    {
                        validation["DataQuality"] = new
                        {
                            TotalRecords = reader["TotalRecords"],
                            Issues = new
                            {
                                NullCountryNames = reader["NullCountryNames"],
                                NullCountryCodes = reader["NullCountryCodes"],
                                NullCountryMasterIds = reader["NullCountryMasterIds"]
                            }
                        };
                    }
                }
                else
                {
                    validation["Error"] = "Source table TBL_COUNTRYMASTER does not exist";
                }
            }
            else
            {
                validation["Error"] = $"Validation not implemented for table: {table}";
            }
            
            return Json(new { success = true, validation = validation });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    // Optimized Material Migration Endpoints
    [HttpPost("material/migrate-optimized")]
    public IActionResult MigrateOptimizedMaterialAsync()
    {
        try
        {
            var migrationId = Guid.NewGuid().ToString();
            var progress = new SignalRMigrationProgress(_hubContext, migrationId);
            
            // Start migration in background
            _ = Task.Run(async () =>
            {
                try
                {
                    await _materialMigration.MigrateAsync(useTransaction: true, progress: progress);
                }
                catch (Exception ex)
                {
                    progress.ReportError(ex.Message, 0);
                }
            });
            
            return Json(new { success = true, migrationId = migrationId, message = "Material migration started. Use the migrationId to track progress via SignalR." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost("material/migrate-with-console-progress")]
    public async Task<IActionResult> MigrateOptimizedMaterialWithConsoleAsync()
    {
        try
        {
            var progress = new ConsoleMigrationProgress();
            int recordCount = await _materialMigration.MigrateAsync(useTransaction: true, progress: progress);
            
            return Json(new { 
                success = true, 
                recordCount = recordCount,
                message = "Material migration completed successfully." 
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpGet("material/estimate-time")]
    public async Task<IActionResult> EstimateMaterialMigrationTimeAsync()
    {
        try
        {
            using var sqlConn = _materialMigration.GetSqlServerConnection();
            await sqlConn.OpenAsync();
            
            using var cmd = new SqlCommand("SELECT COUNT(*) FROM TBL_ITEMMASTER", sqlConn);
            var totalRecords = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            
            // Estimate based on typical performance (assuming ~500 records per second for optimized version)
            var estimatedSecondsOptimized = totalRecords / 500.0;
            var estimatedSecondsOriginal = totalRecords / 50.0; // Original method is much slower
            
            return Json(new { 
                success = true, 
                totalRecords = totalRecords,
                estimatedTimeOptimized = TimeSpan.FromSeconds(estimatedSecondsOptimized).ToString(@"hh\:mm\:ss"),
                estimatedTimeOriginal = TimeSpan.FromSeconds(estimatedSecondsOriginal).ToString(@"hh\:mm\:ss"),
                improvementFactor = "~10x faster with optimized version"
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpGet("material/progress-dashboard")]
    public IActionResult MaterialProgressDashboard()
    {
        return View("OptimizedProgress");
    }

}
public class MigrationRequest
{
    public required string Table { get; set; }
}