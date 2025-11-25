using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

[Route("Migration")]
public class MigrationController : Controller
{
    private readonly UOMMasterMigration _uomMigration;
    private readonly PlantMasterMigration _plantMigration;
    private readonly CurrencyMasterMigration _currencyMigration;
    private readonly MaterialGroupMasterMigration _materialGroupMigration;
    private readonly PurchaseGroupMasterMigration _purchaseGroupMigration;
    private readonly PaymentTermMasterMigration _paymentTermMigration;
    private readonly MaterialMasterMigration _materialMigration;
    private readonly EventMasterMigration _eventMigration;
    private readonly TaxMasterMigration _taxMigration;
    private readonly UsersMasterMigration _usersmasterMigration;
    private readonly ErpPrLinesMigration _erpprlinesMigration;
    private readonly ARCMainMigration _arcMainMigration;


    public MigrationController(
        UOMMasterMigration uomMigration, 
        PlantMasterMigration plantMigration,
        CurrencyMasterMigration currencyMigration,
        MaterialGroupMasterMigration materialGroupMigration,
        PurchaseGroupMasterMigration purchaseGroupMigration,
        PaymentTermMasterMigration paymentTermMigration,
        MaterialMasterMigration materialMigration,
        EventMasterMigration eventMigration,
        TaxMasterMigration taxMigration,
        UsersMasterMigration usersmasterMigration,
        ErpPrLinesMigration erpprlinesMigration,
        ARCMainMigration arcMainMigration)
    {
        _uomMigration = uomMigration;
        _plantMigration = plantMigration;
        _currencyMigration = currencyMigration;
        _materialGroupMigration = materialGroupMigration;
        _purchaseGroupMigration = purchaseGroupMigration;
        _paymentTermMigration = paymentTermMigration;
        _materialMigration = materialMigration;
        _eventMigration = eventMigration;
        _taxMigration = taxMigration;
        _usersmasterMigration = usersmasterMigration;
        _erpprlinesMigration = erpprlinesMigration;
        _arcMainMigration = arcMainMigration;
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
            new { name = "materialgroup", description = "TBL_MaterialGroupMaster to material_group_master" },
            new { name = "purchasegroup", description = "TBL_PurchaseGroupMaster to purchase_group_master" },
            new { name = "paymentterm", description = "TBL_PAYMENTTERMMASTER to payment_term_master" },
            new { name = "material", description = "TBL_ITEMMASTER to material_master" },
            new { name = "eventmaster", description = "TBL_EVENTMASTER to event_master + event_setting" },
            new { name = "tax", description = "TBL_TaxMaster to tax_master" },
            new { name = "users", description = "TBL_USERMASTERFINAL to users" },
            new { name = "erpprlines", description = "TBL_PRTRANSACTION to erp_pr_lines" },
            new { name = "arcmain", description = "TBL_ARCMain to arc_header" },
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
        else if (table.ToLower() == "arcmain")
        {
            var mappings = _arcMainMigration.GetMappings();
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
            else if (request.Table.ToLower() == "material")
            {
                recordCount = await _materialMigration.MigrateAsync();
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
            else if (request.Table.ToLower() == "arcmain")
            {
                recordCount = await _arcMainMigration.MigrateAsync();
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
}

public class MigrationRequest
{
    public required string Table { get; set; }
}