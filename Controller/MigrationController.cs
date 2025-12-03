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
    private readonly WorkflowMasterMigration _workflowMigration;
    private readonly WorkflowMasterHistoryMigration _workflowHistoryMigration;
    private readonly WorkflowHistoryMigration _workflowHistoryTableMigration;
    private readonly WorkflowAmountMigration _workflowAmountMigration;
    private readonly WorkflowAmountHistoryMigration _workflowAmountHistoryMigration;
    private readonly WorkflowApprovalUserMigration _workflowApprovalUserMigration;
    private readonly WorkflowApprovalUserHistoryMigration _workflowApprovalUserHistoryMigration;
    private readonly IHubContext<MigrationProgressHub> _hubContext;
    private readonly ARCMainMigration _arcMainMigration;
    private readonly TaxCodeMasterMigration _taxCodeMasterMigration;
    private readonly CompanyMasterMigration _companyMasterMigration;
    private readonly PurchaseOrganizationMasterMigration _purchaseOrganizationMigration;
    private readonly ValuationTypeMasterMigration _valuationTypeMigration;
    private readonly TypeOfCategoryMasterMigration _typeOfCategoryMigration;
    private readonly SupplierGroupMasterMigration _supplierGroupMigration;
    private readonly SupplierMasterMigration _supplierMigration;

    private readonly SupplierPaymentIncotermMigration _supplierPaymentIncotermMigration;
    private readonly SupplierInactiveMigration _supplierInactiveMigration;
    private readonly SupplierOtherContactMigration _supplierOtherContactMigration;
    private readonly ARCSubMigration _arcSubMigration;
    private readonly ARCPlantMigration _arcPlantMigration;
    private readonly ARCAttachmentMigration _arcAttachmentMigration;
    private readonly ARCApprovalAuthorityMigration _arcApprovalAuthorityMigration;
    private readonly PRAttachmentMigration _prAttachmentMigration;
    private readonly PRBoqItemsMigration _prBoqItemsMigration;
    private readonly EventItemsMigration _eventItemsMigration;
    private readonly EventBoqItemsMigration _eventBoqItemsMigration;
    private readonly AssignedEventVendorMigration _assignedEventVendorMigration;
    private readonly EventCollaborationMigration _eventCollaborationMigration;
    private readonly UserTechnicalParameterMigration _userTechnicalParameterMigration;
    private readonly UserTermMigration _userTermMigration;
    private readonly TechnicalApprovalWorkflowMigration _technicalApprovalWorkflowMigration;
    private readonly TechnicalApprovalScoreMigration _technicalApprovalScoreMigration;
    private readonly TechnicalApprovalScoreHistoryMigration _technicalApprovalScoreHistoryMigration;
    private readonly TechnicalDocumentsMigration _technicalDocumentsMigration;
    private readonly TechnicalParameterTemplateMigration _technicalParameterTemplateMigration;
    private readonly TermMasterMigration _termMasterMigration;
    private readonly TermTemplateMasterMigration _termTemplateMasterMigration;
    private readonly UserPriceBidLotChargesMigration _userPriceBidLotChargesMigration;
    private readonly UserPriceBidNonPricingMigration _userPriceBidNonPricingMigration;
    private readonly IConfiguration _configuration;
    private readonly EventSettingMigrationService _eventSettingMigrationService;
    private readonly ILogger<MigrationController> _logger;


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
        WorkflowMasterMigration workflowMigration,
        WorkflowMasterHistoryMigration workflowHistoryMigration,
        WorkflowHistoryMigration workflowHistoryTableMigration,
        WorkflowAmountMigration workflowAmountMigration,
        WorkflowAmountHistoryMigration workflowAmountHistoryMigration,
        WorkflowApprovalUserMigration workflowApprovalUserMigration,
        WorkflowApprovalUserHistoryMigration workflowApprovalUserHistoryMigration,
        ARCMainMigration arcMainMigration,
        ARCSubMigration arcSubMigration,
        ARCPlantMigration arcPlantMigration,
        ARCAttachmentMigration arcAttachmentMigration,
        ARCApprovalAuthorityMigration arcApprovalAuthorityMigration,
        PRAttachmentMigration prAttachmentMigration,
        PRBoqItemsMigration prBoqItemsMigration,
        EventItemsMigration eventItemsMigration,
        EventBoqItemsMigration eventBoqItemsMigration,
        AssignedEventVendorMigration assignedEventVendorMigration,
        EventCollaborationMigration eventCollaborationMigration,
        UserTechnicalParameterMigration userTechnicalParameterMigration,
        UserTermMigration userTermMigration,
        TechnicalApprovalWorkflowMigration technicalApprovalWorkflowMigration,
        TechnicalApprovalScoreMigration technicalApprovalScoreMigration,
        TechnicalApprovalScoreHistoryMigration technicalApprovalScoreHistoryMigration,
        TechnicalDocumentsMigration technicalDocumentsMigration,
        TechnicalParameterTemplateMigration technicalParameterTemplateMigration,
        TermMasterMigration termMasterMigration,
        TermTemplateMasterMigration termTemplateMasterMigration,
        UserPriceBidLotChargesMigration userPriceBidLotChargesMigration,
        UserPriceBidNonPricingMigration userPriceBidNonPricingMigration,
        TaxCodeMasterMigration taxCodeMasterMigration,
        CompanyMasterMigration companyMasterMigration,
        PurchaseOrganizationMasterMigration purchaseOrganizationMigration,
        ValuationTypeMasterMigration valuationTypeMigration,
        TypeOfCategoryMasterMigration typeOfCategoryMigration,
        SupplierGroupMasterMigration supplierGroupMigration,
        SupplierMasterMigration supplierMigration,
        SupplierPaymentIncotermMigration supplierPaymentIncotermMigration,
        SupplierInactiveMigration supplierInactiveMigration,
        SupplierOtherContactMigration supplierOtherContactMigration,
        IHubContext<MigrationProgressHub> hubContext,
        IConfiguration configuration,
        EventSettingMigrationService eventSettingMigrationService,
        ILogger<MigrationController> logger)
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
        _workflowMigration = workflowMigration;
        _workflowHistoryMigration = workflowHistoryMigration;
        _workflowHistoryTableMigration = workflowHistoryTableMigration;
        _workflowAmountMigration = workflowAmountMigration;
        _workflowAmountHistoryMigration = workflowAmountHistoryMigration;
        _workflowApprovalUserMigration = workflowApprovalUserMigration;
        _workflowApprovalUserHistoryMigration = workflowApprovalUserHistoryMigration;
        _arcMainMigration = arcMainMigration;
        _arcSubMigration = arcSubMigration;
        _arcPlantMigration = arcPlantMigration;
        _arcAttachmentMigration = arcAttachmentMigration;
        _arcApprovalAuthorityMigration = arcApprovalAuthorityMigration;
        _prAttachmentMigration = prAttachmentMigration;
        _prBoqItemsMigration = prBoqItemsMigration;
        _eventItemsMigration = eventItemsMigration;
        _eventBoqItemsMigration = eventBoqItemsMigration;
        _assignedEventVendorMigration = assignedEventVendorMigration;
        _eventCollaborationMigration = eventCollaborationMigration;
        _userTechnicalParameterMigration = userTechnicalParameterMigration;
        _userTermMigration = userTermMigration;
        _technicalApprovalWorkflowMigration = technicalApprovalWorkflowMigration;
        _technicalApprovalScoreMigration = technicalApprovalScoreMigration;
        _technicalApprovalScoreHistoryMigration = technicalApprovalScoreHistoryMigration;
        _technicalDocumentsMigration = technicalDocumentsMigration;
        _technicalParameterTemplateMigration = technicalParameterTemplateMigration;
        _termMasterMigration = termMasterMigration;
        _termTemplateMasterMigration = termTemplateMasterMigration;
        _userPriceBidLotChargesMigration = userPriceBidLotChargesMigration;
        _userPriceBidNonPricingMigration = userPriceBidNonPricingMigration;
        _taxCodeMasterMigration = taxCodeMasterMigration;
        _companyMasterMigration = companyMasterMigration;
        _purchaseOrganizationMigration = purchaseOrganizationMigration;
        _valuationTypeMigration = valuationTypeMigration;
        _typeOfCategoryMigration = typeOfCategoryMigration;
        _supplierGroupMigration = supplierGroupMigration;
        _supplierMigration = supplierMigration;
        _supplierPaymentIncotermMigration = supplierPaymentIncotermMigration;
        _supplierInactiveMigration = supplierInactiveMigration;
        _supplierOtherContactMigration = supplierOtherContactMigration;
        _hubContext = hubContext;
        _configuration = configuration;
        _eventSettingMigrationService = eventSettingMigrationService;
        _logger = logger;
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
            new { name = "taxcode", description = "TBL_TaxCodeMaster to tax_code_master" },
            new { name = "company", description = "TBL_CompanyMaster to company_master" },
            new { name = "arcmain", description = "TBL_ARCMain to arc_main" },
            new {name = "arcsub", description = "TBL_ARCSub to arc_sub" },
            new {name = "arcplant", description = "TBL_ARCPlant to arc_plant" },
            new {name = "arcattachment", description = "TBL_ARCATTACHMENT to arc_attachments" },
            new {name = "prattachment", description = "TBL_PRATTACHMENT to pr_attachments" },
            new {name = "prboqitems", description = "tbl_PRBOQItems to pr_boq_items" },
            new {name = "eventitems", description = "TBL_PB_BUYER to event_items" },
            new {name = "eventboqitems", description = "TBL_PB_BUYER_SUB to event_boq_items" },
            new {name = "assignedeventvendor", description = "TBL_EVENTSELECTEDUSER (Vendor only) to assigned_event_vendor" },
            new {name = "eventcollaboration", description = "TBL_EVENTSELECTEDUSER (Non-Vendor) to event_collaboration" },
            new {name = "usertechnicalparameter", description = "TBL_TechItemTerms to user_technical_parameter" },
            new {name = "userterm", description = "TBL_CLAUSEEVENTWISE to user_term" },
            new {name = "technicalapprovalworkflow", description = "TBL_TechnicalApproval_History to technical_approval_workflow" },
            new {name = "technicalapprovalscore", description = "TBL_TechApproval to technical_approval_score" },
            new {name = "technicalapprovalscorehistory", description = "TBL_TechApprovalScoreHistory to technical_approval_score_history" },
            new {name = "technicaldocuments", description = "TBL_TECHNICALATTACHMENT to technical_documents" },
            new {name = "technicalparametertemplate", description = "TBL_TECHPARAMAIN + TBL_TECHPARASUB to technical_parameter_template" },
            new {name = "termmaster", description = "TermMaster to term_master" },
            new {name = "termtemplatemaster", description = "TBL_CLAUSEMASTER to term_template_master" },
            new {name = "userpricebidlotcharges", description = "TBL_PB_BUYEROTHERCHARGES to user_price_bid_lot_charges" },
            new {name = "userpricebidnonpricing", description = "TBL_PB_BUYEROTHERCHARGES to user_price_bid_lot_charges (Non Pricing)" },
            new {name = "arcapprovalauthority", description = "TBL_ARCApprovalAuthority to arc_workflow" },
            new { name = "valuationtype", description = "TBL_ValuationTypeMaster to valuation_type_master" },
            new { name = "typeofcategory", description = "TBL_TypeOfCategoryMaster to type_of_category_master" },
            new { name = "suppliergroup", description = "TBL_SupplierGroupMaster to supplier_group_master" },
            new { name = "supplier", description = "TBL_SupplierMaster to supplier_master" },
            new { name = "supplierpaymentincoterm", description = "TBL_Vendor_Pay_Inco_Terms to supplier_payment_incoterm" },
            new { name = "supplierinactive", description = "TBL_VendorInactive to supplier_inactive" },
            new { name = "supplierothercontact", description = "TBL_COMUNICATION to supplier_other_contact" },
            new { name = "users", description = "TBL_USERMASTERFINAL to users" },
            new { name = "erpprlines", description = "TBL_PRTRANSACTION to erp_pr_lines" },
            new { name = "podoctype", description = "TBL_PO_DOC_TYPE to po_doc_type_master" },
            new { name = "pocondition", description = "TBL_POConditionTypeMaster to po_condition_master" },
            new { name = "workflow", description = "TBL_WorkFlowMain to workflow_master" },
            new { name = "workflowhistory", description = "TBL_WorkFlowMain_History to workflow_master_history" },
            new { name = "workflowhistorytable", description = "TBL_WORKFLOW_HISTORY to workflow_history" },
            new { name = "workflowamount", description = "TBL_WorkFlowSub to workflow_amount" },
            new { name = "workflowamounthistory", description = "TBL_WorkFlowSub_History to workflow_amount_history" },
            new { name = "workflowapprovaluser", description = "TBL_WorkFlowSubSub to workflow_approval_user" },
            new { name = "workflowapprovaluserhistory", description = "TBL_WorkFlowSubSub_History to workflow_approval_user_history" },
            new { name = "eventsetting", description = "TBL_EVENTMASTER to event_setting" },
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
        else if (table.ToLower() == "erpprlines")
        {
            var mappings = _erpprlinesMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "arcmain")
        {
            var mappings = _arcMainMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "taxcodemaster")
        {
            var mappings = _taxCodeMasterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "companymaster")
        {
            var mappings = _companyMasterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "purchaseorganization")
        {
            var mappings = _purchaseOrganizationMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "valuationtype")
        {
            var mappings = _valuationTypeMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "typeofcategory")
        {
            var mappings = _typeOfCategoryMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "suppliergroup")
        {
            var mappings = _supplierGroupMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplier")
        {
            var mappings = _supplierMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierothercontact")
        {
            var mappings = _supplierOtherContactMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierinactive")
        {
            var mappings = _supplierInactiveMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflow")
        {
            var mappings = _workflowMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflowhistory")
        {
            var mappings = _workflowHistoryMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflowhistorytable")
        {
            var mappings = _workflowHistoryTableMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflowamount")
        {
            var mappings = _workflowAmountMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflowamounthistory")
        {
            var mappings = _workflowAmountHistoryMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflowapprovaluser")
        {
            var mappings = _workflowApprovalUserMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "workflowapprovaluserhistory")
        {
            var mappings = _workflowApprovalUserHistoryMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierpaymentincoterm")
        {
            var mappings = _supplierPaymentIncotermMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "arcsub")
        {
            var mappings = _arcSubMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "arcplant")
        {
            var mappings = _arcPlantMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "arcattachment")
        {
            var mappings = _arcAttachmentMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "prattachment")
        {
            var mappings = _prAttachmentMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "prboqitems")
        {
            var mappings = _prBoqItemsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventitems")
        {
            var mappings = _eventItemsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventboqitems")
        {
            var mappings = _eventBoqItemsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "assignedeventvendor")
        {
            var mappings = _assignedEventVendorMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventcollaboration")
        {
            var mappings = _eventCollaborationMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "usertechnicalparameter")
        {
            var mappings = _userTechnicalParameterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "userterm")
        {
            var mappings = _userTermMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "technicalapprovalworkflow")
        {
            var mappings = _technicalApprovalWorkflowMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "technicalapprovalscore")
        {
            var mappings = _technicalApprovalScoreMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "technicalapprovalscorehistory")
        {
            var mappings = _technicalApprovalScoreHistoryMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "technicaldocuments")
        {
            var mappings = _technicalDocumentsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "technicalparametertemplate")
        {
            var mappings = _technicalParameterTemplateMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "termmaster")
        {
            var mappings = _termMasterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "termtemplatemaster")
        {
            var mappings = _termTemplateMasterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "userpricebidlotcharges")
        {
            var mappings = _userPriceBidLotChargesMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "userpricebidnonpricing")
        {
            var mappings = _userPriceBidNonPricingMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "arcapprovalauthority")
        {
            var mappings = _arcApprovalAuthorityMigration.GetMappings();
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
            else if (request.Table.ToLower() == "supplierothercontact")
            {
                recordCount = await _supplierOtherContactMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierinactive")
            {
                recordCount = await _supplierInactiveMigration.MigrateAsync();
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
            else if (request.Table.ToLower() == "erpprlines")
            {
                recordCount = await _erpprlinesMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "arcmain")
            {
                recordCount = await _arcMainMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "taxcodemaster")
            {
                recordCount = await _taxCodeMasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "companymaster")
            {
                recordCount = await _companyMasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "purchaseorganization")
            {
                recordCount = await _purchaseOrganizationMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "valuationtype")
            {
                recordCount = await _valuationTypeMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "typeofcategory")
            {
                recordCount = await _typeOfCategoryMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "suppliergroup")
            {
                recordCount = await _supplierGroupMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplier")
            {
                recordCount = await _supplierMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflow")
            {
                recordCount = await _workflowMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflowhistory")
            {
                recordCount = await _workflowHistoryMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflowhistorytable")
            {
                recordCount = await _workflowHistoryTableMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflowamount")
            {
                recordCount = await _workflowAmountMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflowamounthistory")
            {
                recordCount = await _workflowAmountHistoryMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflowapprovaluser")
            {
                recordCount = await _workflowApprovalUserMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "workflowapprovaluserhistory")
            {
                recordCount = await _workflowApprovalUserHistoryMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierpaymentincoterm")
            {
                recordCount = await _supplierPaymentIncotermMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "arcsub")
            {
                recordCount = await _arcSubMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "arcplant")
            {
                recordCount = await _arcPlantMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "arcattachment")
            {
                recordCount = await _arcAttachmentMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "prattachment")
            {
                recordCount = await _prAttachmentMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "prboqitems")
            {
                recordCount = await _prBoqItemsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventitems")
            {
                recordCount = await _eventItemsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventboqitems")
            {
                recordCount = await _eventBoqItemsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "assignedeventvendor")
            {
                recordCount = await _assignedEventVendorMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventcollaboration")
            {
                recordCount = await _eventCollaborationMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "usertechnicalparameter")
            {
                recordCount = await _userTechnicalParameterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "userterm")
            {
                recordCount = await _userTermMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "technicalapprovalworkflow")
            {
                recordCount = await _technicalApprovalWorkflowMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "technicalapprovalscore")
            {
                recordCount = await _technicalApprovalScoreMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "technicalapprovalscorehistory")
            {
                recordCount = await _technicalApprovalScoreHistoryMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "technicaldocuments")
            {
                recordCount = await _technicalDocumentsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "technicalparametertemplate")
            {
                recordCount = await _technicalParameterTemplateMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "termmaster")
            {
                recordCount = await _termMasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "termtemplatemaster")
            {
                recordCount = await _termTemplateMasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "userpricebidlotcharges")
            {
                recordCount = await _userPriceBidLotChargesMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "userpricebidnonpricing")
            {
                recordCount = await _userPriceBidNonPricingMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "arcapprovalauthority")
            {
                recordCount = await _arcApprovalAuthorityMigration.MigrateAsync();
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

    [HttpPost("MigrateDebugAsync")]
    public async Task<IActionResult> MigrateDebugAsync([FromBody] MigrationRequest request)
    {
        try
        {   
            int recordCount = 0;
            if (request.Table.ToLower() == "workflowamount")
            {
                recordCount = await _workflowAmountMigration.MigrateWithoutTransactionAsync();
            }
            else if (request.Table.ToLower() == "workflowamounthistory")
            {
                recordCount = await _workflowAmountHistoryMigration.MigrateAsync(); // Will need to add debug method
            }
            else if (request.Table.ToLower() == "workflowapprovaluser")
            {
                recordCount = await _workflowApprovalUserMigration.MigrateAsync(); // Will need to add debug method
            }
            else if (request.Table.ToLower() == "workflowapprovaluserhistory")
            {
                recordCount = await _workflowApprovalUserHistoryMigration.MigrateAsync(); // Will need to add debug method
            }
            else
            {
                return Json(new { success = false, error = "Debug migration not available for this table" });
            }
            
            return Json(new { success = true, message = $"Debug migration completed for {request.Table}. {recordCount} records migrated." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message, stackTrace = ex.StackTrace });
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
                _poConditionMigration,
                _workflowMigration,
                _workflowHistoryMigration,
                _workflowHistoryTableMigration,
                _workflowAmountMigration,
                _workflowAmountHistoryMigration,
                _workflowApprovalUserMigration,
                _workflowApprovalUserHistoryMigration
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
            results["PODocType"] = new { count = await _poDocTypeMigration.MigrateAsync(), success = true };
            results["POCondition"] = new { count = await _poConditionMigration.MigrateAsync(), success = true };
            results["Workflow"] = new { count = await _workflowMigration.MigrateAsync(), success = true };
            results["WorkflowHistory"] = new { count = await _workflowHistoryMigration.MigrateAsync(), success = true };
            results["WorkflowHistoryTable"] = new { count = await _workflowHistoryTableMigration.MigrateAsync(), success = true };
            results["WorkflowAmount"] = new { count = await _workflowAmountMigration.MigrateAsync(), success = true };
            results["WorkflowAmountHistory"] = new { count = await _workflowAmountHistoryMigration.MigrateAsync(), success = true };
            results["WorkflowApprovalUser"] = new { count = await _workflowApprovalUserMigration.MigrateAsync(), success = true };
            results["WorkflowApprovalUserHistory"] = new { count = await _workflowApprovalUserHistoryMigration.MigrateAsync(), success = true };

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

    [HttpGet("validate-workflow-data/{table}")]
    public async Task<IActionResult> ValidateWorkflowData(string table)
    {
        try
        {
            object validation;
            
            if (table.ToLower() == "workflowamount")
            {
                validation = await _workflowAmountMigration.ValidateSourceDataAsync();
            }
            else if (table.ToLower() == "workflowhistorytable")
            {
                validation = await _workflowHistoryTableMigration.ValidateSourceDataAsync();
            }
            else
            {
                return Json(new { success = false, error = "Validation not available for this table" });
            }
            
            return Json(new { success = true, validation = validation });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpGet("validate-workflow-constraints")]
    public async Task<IActionResult> ValidateWorkflowConstraints()
    {
        try
        {
            var validation = await _workflowAmountMigration.ValidateForeignKeyConstraintsAsync();
            return Json(new { success = true, validation = validation });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost("migrate-workflow-without-transaction")]
    public async Task<IActionResult> MigrateWorkflowWithoutTransaction()
    {
        try
        {
            var recordCount = await _workflowAmountMigration.MigrateWithoutTransactionAsync();
            return Json(new { success = true, message = $"Workflow amount migration completed without transaction. {recordCount} records migrated." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpGet("validate-workflow-amount")]
    public async Task<IActionResult> ValidateWorkflowAmount()
    {
        try
        {
            var sourceValidation = await _workflowAmountMigration.ValidateSourceDataAsync();
            var foreignKeyValidation = await _workflowAmountMigration.ValidateForeignKeyConstraintsAsync();
            var numericAnalysis = await _workflowAmountMigration.AnalyzeNumericFieldIssuesAsync();
            var tableSchema = await _workflowAmountMigration.GetTargetTableSchemaAsync();
            
            return Json(new 
            { 
                success = true, 
                sourceValidation = sourceValidation,
                foreignKeyValidation = foreignKeyValidation,
                numericAnalysis = numericAnalysis,
                tableSchema = tableSchema,
                timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost("migrate-workflow-amount-no-transaction")]
    public async Task<IActionResult> MigrateWorkflowAmountNoTransaction()
    {
        try
        {
            var recordCount = await _workflowAmountMigration.MigrateWithoutTransactionAsync();
            return Json(new { success = true, message = $"WorkflowAmount migration completed without transaction. {recordCount} records migrated." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost]
    [Route("MigrateSupplierInactive")]
    public async Task<IActionResult> MigrateSupplierInactive()
    {
        int migrated = await _supplierInactiveMigration.MigrateAsync();
        return Ok(new { migrated });
    }

    [HttpPost]
    [Route("MigrateSupplierOtherContact")]
    public async Task<IActionResult> MigrateSupplierOtherContact()
    {
        int migrated = await _supplierOtherContactMigration.MigrateAsync();
        return Ok(new { migrated });
    }

    [HttpPost("migrate-workflow-history-no-transaction")]
    public async Task<IActionResult> MigrateWorkflowHistoryNoTransaction()
    {
        try
        {
            var recordCount = await _workflowHistoryTableMigration.MigrateWithoutTransactionAsync();
            return Json(new { success = true, message = $"WorkflowHistory migration completed without transaction. {recordCount} records migrated." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost("truncate-table/{table}")]
    public async Task<IActionResult> TruncateTable(string table)
    {
        try
        {
            // Map table names to PostgreSQL table names
            var tableMapping = new Dictionary<string, string>
            {
                { "uom", "uom_master" },
                { "plant", "plant_master" },
                { "currency", "currency_master" },
                { "country", "country_master" },
                { "material_group", "material_group_master" },
                { "materialgroup", "material_group_master" },
                { "purchase_group", "purchase_group_master" },
                { "purchasegroup", "purchase_group_master" },
                { "payment_term", "payment_term_master" },
                { "paymentterm", "payment_term_master" },
                { "material", "material_master" },
                { "event", "event_master" },
                { "eventmaster", "event_master" },
                { "tax", "tax_master" },
                { "users", "users" },
                { "erp_pr_lines", "erp_pr_lines" },
                { "incoterm", "incoterm_master" },
                { "po_doc_type", "po_doc_type_master" },
                { "podoctype", "po_doc_type_master" },
                { "po_condition", "po_condition_master" },
                { "pocondition", "po_condition_master" },
                { "workflow", "workflow_master" },
                { "workflow_history", "workflow_master_history" },
                { "workflow_history_table", "workflow_history" },
                { "workflow_amount", "workflow_amount" },
                { "workflow_amount_history", "workflow_amount_history" },
                { "workflow_approval_user", "workflow_approval_user" },
                { "workflow_approval_user_history", "workflow_approval_user_history" }
            };

            if (!tableMapping.TryGetValue(table.ToLower(), out var pgTableName))
            {
                return Json(new { success = false, error = $"Unknown table: {table}" });
            }

            // Get PostgreSQL connection string from configuration
            var connectionString = _configuration.GetConnectionString("PostgreSql");
            
            using (var conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();
                
                // Special handling for event_master - also truncate event_setting
                if (pgTableName == "event_master")
                {
                    // Truncate event_setting first (child table)
                    using (var cmdSetting = new Npgsql.NpgsqlCommand("TRUNCATE TABLE event_setting CASCADE", conn))
                    {
                        await cmdSetting.ExecuteNonQueryAsync();
                    }
                    
                    // Then truncate event_master
                    using (var cmdMaster = new Npgsql.NpgsqlCommand("TRUNCATE TABLE event_master CASCADE", conn))
                    {
                        await cmdMaster.ExecuteNonQueryAsync();
                    }
                    
                    return Json(new { success = true, message = "Tables 'event_master' and 'event_setting' truncated successfully (CASCADE)" });
                }
                
                // Use TRUNCATE CASCADE to handle foreign key constraints
                var truncateCommand = $"TRUNCATE TABLE {pgTableName} CASCADE";
                
                using (var cmd = new Npgsql.NpgsqlCommand(truncateCommand, conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            return Json(new { success = true, message = $"Table '{pgTableName}' truncated successfully (CASCADE)" });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    [HttpPost("event-setting/migrate")]
    public async Task<IActionResult> MigrateEventSettings()
    {
        try
        {
            var migratedCount = await _eventSettingMigrationService.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_setting records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_setting migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }
}
public class MigrationRequest
{
    public required string Table { get; set; }
}