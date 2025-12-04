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
    private readonly IConfiguration _configuration;
    private readonly EventSettingMigrationService _eventSettingMigrationService;
    private readonly EventScheduleMigrationService _eventScheduleMigrationService;
    private readonly EventScheduleHistoryMigrationService _eventScheduleHistoryMigrationService;
    private readonly ErpCurrencyExchangeRateMigration _erpCurrencyExchangeRateMigration;
    private readonly AuctionMinMaxTargetPriceMigration _auctionMinMaxTargetPriceMigration;
    private readonly EventPriceBidColumnsMigration _eventPriceBidColumnsMigration;
    private readonly EventFreezeCurrencyMigration _eventFreezeCurrencyMigration;
    private readonly EventPublishMigration _eventPublishMigration;
    private readonly EventSupplierPriceBidMigration _eventSupplierPriceBidMigration;
    private readonly EventSupplierLineItemMigration _eventSupplierLineItemMigration;
    private readonly SourceListMasterMigration _sourceListMasterMigration;
    private readonly PriceBidChargesMasterMigration _priceBidChargesMasterMigration;
    private readonly PoHeaderMigration _poHeaderMigration;
    private readonly PoLineMigration _poLineMigration;
    private readonly SupplierBankDetailsMigration _supplierBankDetailsMigration;
    private readonly SupplierEventPriceBidColumnsMigration _supplierEventPriceBidColumnsMigration;
    private readonly SupplierPriceBidLotChargesMigration _supplierPriceBidLotChargesMigration;
    private readonly SupplierPriceBidLotPriceMigration _supplierPriceBidLotPriceMigration;
    private readonly SupplierPriceBidDocumentMigration _supplierPriceBidDocumentMigration;
    private readonly SupplierPriceBoqItemsMigration _supplierPriceBoqItemsMigration;
    private readonly SupplierPriceBidNonPricingMigration _supplierPriceBidNonPricingMigration;
    private readonly EventCommunicationSenderMigration _eventCommunicationSenderMigration;
    private readonly EventCommunicationReceiverMigration _eventCommunicationReceiverMigration;
    private readonly EventCommunicationAttachmentMigration _eventCommunicationAttachmentMigration;
    private readonly NfaClarificationMigration _nfaClarificationMigration;
    private readonly NfaBoqItemsMigration _nfaBoqItemsMigration;
    private readonly NfaAttachmentsMigration _nfaAttachmentsMigration;
    private readonly NfaPoConditionMigration _nfaPoConditionMigration;
    private readonly NfaWorkflowMigration _nfaWorkflowMigration;
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
        IHubContext<MigrationProgressHub> hubContext,
        IConfiguration configuration,
        EventSettingMigrationService eventSettingMigrationService,
        EventScheduleMigrationService eventScheduleMigrationService,
        EventScheduleHistoryMigrationService eventScheduleHistoryMigrationService,
        ErpCurrencyExchangeRateMigration erpCurrencyExchangeRateMigration,
        AuctionMinMaxTargetPriceMigration auctionMinMaxTargetPriceMigration,
        EventPriceBidColumnsMigration eventPriceBidColumnsMigration,
        EventFreezeCurrencyMigration eventFreezeCurrencyMigration,
        EventPublishMigration eventPublishMigration,
        EventSupplierPriceBidMigration eventSupplierPriceBidMigration,
        EventSupplierLineItemMigration eventSupplierLineItemMigration,
        SourceListMasterMigration sourceListMasterMigration,
        PriceBidChargesMasterMigration priceBidChargesMasterMigration,
        PoHeaderMigration poHeaderMigration,
        PoLineMigration poLineMigration,
        SupplierBankDetailsMigration supplierBankDetailsMigration,
        SupplierEventPriceBidColumnsMigration supplierEventPriceBidColumnsMigration,
        SupplierPriceBidLotChargesMigration supplierPriceBidLotChargesMigration,
        SupplierPriceBidLotPriceMigration supplierPriceBidLotPriceMigration,
        SupplierPriceBidDocumentMigration supplierPriceBidDocumentMigration,
        SupplierPriceBoqItemsMigration supplierPriceBoqItemsMigration,
        SupplierPriceBidNonPricingMigration supplierPriceBidNonPricingMigration,
        EventCommunicationSenderMigration eventCommunicationSenderMigration,
        EventCommunicationReceiverMigration eventCommunicationReceiverMigration,
        EventCommunicationAttachmentMigration eventCommunicationAttachmentMigration,
        NfaClarificationMigration nfaClarificationMigration,
        NfaBoqItemsMigration nfaBoqItemsMigration,
        NfaAttachmentsMigration nfaAttachmentsMigration,
        NfaPoConditionMigration nfaPoConditionMigration,
        NfaWorkflowMigration nfaWorkflowMigration,
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
        _hubContext = hubContext;
        _configuration = configuration;
        _eventSettingMigrationService = eventSettingMigrationService;
        _eventScheduleMigrationService = eventScheduleMigrationService;
        _eventScheduleHistoryMigrationService = eventScheduleHistoryMigrationService;
        _erpCurrencyExchangeRateMigration = erpCurrencyExchangeRateMigration;
        _auctionMinMaxTargetPriceMigration = auctionMinMaxTargetPriceMigration;
        _eventPriceBidColumnsMigration = eventPriceBidColumnsMigration;
        _eventFreezeCurrencyMigration = eventFreezeCurrencyMigration;
        _eventPublishMigration = eventPublishMigration;
        _eventSupplierPriceBidMigration = eventSupplierPriceBidMigration;
        _eventSupplierLineItemMigration = eventSupplierLineItemMigration;
        _sourceListMasterMigration = sourceListMasterMigration;
        _priceBidChargesMasterMigration = priceBidChargesMasterMigration;
        _poHeaderMigration = poHeaderMigration;
        _poLineMigration = poLineMigration;
        _supplierBankDetailsMigration = supplierBankDetailsMigration;
        _supplierEventPriceBidColumnsMigration = supplierEventPriceBidColumnsMigration;
        _supplierPriceBidLotChargesMigration = supplierPriceBidLotChargesMigration;
        _supplierPriceBidLotPriceMigration = supplierPriceBidLotPriceMigration;
        _supplierPriceBidDocumentMigration = supplierPriceBidDocumentMigration;
        _supplierPriceBoqItemsMigration = supplierPriceBoqItemsMigration;
        _supplierPriceBidNonPricingMigration = supplierPriceBidNonPricingMigration;
        _eventCommunicationSenderMigration = eventCommunicationSenderMigration;
        _eventCommunicationReceiverMigration = eventCommunicationReceiverMigration;
        _eventCommunicationAttachmentMigration = eventCommunicationAttachmentMigration;
        _nfaClarificationMigration = nfaClarificationMigration;
        _nfaBoqItemsMigration = nfaBoqItemsMigration;
        _nfaAttachmentsMigration = nfaAttachmentsMigration;
        _nfaPoConditionMigration = nfaPoConditionMigration;
        _nfaWorkflowMigration = nfaWorkflowMigration;
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
            new { name = "eventschedule", description = "TBL_EVENTSCHEDULEAR to event_schedule" },
            new { name = "eventschedulehistory", description = "TBL_EVENTSCHEDULEAR to event_schedule_history" },
            new { name = "erpcurrencyexchangerate", description = "TBL_CurrencyConversionMaster to erp_currency_exchange_rate" },
            new { name = "auctionminmaxtargetprice", description = "TBL_MinMaxTargetPrice to auction_min_max_target_price" },
            new { name = "eventpricebidcolumns", description = "TBL_PB_BUYER to event_price_bid_columns (unpivot headers)" },
            new { name = "eventfreezecurrency", description = "TBL_FreezeCurrency to event_freeze_currency" },
            new { name = "eventpublish", description = "TBL_PublishEventAutoMailSend to event_publish" },
            new { name = "eventsupplierpricebid", description = "TBL_PB_SUPPLIER to event_supplier_price_bid (aggregated)" },
            new { name = "eventsupplierlineitem", description = "TBL_PB_SUPPLIER to event_supplier_line_item" },
            new { name = "sourcelistmaster", description = "tbl_InfoRecord to sourcelist_master" },
            new { name = "pricebidchargesmaster", description = "TBL_PB_OTHERCHARGESMST to price_bid_charges_master" },
            new { name = "poheader", description = "TBL_POMain to po_header" },
            new { name = "poline", description = "TBL_PO_Sub to po_line" },
            new { name = "supplierbankdetails", description = "tbl_VendorBankDetail to supplier_bank_deails" },
            new { name = "suppliereventpricebidcolumns", description = "TBL_PB_SUPPLIER to supplier_event_price_bid_columns (HEADER unpivot)" },
            new { name = "supplierpricebidlotcharges", description = "TBL_PB_SUPPLIEROTHERCHARGES to supplier_price_bid_lot_charges" },
            new { name = "supplierpricebidlotprice", description = "TBL_PB_SUPPLIERLotPrice to supplier_price_bid_lot_price" },
            new { name = "supplierpricebiddocument", description = "TBL_PB_SUPPLIER_ATTACHMENT to supplier_price_bid_document" },
            new { name = "supplierpriceboqitems", description = "TBL_PB_SUPPLIER_SUB to supplier_price_boq_items (multi-table join)" },
            new { name = "supplierpricebidnonpricing", description = "TBL_PB_SUPPLIERNonPricing to supplier_price_bid_non_pricing" },
            new { name = "eventcommunicationsender", description = "TBL_MAILMSGMAIN to event_communication_sender" },
            new { name = "eventcommunicationreceiver", description = "TBL_MAILMSGSUB to event_communication_receiver" },
            new { name = "eventcommunicationattachment", description = "TBL_MailAttachment to event_communication_attachment (with document lookup)" },
            new { name = "nfaclarification", description = "TBL_NFA_Clarification to nfa_clarification" },
            new { name = "nfaboqitems", description = "Tbl_AwardEventItemSub to nfa_boq_items (multi-table join)" },
            new { name = "nfaattachments", description = "TBL_QCSPOMailAttachmentFile + TBL_STANDALONENFAATTACHMENT to nfa_attachments" },
            new { name = "nfapocondition", description = "TBL_AwardEventPoCondition to nfa_po_condition" },
            new { name = "nfaworkflow", description = "TBL_StandAloneQCSApprovalAuthority to nfa_workflow" },
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
        else if (table.ToLower() == "eventsetting")
        {
            var mappings = _eventSettingMigrationService.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventschedule")
        {
            var mappings = _eventScheduleMigrationService.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventschedulehistory")
        {
            var mappings = _eventScheduleHistoryMigrationService.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "erpcurrencyexchangerate")
        {
            var mappings = _erpCurrencyExchangeRateMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "auctionminmaxtargetprice")
        {
            var mappings = _auctionMinMaxTargetPriceMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventpricebidcolumns")
        {
            var mappings = _eventPriceBidColumnsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventfreezecurrency")
        {
            var mappings = _eventFreezeCurrencyMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventpublish")
        {
            var mappings = _eventPublishMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventsupplierpricebid")
        {
            var mappings = _eventSupplierPriceBidMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventsupplierlineitem")
        {
            var mappings = _eventSupplierLineItemMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "sourcelistmaster")
        {
            var mappings = _sourceListMasterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "pricebidchargesmaster")
        {
            var mappings = _priceBidChargesMasterMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "poheader")
        {
            var mappings = _poHeaderMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "poline")
        {
            var mappings = _poLineMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierbankdetails")
        {
            var mappings = _supplierBankDetailsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "suppliereventpricebidcolumns")
        {
            var mappings = _supplierEventPriceBidColumnsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierpricebidlotcharges")
        {
            var mappings = _supplierPriceBidLotChargesMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierpricebidlotprice")
        {
            var mappings = _supplierPriceBidLotPriceMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierpricebiddocument")
        {
            var mappings = _supplierPriceBidDocumentMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierpriceboqitems")
        {
            var mappings = _supplierPriceBoqItemsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "supplierpricebidnonpricing")
        {
            var mappings = _supplierPriceBidNonPricingMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventcommunicationsender")
        {
            var mappings = _eventCommunicationSenderMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventcommunicationreceiver")
        {
            var mappings = _eventCommunicationReceiverMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "eventcommunicationattachment")
        {
            var mappings = _eventCommunicationAttachmentMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "nfaclarification")
        {
            var mappings = _nfaClarificationMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "nfaboqitems")
        {
            var mappings = _nfaBoqItemsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "nfaattachments")
        {
            var mappings = _nfaAttachmentsMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "nfapocondition")
        {
            var mappings = _nfaPoConditionMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "nfaworkflow")
        {
            var mappings = _nfaWorkflowMigration.GetMappings();
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
            else if (request.Table.ToLower() == "eventsetting")
            {
                recordCount = await _eventSettingMigrationService.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventschedule")
            {
                recordCount = await _eventScheduleMigrationService.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventschedulehistory")
            {
                recordCount = await _eventScheduleHistoryMigrationService.MigrateAsync();
            }
            else if (request.Table.ToLower() == "erpcurrencyexchangerate")
            {
                recordCount = await _erpCurrencyExchangeRateMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "auctionminmaxtargetprice")
            {
                recordCount = await _auctionMinMaxTargetPriceMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventpricebidcolumns")
            {
                recordCount = await _eventPriceBidColumnsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventfreezecurrency")
            {
                recordCount = await _eventFreezeCurrencyMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventpublish")
            {
                recordCount = await _eventPublishMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventsupplierpricebid")
            {
                recordCount = await _eventSupplierPriceBidMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventsupplierlineitem")
            {
                recordCount = await _eventSupplierLineItemMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "sourcelistmaster")
            {
                recordCount = await _sourceListMasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "pricebidchargesmaster")
            {
                recordCount = await _priceBidChargesMasterMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "poheader")
            {
                recordCount = await _poHeaderMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "poline")
            {
                recordCount = await _poLineMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierbankdetails")
            {
                recordCount = await _supplierBankDetailsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "suppliereventpricebidcolumns")
            {
                recordCount = await _supplierEventPriceBidColumnsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierpricebidlotcharges")
            {
                recordCount = await _supplierPriceBidLotChargesMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierpricebidlotprice")
            {
                recordCount = await _supplierPriceBidLotPriceMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierpricebiddocument")
            {
                recordCount = await _supplierPriceBidDocumentMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierpriceboqitems")
            {
                recordCount = await _supplierPriceBoqItemsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "supplierpricebidnonpricing")
            {
                recordCount = await _supplierPriceBidNonPricingMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventcommunicationsender")
            {
                recordCount = await _eventCommunicationSenderMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventcommunicationreceiver")
            {
                recordCount = await _eventCommunicationReceiverMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "eventcommunicationattachment")
            {
                recordCount = await _eventCommunicationAttachmentMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "nfaclarification")
            {
                recordCount = await _nfaClarificationMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "nfaboqitems")
            {
                recordCount = await _nfaBoqItemsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "nfaattachments")
            {
                recordCount = await _nfaAttachmentsMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "nfapocondition")
            {
                recordCount = await _nfaPoConditionMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "nfaworkflow")
            {
                recordCount = await _nfaWorkflowMigration.MigrateAsync();
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

    [HttpPost("erp-currency-exchange-rate/migrate")]
    public async Task<IActionResult> MigrateErpCurrencyExchangeRate()
    {
        try
        {
            var migratedCount = await _erpCurrencyExchangeRateMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} erp_currency_exchange_rate records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during erp_currency_exchange_rate migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("auction-min-max-target-price/migrate")]
    public async Task<IActionResult> MigrateAuctionMinMaxTargetPrice()
    {
        try
        {
            var migratedCount = await _auctionMinMaxTargetPriceMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} auction_min_max_target_price records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during auction_min_max_target_price migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-price-bid-columns/migrate")]
    public async Task<IActionResult> MigrateEventPriceBidColumns()
    {
        try
        {
            var migratedCount = await _eventPriceBidColumnsMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_price_bid_columns source records (multiple rows generated)." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_price_bid_columns migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-freeze-currency/migrate")]
    public async Task<IActionResult> MigrateEventFreezeCurrency()
    {
        try
        {
            var migratedCount = await _eventFreezeCurrencyMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_freeze_currency records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_freeze_currency migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-publish/migrate")]
    public async Task<IActionResult> MigrateEventPublish()
    {
        try
        {
            var migratedCount = await _eventPublishMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_publish records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_publish migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-supplier-price-bid/migrate")]
    public async Task<IActionResult> MigrateEventSupplierPriceBid()
    {
        try
        {
            var migratedCount = await _eventSupplierPriceBidMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_supplier_price_bid records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_supplier_price_bid migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-supplier-line-item/migrate")]
    public async Task<IActionResult> MigrateEventSupplierLineItem()
    {
        try
        {
            var migratedCount = await _eventSupplierLineItemMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_supplier_line_item records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_supplier_line_item migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("sourcelist-master/migrate")]
    public async Task<IActionResult> MigrateSourceListMaster()
    {
        try
        {
            var migratedCount = await _sourceListMasterMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} sourcelist_master records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during sourcelist_master migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("price-bid-charges-master/migrate")]
    public async Task<IActionResult> MigratePriceBidChargesMaster()
    {
        try
        {
            var migratedCount = await _priceBidChargesMasterMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} price_bid_charges_master records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during price_bid_charges_master migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("po-header/migrate")]
    public async Task<IActionResult> MigratePoHeader()
    {
        try
        {
            var migratedCount = await _poHeaderMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} po_header records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during po_header migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("po-line/migrate")]
    public async Task<IActionResult> MigratePoLine()
    {
        try
        {
            var migratedCount = await _poLineMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} po_line records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during po_line migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-bank-details/migrate")]
    public async Task<IActionResult> MigrateSupplierBankDetails()
    {
        try
        {
            var migratedCount = await _supplierBankDetailsMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_bank_deails records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_bank_details migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-event-price-bid-columns/migrate")]
    public async Task<IActionResult> MigrateSupplierEventPriceBidColumns()
    {
        try
        {
            var migratedCount = await _supplierEventPriceBidColumnsMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_event_price_bid_columns records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_event_price_bid_columns migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-price-bid-lot-charges/migrate")]
    public async Task<IActionResult> MigrateSupplierPriceBidLotCharges()
    {
        try
        {
            var migratedCount = await _supplierPriceBidLotChargesMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_price_bid_lot_charges records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_price_bid_lot_charges migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-price-bid-lot-price/migrate")]
    public async Task<IActionResult> MigrateSupplierPriceBidLotPrice()
    {
        try
        {
            var migratedCount = await _supplierPriceBidLotPriceMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_price_bid_lot_price records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_price_bid_lot_price migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-price-bid-document/migrate")]
    public async Task<IActionResult> MigrateSupplierPriceBidDocument()
    {
        try
        {
            var migratedCount = await _supplierPriceBidDocumentMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_price_bid_document records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_price_bid_document migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-price-boq-items/migrate")]
    public async Task<IActionResult> MigrateSupplierPriceBoqItems()
    {
        try
        {
            var migratedCount = await _supplierPriceBoqItemsMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_price_boq_items records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_price_boq_items migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("supplier-price-bid-non-pricing/migrate")]
    public async Task<IActionResult> MigrateSupplierPriceBidNonPricing()
    {
        try
        {
            var migratedCount = await _supplierPriceBidNonPricingMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} supplier_price_bid_non_pricing records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during supplier_price_bid_non_pricing migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-communication-sender/migrate")]
    public async Task<IActionResult> MigrateEventCommunicationSender()
    {
        try
        {
            var migratedCount = await _eventCommunicationSenderMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_communication_sender records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_communication_sender migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-communication-receiver/migrate")]
    public async Task<IActionResult> MigrateEventCommunicationReceiver()
    {
        try
        {
            var migratedCount = await _eventCommunicationReceiverMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_communication_receiver records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_communication_receiver migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("event-communication-attachment/migrate")]
    public async Task<IActionResult> MigrateEventCommunicationAttachment()
    {
        try
        {
            var migratedCount = await _eventCommunicationAttachmentMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} event_communication_attachment records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during event_communication_attachment migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("nfa-clarification/migrate")]
    public async Task<IActionResult> MigrateNfaClarification()
    {
        try
        {
            var migratedCount = await _nfaClarificationMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} nfa_clarification records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during nfa_clarification migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("nfa-boq-items/migrate")]
    public async Task<IActionResult> MigrateNfaBoqItems()
    {
        try
        {
            var migratedCount = await _nfaBoqItemsMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} nfa_boq_items records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during nfa_boq_items migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("nfa-attachments/migrate")]
    public async Task<IActionResult> MigrateNfaAttachments()
    {
        try
        {
            var migratedCount = await _nfaAttachmentsMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} nfa_attachments records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during nfa_attachments migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("nfa-po-condition/migrate")]
    public async Task<IActionResult> MigrateNfaPoCondition()
    {
        try
        {
            var migratedCount = await _nfaPoConditionMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} nfa_po_condition records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during nfa_po_condition migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }

    [HttpPost("nfa-workflow/migrate")]
    public async Task<IActionResult> MigrateNfaWorkflow()
    {
        try
        {
            var migratedCount = await _nfaWorkflowMigration.MigrateAsync();
            return Ok(new { Message = $"Successfully migrated {migratedCount} nfa_workflow records." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during nfa_workflow migration.");
            return StatusCode(500, new { Error = "An error occurred during migration." });
        }
    }
}
public class MigrationRequest
{
    public required string Table { get; set; }
}