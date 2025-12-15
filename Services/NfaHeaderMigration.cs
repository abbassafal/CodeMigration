using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class NfaHeaderMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<NfaHeaderMigration> _logger;
    private MigrationLogger? _migrationLogger;
    private HashSet<int> _validCurrencyIds = new HashSet<int>();
    private HashSet<int> _validSupplierIds = new HashSet<int>();
    private int _defaultCurrencyId = 1; // Will be loaded from DB

    public NfaHeaderMigration(IConfiguration configuration, ILogger<NfaHeaderMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;
    
    // Load valid supplier IDs at the start of migration
    private async Task LoadValidSupplierIdsAsync(NpgsqlConnection pgConn)
    {
        _validSupplierIds.Clear();
        using var cmd = new NpgsqlCommand("SELECT supplier_id FROM supplier_master WHERE supplier_id IS NOT NULL ORDER BY supplier_id", pgConn);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _validSupplierIds.Add(reader.GetInt32(0));
        }
        
        _logger.LogInformation($"Loaded {_validSupplierIds.Count} valid supplier IDs");
    }
    
    // Load valid currency IDs at the start of migration
    private async Task LoadValidCurrencyIdsAsync(NpgsqlConnection pgConn)
    {
        _validCurrencyIds.Clear();
        using var cmd = new NpgsqlCommand("SELECT currency_id FROM currency_master WHERE currency_id IS NOT NULL ORDER BY currency_id", pgConn);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _validCurrencyIds.Add(reader.GetInt32(0));
        }
        
        // Set default to first available currency, or try to find INR/USD
        if (_validCurrencyIds.Count > 0)
        {
            // Prefer common currencies: 1 (often USD), 86 (INR if exists)
            if (_validCurrencyIds.Contains(1)) _defaultCurrencyId = 1;
            else if (_validCurrencyIds.Contains(86)) _defaultCurrencyId = 86;
            else _defaultCurrencyId = _validCurrencyIds.First();
        }
        
        _logger.LogInformation($"Loaded {_validCurrencyIds.Count} valid currency IDs. Default currency_id: {_defaultCurrencyId}");
    }
    
    // Helper to validate and fix currency ID
    private int ValidateCurrencyId(int? currencyId)
    {
        if (currencyId.HasValue && _validCurrencyIds.Contains(currencyId.Value))
            return currencyId.Value;
        return _defaultCurrencyId;
    }

    protected override string SelectQuery => @"
        SELECT
            TBL_AWARDEVENTMAIN.AWARDEVENTMAINID,
            TBL_AWARDEVENTMAIN.AwardNo,
            TBL_AWARDEVENTMAIN.AwardStatus,
            TBL_AWARDEVENTMAIN.EventId,
            TBL_AWARDEVENTMAIN.VendorId,
            TBL_AWARDEVENTMAIN.TotalLotCharges,
            TBL_AWARDEVENTMAIN.TotalCSValue,
            TBL_AWARDEVENTMAIN.TotalGSTAmount,
            TBL_AWARDEVENTMAIN.TotalLineOtherCharges,
            TBL_AWARDEVENTMAIN.TotalDiscount,
            TBL_AWARDEVENTMAIN.GrandDiscountPer,
            TBL_AWARDEVENTMAIN.ItemTotal,
            TBL_AWARDEVENTMAIN.PaymentTermsId,
            TBL_AWARDEVENTMAIN.IncotermsId,
            TBL_AWARDEVENTMAIN.POType,
            TBL_AWARDEVENTMAIN.PONo,
            TBL_AWARDEVENTMAIN.POHeaderText,
            TBL_AWARDEVENTMAIN.PurchasingOrgId,
            TBL_AWARDEVENTMAIN.PurchasingGroupId,
            TBL_AWARDEVENTMAIN.PaymentTermsRemarks,
            TBL_AWARDEVENTMAIN.IncotermsRemarks,
            TBL_AWARDEVENTMAIN.SONo,
            TBL_AWARDEVENTMAIN.OrgVendorId,
            TBL_AWARDEVENTMAIN.IsSilverAnglePO,
            TBL_AWARDEVENTMAIN.AWARDEVENTMAINREFID,
            TBL_AWARDEVENTMAIN.SummaryNote,
            TBL_AWARDEVENTMAIN.ClosingNegotiationNote,
            TBL_AWARDEVENTMAIN.ARCMainId,
            TBL_AWARDEVENTMAIN.PRtoARCPOMAINId,
            TBL_AWARDEVENTMAIN.HeaderNote,
            TBL_AWARDEVENTMAIN.IsNFAChecked,
            TBL_AWARDEVENTMAIN.AttachmentPath,
            TBL_AWARDEVENTMAIN.AttachmentName,
            TBL_AWARDEVENTMAIN.IsStandAlone,
            TBL_AWARDEVENTMAIN.Purpose,
            TBL_AWARDEVENTMAIN.ClientSAPId,
            TBL_AWARDEVENTMAIN.PlantId,
            TBL_AWARDEVENTMAIN.TechnicallyAppoved,
            TBL_AWARDEVENTMAIN.CommerclalTC,
            TBL_AWARDEVENTMAIN.Meetingdeliverytimelineexpectation,
            TBL_AWARDEVENTMAIN.Meetingqualityrequirement,
            TBL_AWARDEVENTMAIN.PurchaseordershouldbeallottedtoL1supplier,
            TBL_AWARDEVENTMAIN.TechnicallyAppovedJustification,
            TBL_AWARDEVENTMAIN.CommerclalTCJustification,
            TBL_AWARDEVENTMAIN.MeetingdeliverytimelineexpectationJustification,
            TBL_AWARDEVENTMAIN.MeetingqualityrequirementJustification,
            TBL_AWARDEVENTMAIN.PurchaseordershouldbeallottedtoL1supplierJustification,
            TBL_AWARDEVENTMAIN.StandAloneBriefNote,
            TBL_AWARDEVENTMAIN.AdditionalDocument,
            TBL_AWARDEVENTMAIN.SALES_PERSON,
            TBL_AWARDEVENTMAIN.TELEPHONE,
            TBL_AWARDEVENTMAIN.YOUR_REFERENCE,
            TBL_AWARDEVENTMAIN.OUR_REFERENCE,
            TBL_AWARDEVENTMAIN.CurrencyId,
            TBL_EVENTSELECTEDUSER.VendorCurrencyId,
            TBL_AWARDEVENTMAIN.TypeofCategory,
            TBL_AWARDEVENTMAIN.POCreatedDate,
            TBL_AWARDEVENTMAIN.ME2mScreenshotName,
            TBL_AWARDEVENTMAIN.ME2mScreenshotPath,
            TBL_AWARDEVENTMAIN.OEMVendorId,
            TBL_AWARDEVENTMAIN.CreatedBy,
            TBL_AWARDEVENTMAIN.CreatedDate,
            TBL_AWARDEVENTMAIN.BuyerRemarks,
            TBL_AWARDEVENTMAIN.ApprovalStatus
        FROM TBL_AWARDEVENTMAIN
        LEFT JOIN TBL_EVENTSELECTEDUSER ON TBL_EVENTSELECTEDUSER.EVENTID = TBL_AWARDEVENTMAIN.EventId 
            AND TBL_EVENTSELECTEDUSER.USERID = TBL_AWARDEVENTMAIN.VendorId 
            AND TBL_EVENTSELECTEDUSER.UserType = 'Vendor'
        ORDER BY TBL_AWARDEVENTMAIN.AWARDEVENTMAINID";

    protected override string InsertQuery => @"
        INSERT INTO nfa_header (
            nfa_header_id,
            nfa_number,
            event_id,
            supplier_id,
            lot_total,
            total_before_tax,
            total_tax_value,
            total_after_tax,
            item_total,
            payment_term_id,
            payment_term_code,
            incoterm_id,
            incoterm_code,
            po_doc_type_id,
            po_doc_type_code,
            po_number,
            po_header_text,
            purchase_organization_id,
            purchase_organization_code,
            purchase_group_id,
            purchase_group_code,
            incoterm_remark,
            repeat_poid,
            summery_note,
            closing_negotiation_note,
            arc_id,
            arcpo_id,
            header_note,
            nfa_for_review,
            auction_chart_file_path,
            auction_chart_file_name,
            standalone_nfa_flag,
            company_id,
            plant_id,
            plant_code,
            technically_appoved,
            commerclal_tc,
            meeting_delivery_timeline_expectation,
            meeting_quality_requirement,
            purchase_order_should_be_allotted_to_l1_supplier,
            technically_appoved_justification,
            commerclal_tc_justification,
            meeting_delivery_timeline_expectation_justification,
            meeting_quality_requirement_justification,
            purchase_order_should_be_allotted_to_l1_supplier_justification,
            brief_note,
            nfa_printt_file_path,
            nfa_printt_file_name,
            sales_person_name,
            sales_phone_number,
            your_referance,
            our_referance,
            currency_id,
            type_of_category_id,
            po_post_date,
            price_variation_report_name,
            price_variation_report_path,
            orignal_supplier_id,
            purchase_remarks,
            nfa_approval_status,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @nfa_header_id,
            @nfa_number,
            @event_id,
            @supplier_id,
            @lot_total,
            @total_before_tax,
            @total_tax_value,
            @total_after_tax,
            @item_total,
            @payment_term_id,
            @payment_term_code,
            @incoterm_id,
            @incoterm_code,
            @po_doc_type_id,
            @po_doc_type_code,
            @po_number,
            @po_header_text,
            @purchase_organization_id,
            @purchase_organization_code,
            @purchase_group_id,
            @purchase_group_code,
            @incoterm_remark,
            @repeat_poid,
            @summery_note,
            @closing_negotiation_note,
            @arc_id,
            @arcpo_id,
            @header_note,
            @nfa_for_review,
            @auction_chart_file_path,
            @auction_chart_file_name,
            @standalone_nfa_flag,
            @company_id,
            @plant_id,
            @plant_code,
            @technically_appoved,
            @commerclal_tc,
            @meeting_delivery_timeline_expectation,
            @meeting_quality_requirement,
            @purchase_order_should_be_allotted_to_l1_supplier,
            @technically_appoved_justification,
            @commerclal_tc_justification,
            @meeting_delivery_timeline_expectation_justification,
            @meeting_quality_requirement_justification,
            @purchase_order_should_be_allotted_to_l1_supplier_justification,
            @brief_note,
            @nfa_printt_file_path,
            @nfa_printt_file_name,
            @sales_person_name,
            @sales_phone_number,
            @your_referance,
            @our_referance,
            @currency_id,
            @type_of_category_id,
            @po_post_date,
            @price_variation_report_name,
            @price_variation_report_path,
            @orignal_supplier_id,
            @purchase_remarks,
            @nfa_approval_status,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (nfa_header_id) DO UPDATE SET
            nfa_number = EXCLUDED.nfa_number,
            event_id = EXCLUDED.event_id,
            supplier_id = EXCLUDED.supplier_id,
            lot_total = EXCLUDED.lot_total,
            total_before_tax = EXCLUDED.total_before_tax,
            total_tax_value = EXCLUDED.total_tax_value,
            total_after_tax = EXCLUDED.total_after_tax,
            item_total = EXCLUDED.item_total,
            payment_term_id = EXCLUDED.payment_term_id,
            payment_term_code = EXCLUDED.payment_term_code,
            incoterm_id = EXCLUDED.incoterm_id,
            incoterm_code = EXCLUDED.incoterm_code,
            po_doc_type_id = EXCLUDED.po_doc_type_id,
            po_doc_type_code = EXCLUDED.po_doc_type_code,
            po_number = EXCLUDED.po_number,
            po_header_text = EXCLUDED.po_header_text,
            purchase_organization_id = EXCLUDED.purchase_organization_id,
            purchase_organization_code = EXCLUDED.purchase_organization_code,
            purchase_group_id = EXCLUDED.purchase_group_id,
            purchase_group_code = EXCLUDED.purchase_group_code,
            incoterm_remark = EXCLUDED.incoterm_remark,
            repeat_poid = EXCLUDED.repeat_poid,
            summery_note = EXCLUDED.summery_note,
            closing_negotiation_note = EXCLUDED.closing_negotiation_note,
            arc_id = EXCLUDED.arc_id,
            arcpo_id = EXCLUDED.arcpo_id,
            header_note = EXCLUDED.header_note,
            nfa_for_review = EXCLUDED.nfa_for_review,
            auction_chart_file_path = EXCLUDED.auction_chart_file_path,
            auction_chart_file_name = EXCLUDED.auction_chart_file_name,
            standalone_nfa_flag = EXCLUDED.standalone_nfa_flag,
            company_id = EXCLUDED.company_id,
            plant_id = EXCLUDED.plant_id,
            plant_code = EXCLUDED.plant_code,
            technically_appoved = EXCLUDED.technically_appoved,
            commerclal_tc = EXCLUDED.commerclal_tc,
            meeting_delivery_timeline_expectation = EXCLUDED.meeting_delivery_timeline_expectation,
            meeting_quality_requirement = EXCLUDED.meeting_quality_requirement,
            purchase_order_should_be_allotted_to_l1_supplier = EXCLUDED.purchase_order_should_be_allotted_to_l1_supplier,
            technically_appoved_justification = EXCLUDED.technically_appoved_justification,
            commerclal_tc_justification = EXCLUDED.commerclal_tc_justification,
            meeting_delivery_timeline_expectation_justification = EXCLUDED.meeting_delivery_timeline_expectation_justification,
            meeting_quality_requirement_justification = EXCLUDED.meeting_quality_requirement_justification,
            purchase_order_should_be_allotted_to_l1_supplier_justification = EXCLUDED.purchase_order_should_be_allotted_to_l1_supplier_justification,
            brief_note = EXCLUDED.brief_note,
            nfa_printt_file_path = EXCLUDED.nfa_printt_file_path,
            nfa_printt_file_name = EXCLUDED.nfa_printt_file_name,
            sales_person_name = EXCLUDED.sales_person_name,
            sales_phone_number = EXCLUDED.sales_phone_number,
            your_referance = EXCLUDED.your_referance,
            our_referance = EXCLUDED.our_referance,
            currency_id = EXCLUDED.currency_id,
            type_of_category_id = EXCLUDED.type_of_category_id,
            po_post_date = EXCLUDED.po_post_date,
            price_variation_report_name = EXCLUDED.price_variation_report_name,
            price_variation_report_path = EXCLUDED.price_variation_report_path,
            orignal_supplier_id = EXCLUDED.orignal_supplier_id,
            purchase_remarks = EXCLUDED.purchase_remarks,
            nfa_approval_status = EXCLUDED.nfa_approval_status,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // nfa_header_id
            "Direct",  // nfa_number
            "Direct",  // event_id
            "Direct",  // supplier_id
            "Direct",  // lot_total
            "Direct",  // total_before_tax
            "Direct",  // total_tax_value
            "Direct",  // total_after_tax
            "Direct",  // item_total
            "Direct",  // payment_term_id
            "Lookup",  // payment_term_code
            "Direct",  // incoterm_id
            "Lookup",  // incoterm_code
            "Lookup",  // po_doc_type_id
            "Direct",  // po_doc_type_code
            "Direct",  // po_number
            "Direct",  // po_header_text
            "Direct",  // purchase_organization_id
            "Lookup",  // purchase_organization_code
            "Direct",  // purchase_group_id
            "Lookup",  // purchase_group_code
            "Direct",  // incoterm_remark
            "Direct",  // repeat_poid
            "Direct",  // summery_note
            "Direct",  // closing_negotiation_note
            "Direct",  // arc_id
            "Direct",  // arcpo_id
            "Direct",  // header_note
            "Direct",  // nfa_for_review
            "Direct",  // auction_chart_file_path
            "Direct",  // auction_chart_file_name
            "Direct",  // standalone_nfa_flag
            "Direct",  // company_id
            "Direct",  // plant_id
            "Lookup",  // plant_code
            "Direct",  // technically_appoved
            "Direct",  // commerclal_tc
            "Direct",  // meeting_delivery_timeline_expectation
            "Direct",  // meeting_quality_requirement
            "Direct",  // purchase_order_should_be_allotted_to_l1_supplier
            "Fixed",   // technically_appoved_justification
            "Fixed",   // commerclal_tc_justification
            "Fixed",   // meeting_delivery_timeline_expectation_justification
            "Fixed",   // meeting_quality_requirement_justification
            "Fixed",   // purchase_order_should_be_allotted_to_l1_supplier_justification
            "Direct",  // brief_note
            "Fixed",   // nfa_printt_file_path
            "Direct",  // nfa_printt_file_name
            "Direct",  // sales_person_name
            "Direct",  // sales_phone_number
            "Direct",  // your_referance
            "Direct",  // our_referance
            "Direct",  // currency_id
            "Direct",  // type_of_category_id
            "Direct",  // po_post_date
            "Direct",  // price_variation_report_name
            "Direct",  // price_variation_report_path
            "Direct",  // orignal_supplier_id
            "Direct",  // purchase_remarks
            "Direct",  // nfa_approval_status
            "Direct",  // created_by
            "Direct",  // created_date
            "Fixed",   // modified_by
            "Fixed",   // modified_date
            "Fixed",   // is_deleted
            "Fixed",   // deleted_by
            "Fixed"    // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "AWARDEVENTMAINID", logic = "AWARDEVENTMAINID -> nfa_header_id (Primary key - NFAHeaderID)", target = "nfa_header_id" },
            new { source = "AwardNo", logic = "AwardNo -> nfa_number (NFANumber)", target = "nfa_number" },
            new { source = "EventId", logic = "EventId -> event_id (Foreign key to event_master - EventId)", target = "event_id" },
            new { source = "VendorId", logic = "VendorId -> supplier_id (Foreign key to supplier_master - SupplierId)", target = "supplier_id" },
            new { source = "TotalLotCharges", logic = "TotalLotCharges -> lot_total (SubTotal)", target = "lot_total" },
            new { source = "ItemTotal, TotalDiscount, TotalLotCharges", logic = "(ItemTotal - TotalDiscount) + TotalLotCharges -> total_before_tax", target = "total_before_tax" },
            new { source = "TotalGSTAmount", logic = "TotalGSTAmount -> total_tax_value (TotalTaxValue)", target = "total_tax_value" },
            new { source = "ItemTotal, TotalDiscount, TotalLotCharges, TotalGSTAmount", logic = "((ItemTotal - TotalDiscount) + TotalLotCharges) + TotalGSTAmount -> total_after_tax", target = "total_after_tax" },
            new { source = "ItemTotal, TotalDiscount", logic = "ItemTotal - TotalDiscount -> item_total", target = "item_total" },
            new { source = "PaymentTermsId", logic = "PaymentTermsId -> payment_term_id (Foreign key to payment_term_master - PaymentTermId)", target = "payment_term_id" },
            new { source = "-", logic = "payment_term_code -> Lookup from payment_term_master (PaymentTermCode)", target = "payment_term_code" },
            new { source = "IncotermsId", logic = "IncotermsId -> incoterm_id (Foreign key to incoterm_master - IncoTermId)", target = "incoterm_id" },
            new { source = "-", logic = "incoterm_code -> Lookup from incoterm_master (IncoTermCode)", target = "incoterm_code" },
            new { source = "-", logic = "po_doc_type_id -> Lookup from po_doc_type_master via POType (PODocTypeId)", target = "po_doc_type_id" },
            new { source = "POType", logic = "POType -> po_doc_type_code (PODocTypeCode)", target = "po_doc_type_code" },
            new { source = "PONo", logic = "PONo -> po_number (PONumber)", target = "po_number" },
            new { source = "POHeaderText", logic = "POHeaderText -> po_header_text (POHeaderText)", target = "po_header_text" },
            new { source = "PurchasingOrgId", logic = "PurchasingOrgId -> purchase_organization_id (Foreign key to purchase_organization_master - PurchaseOrganizationId)", target = "purchase_organization_id" },
            new { source = "-", logic = "purchase_organization_code -> Lookup from purchase_organization_master (PurchaseOrganizationCode)", target = "purchase_organization_code" },
            new { source = "PurchasingGroupId", logic = "PurchasingGroupId -> purchase_group_id (Foreign key to purchase_group_master - PurchaseGroupId)", target = "purchase_group_id" },
            new { source = "-", logic = "purchase_group_code -> Lookup from purchase_group_master (PurchaseGroupCode)", target = "purchase_group_code" },
            new { source = "IncotermsRemarks", logic = "IncotermsRemarks -> incoterm_remark (IncotermsRemarks)", target = "incoterm_remark" },
            new { source = "AWARDEVENTMAINREFID", logic = "AWARDEVENTMAINREFID -> repeat_poid (RepeatPOID)", target = "repeat_poid" },
            new { source = "SummaryNote", logic = "SummaryNote -> summery_note (SummeryNote)", target = "summery_note" },
            new { source = "ClosingNegotiationNote", logic = "ClosingNegotiationNote -> closing_negotiation_note (ClosingNegotiationNote)", target = "closing_negotiation_note" },
            new { source = "ARCMainId", logic = "ARCMainId -> arc_id (ARCId)", target = "arc_id" },
            new { source = "PRtoARCPOMAINId", logic = "PRtoARCPOMAINId -> arcpo_id (ARCPOId)", target = "arcpo_id" },
            new { source = "HeaderNote", logic = "HeaderNote -> header_note (HeaderNote)", target = "header_note" },
            new { source = "IsNFAChecked", logic = "IsNFAChecked -> nfa_for_review (NFAforReview)", target = "nfa_for_review" },
            new { source = "AttachmentPath", logic = "AttachmentPath -> auction_chart_file_path (AuctionChartFilePath)", target = "auction_chart_file_path" },
            new { source = "AttachmentName", logic = "AttachmentName -> auction_chart_file_name (AuctionChartFileName)", target = "auction_chart_file_name" },
            new { source = "IsStandAlone", logic = "IsStandAlone -> standalone_nfa_flag (StandAloneNFAFlag)", target = "standalone_nfa_flag" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Foreign key to company_master - CompanyId)", target = "company_id" },
            new { source = "PlantId", logic = "PlantId -> plant_id (Foreign key to plant_master - PlantId)", target = "plant_id" },
            new { source = "-", logic = "plant_code -> Lookup from plant_master (PlantCode)", target = "plant_code" },
            new { source = "TechnicallyAppoved", logic = "TechnicallyAppoved -> technically_appoved (TechnicallyAppoved)", target = "technically_appoved" },
            new { source = "CommerclalTC", logic = "CommerclalTC -> commerclal_tc (CommerclalTC)", target = "commerclal_tc" },
            new { source = "Meetingdeliverytimelineexpectation", logic = "Meetingdeliverytimelineexpectation -> meeting_delivery_timeline_expectation (Meetingdeliverytimelineexpectation)", target = "meeting_delivery_timeline_expectation" },
            new { source = "Meetingqualityrequirement", logic = "Meetingqualityrequirement -> meeting_quality_requirement (Meetingqualityrequirement)", target = "meeting_quality_requirement" },
            new { source = "PurchaseordershouldbeallottedtoL1supplier", logic = "PurchaseordershouldbeallottedtoL1supplier -> purchase_order_should_be_allotted_to_l1_supplier (PurchaseordershouldbeallottedtoL1supplier)", target = "purchase_order_should_be_allotted_to_l1_supplier" },
            new { source = "TechnicallyAppovedJustification", logic = "TechnicallyAppovedJustification -> technically_appoved_justification", target = "technically_appoved_justification" },
            new { source = "CommerclalTCJustification", logic = "CommerclalTCJustification -> commerclal_tc_justification", target = "commerclal_tc_justification" },
            new { source = "MeetingdeliverytimelineexpectationJustification", logic = "MeetingdeliverytimelineexpectationJustification -> meeting_delivery_timeline_expectation_justification", target = "meeting_delivery_timeline_expectation_justification" },
            new { source = "MeetingqualityrequirementJustification", logic = "MeetingqualityrequirementJustification -> meeting_quality_requirement_justification", target = "meeting_quality_requirement_justification" },
            new { source = "PurchaseordershouldbeallottedtoL1supplierJustification", logic = "PurchaseordershouldbeallottedtoL1supplierJustification -> purchase_order_should_be_allotted_to_l1_supplier_justification", target = "purchase_order_should_be_allotted_to_l1_supplier_justification" },
            new { source = "StandAloneBriefNote", logic = "StandAloneBriefNote -> brief_note (BriefNote)", target = "brief_note" },
            new { source = "-", logic = "nfa_printt_file_path -> Empty/NULL (NFAPrintFilePath)", target = "nfa_printt_file_path" },
            new { source = "AdditionalDocument", logic = "AdditionalDocument -> nfa_printt_file_name (NFAPrintFileName)", target = "nfa_printt_file_name" },
            new { source = "SALES_PERSON", logic = "SALES_PERSON -> sales_person_name (SalesPersonName)", target = "sales_person_name" },
            new { source = "TELEPHONE", logic = "TELEPHONE -> sales_phone_number (SalesPhoneNumber)", target = "sales_phone_number" },
            new { source = "YOUR_REFERENCE", logic = "YOUR_REFERENCE -> your_referance (YourReference)", target = "your_referance" },
            new { source = "OUR_REFERENCE", logic = "OUR_REFERENCE -> our_referance (OurReference)", target = "our_referance" },
            new { source = "CurrencyId, VendorCurrencyId", logic = "COALESCE(CurrencyId, VendorCurrencyId, 86) -> currency_id (Fallback: INR=86)", target = "currency_id" },
            new { source = "TypeofCategory", logic = "TypeofCategory -> type_of_category_id (Foreign key to type_of_category_master - TypeofCategoryId)", target = "type_of_category_id" },
            new { source = "POCreatedDate", logic = "POCreatedDate -> po_post_date (POPostDate)", target = "po_post_date" },
            new { source = "ME2mScreenshotName", logic = "ME2mScreenshotName -> price_variation_report_name (PriceVariationReportName)", target = "price_variation_report_name" },
            new { source = "ME2mScreenshotPath", logic = "ME2mScreenshotPath -> price_variation_report_path (PriceVariationReportPath)", target = "price_variation_report_path" },
            new { source = "OEMVendorId", logic = "OEMVendorId -> orignal_supplier_id (Foreign key to supplier_master - OriginalSupplierId)", target = "orignal_supplier_id" },
            new { source = "BuyerRemarks", logic = "BuyerRemarks -> purchase_remarks (PurchaseRemarks)", target = "purchase_remarks" },
            new { source = "ApprovalStatus", logic = "ApprovalStatus -> nfa_approval_status (NFAApprovalStatus)", target = "nfa_approval_status" },
            new { source = "CreatedBy", logic = "CreatedBy -> created_by", target = "created_by" },
            new { source = "CreatedDate", logic = "CreatedDate -> created_date", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "nfa_header");
        _migrationLogger.LogInfo("Starting migration");

        _logger.LogInformation("Starting NFA Header migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;
        var skippedRecordList = new List<(string RecordId, string Reason)>();

        try
        {
            // Load valid currency IDs first
            await LoadValidCurrencyIdsAsync(pgConn);
            
            // Load valid supplier IDs
            await LoadValidSupplierIdsAsync(pgConn);
            
            // Load lookup data for codes
            var paymentTermCodes = await LoadPaymentTermCodesAsync(pgConn);
            var incotermCodes = await LoadIncotermCodesAsync(pgConn);
            var poDocTypes = await LoadPoDocTypesAsync(pgConn);
            var purchaseOrgCodes = await LoadPurchaseOrgCodesAsync(pgConn);
            var purchaseGroupCodes = await LoadPurchaseGroupCodesAsync(pgConn);
            var plantCodes = await LoadPlantCodesAsync(pgConn);

            _logger.LogInformation($"Loaded lookup data: PaymentTerms={paymentTermCodes.Count}, Incoterms={incotermCodes.Count}, PODocTypes={poDocTypes.Count}, Suppliers={_validSupplierIds.Count}");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var awardEventMainId = reader["AWARDEVENTMAINID"];

                // Skip if AWARDEVENTMAINID is NULL
                if (awardEventMainId == DBNull.Value)
                {
                    skippedRecords++;
                    skippedRecordList.Add(("", "AWARDEVENTMAINID is NULL"));
                    _logger.LogWarning("Skipping record - AWARDEVENTMAINID is NULL");
                    continue;
                }

                int awardEventMainIdValue = Convert.ToInt32(awardEventMainId);

                // Skip duplicates
                if (processedIds.Contains(awardEventMainIdValue))
                {
                    skippedRecords++;
                    skippedRecordList.Add((awardEventMainIdValue.ToString(), "Duplicate AWARDEVENTMAINID"));
                    continue;
                }

                // Skip if supplier_id (VendorId) is NULL or 0
                var vendorId = reader["VendorId"];
                if (vendorId == DBNull.Value || Convert.ToInt32(vendorId) == 0)
                {
                    skippedRecords++;
                    skippedRecordList.Add((awardEventMainIdValue.ToString(), "VendorId is NULL or 0"));
                    _logger.LogWarning($"Skipping record {awardEventMainIdValue} - VendorId is NULL or 0");
                    continue;
                }

                // Skip if supplier_id doesn't exist in supplier_master
                int vendorIdValue = Convert.ToInt32(vendorId);
                if (!_validSupplierIds.Contains(vendorIdValue))
                {
                    skippedRecords++;
                    skippedRecordList.Add((awardEventMainIdValue.ToString(), $"VendorId {vendorIdValue} not found in supplier_master"));
                    _logger.LogWarning($"Skipping record {awardEventMainIdValue} - VendorId {vendorIdValue} not found in supplier_master");
                    continue;
                }

                // Skip if company_id (ClientSAPId) is NULL or 0
                var clientSAPId = reader["ClientSAPId"];
                if (clientSAPId == DBNull.Value || Convert.ToInt32(clientSAPId) == 0)
                {
                    skippedRecords++;
                    skippedRecordList.Add((awardEventMainIdValue.ToString(), "ClientSAPId is NULL or 0"));
                    _logger.LogWarning($"Skipping record {awardEventMainIdValue} - ClientSAPId is NULL or 0");
                    continue;
                }

                // Get lookup values
                var paymentTermId = reader["PaymentTermsId"];
                string? paymentTermCode = null;
                if (paymentTermId != DBNull.Value && paymentTermCodes.ContainsKey(Convert.ToInt32(paymentTermId)))
                {
                    paymentTermCode = paymentTermCodes[Convert.ToInt32(paymentTermId)];
                }

                var incotermId = reader["IncotermsId"];
                string? incotermCode = null;
                if (incotermId != DBNull.Value && incotermCodes.ContainsKey(Convert.ToInt32(incotermId)))
                {
                    incotermCode = incotermCodes[Convert.ToInt32(incotermId)];
                }

                var poType = reader["POType"];
                int? poDocTypeId = null;
                if (poType != DBNull.Value && poDocTypes.ContainsKey(poType.ToString()!))
                {
                    poDocTypeId = poDocTypes[poType.ToString()!];
                }

                var purchasingOrgId = reader["PurchasingOrgId"];
                string? purchaseOrgCode = null;
                if (purchasingOrgId != DBNull.Value && purchaseOrgCodes.ContainsKey(Convert.ToInt32(purchasingOrgId)))
                {
                    purchaseOrgCode = purchaseOrgCodes[Convert.ToInt32(purchasingOrgId)];
                }

                var purchasingGroupId = reader["PurchasingGroupId"];
                string? purchaseGroupCode = null;
                if (purchasingGroupId != DBNull.Value && purchaseGroupCodes.ContainsKey(Convert.ToInt32(purchasingGroupId)))
                {
                    purchaseGroupCode = purchaseGroupCodes[Convert.ToInt32(purchasingGroupId)];
                }

                var plantId = reader["PlantId"];
                string? plantCode = null;
                if (plantId != DBNull.Value && plantCodes.ContainsKey(Convert.ToInt32(plantId)))
                {
                    plantCode = plantCodes[Convert.ToInt32(plantId)];
                }

                // Calculate values
                decimal itemTotal = reader["ItemTotal"] != DBNull.Value ? Convert.ToDecimal(reader["ItemTotal"]) : 0;
                decimal totalDiscount = reader["TotalDiscount"] != DBNull.Value ? Convert.ToDecimal(reader["TotalDiscount"]) : 0;
                decimal totalLotCharges = reader["TotalLotCharges"] != DBNull.Value ? Convert.ToDecimal(reader["TotalLotCharges"]) : 0;
                decimal totalGSTAmount = reader["TotalGSTAmount"] != DBNull.Value ? Convert.ToDecimal(reader["TotalGSTAmount"]) : 0;
                
                decimal calculatedItemTotal = itemTotal - totalDiscount;
                decimal calculatedTotalBeforeTax = calculatedItemTotal + totalLotCharges;
                decimal calculatedTotalAfterTax = calculatedTotalBeforeTax + totalGSTAmount;

                var record = new Dictionary<string, object>
                {
                    ["nfa_header_id"] = awardEventMainIdValue,
                    ["nfa_number"] = reader["AwardNo"] ?? DBNull.Value,
                    ["event_id"] = reader["EventId"] ?? DBNull.Value,
                    ["supplier_id"] = reader["VendorId"] ?? DBNull.Value,
                    ["lot_total"] = reader["TotalLotCharges"] ?? DBNull.Value,
                    ["total_before_tax"] = calculatedTotalBeforeTax,
                    ["total_tax_value"] = totalGSTAmount,
                    ["total_after_tax"] = calculatedTotalAfterTax,
                    ["item_total"] = calculatedItemTotal,
                    ["payment_term_id"] = paymentTermId ?? DBNull.Value,
                    ["payment_term_code"] = paymentTermCode ?? (object)DBNull.Value,
                    ["incoterm_id"] = incotermId ?? DBNull.Value,
                    ["incoterm_code"] = incotermCode ?? (object)DBNull.Value,
                    ["po_doc_type_id"] = poDocTypeId.HasValue ? (object)poDocTypeId.Value : DBNull.Value,
                    ["po_doc_type_code"] = poType ?? DBNull.Value,
                    ["po_number"] = reader["PONo"] ?? DBNull.Value,
                    ["po_header_text"] = reader["POHeaderText"] ?? DBNull.Value,
                    ["purchase_organization_id"] = purchasingOrgId ?? DBNull.Value,
                    ["purchase_organization_code"] = purchaseOrgCode ?? (object)DBNull.Value,
                    ["purchase_group_id"] = purchasingGroupId ?? DBNull.Value,
                    ["purchase_group_code"] = purchaseGroupCode ?? (object)DBNull.Value,
                    ["incoterm_remark"] = reader["IncotermsRemarks"] ?? DBNull.Value,
                    ["repeat_poid"] = reader["AWARDEVENTMAINREFID"] ?? DBNull.Value,
                    ["summery_note"] = reader["SummaryNote"] ?? DBNull.Value,
                    ["closing_negotiation_note"] = reader["ClosingNegotiationNote"] ?? DBNull.Value,
                    ["arc_id"] = reader["ARCMainId"] ?? DBNull.Value,
                    ["arcpo_id"] = reader["PRtoARCPOMAINId"] ?? DBNull.Value,
                    ["header_note"] = reader["HeaderNote"] ?? DBNull.Value,
                    ["nfa_for_review"] = reader["IsNFAChecked"] ?? DBNull.Value,
                    ["auction_chart_file_path"] = reader["AttachmentPath"] ?? DBNull.Value,
                    ["auction_chart_file_name"] = reader["AttachmentName"] ?? DBNull.Value,
                    ["standalone_nfa_flag"] = reader["IsStandAlone"] != DBNull.Value ? Convert.ToBoolean(reader["IsStandAlone"]) : false,
                    ["company_id"] = reader["ClientSAPId"] ?? DBNull.Value,
                    ["plant_id"] = plantId ?? DBNull.Value,
                    ["plant_code"] = plantCode ?? (object)DBNull.Value,
                    ["technically_appoved"] = reader["TechnicallyAppoved"] ?? DBNull.Value,
                    ["commerclal_tc"] = reader["CommerclalTC"] ?? DBNull.Value,
                    ["meeting_delivery_timeline_expectation"] = reader["Meetingdeliverytimelineexpectation"] ?? DBNull.Value,
                    ["meeting_quality_requirement"] = reader["Meetingqualityrequirement"] ?? DBNull.Value,
                    ["purchase_order_should_be_allotted_to_l1_supplier"] = reader["PurchaseordershouldbeallottedtoL1supplier"] ?? DBNull.Value,
                    ["technically_appoved_justification"] = reader["TechnicallyAppovedJustification"] ?? DBNull.Value,
                    ["commerclal_tc_justification"] = reader["CommerclalTCJustification"] ?? DBNull.Value,
                    ["meeting_delivery_timeline_expectation_justification"] = reader["MeetingdeliverytimelineexpectationJustification"] ?? DBNull.Value,
                    ["meeting_quality_requirement_justification"] = reader["MeetingqualityrequirementJustification"] ?? DBNull.Value,
                    ["purchase_order_should_be_allotted_to_l1_supplier_justification"] = reader["PurchaseordershouldbeallottedtoL1supplierJustification"] ?? DBNull.Value,
                    ["brief_note"] = reader["StandAloneBriefNote"] ?? DBNull.Value,
                    ["nfa_printt_file_path"] = "/Documents/TechnicalDocuments/"+ (reader["AdditionalDocument"] != DBNull.Value ? reader["AdditionalDocument"].ToString() : ""),
                    ["nfa_printt_file_name"] = reader["AdditionalDocument"] ?? DBNull.Value,
                    ["sales_person_name"] = reader["SALES_PERSON"] ?? DBNull.Value,
                    ["sales_phone_number"] = reader["TELEPHONE"] ?? DBNull.Value,
                    ["your_referance"] = reader["YOUR_REFERENCE"] ?? DBNull.Value,
                    ["our_referance"] = reader["OUR_REFERENCE"] ?? DBNull.Value,
                    ["currency_id"] = ValidateCurrencyId(
                        reader["CurrencyId"] != DBNull.Value && Convert.ToInt32(reader["CurrencyId"]) > 0 
                            ? Convert.ToInt32(reader["CurrencyId"])
                            : (reader["VendorCurrencyId"] != DBNull.Value && Convert.ToInt32(reader["VendorCurrencyId"]) > 0 
                                ? Convert.ToInt32(reader["VendorCurrencyId"]) 
                                : (int?)null)
                    ),
                    ["type_of_category_id"] = reader["TypeofCategory"] ?? DBNull.Value,
                    ["po_post_date"] = reader["POCreatedDate"] ?? DBNull.Value,
                    ["price_variation_report_name"] = reader["ME2mScreenshotName"] ?? DBNull.Value,
                    ["price_variation_report_path"] = reader["ME2mScreenshotPath"] ?? DBNull.Value,
                    ["orignal_supplier_id"] = reader["OEMVendorId"] ?? DBNull.Value,
                    ["purchase_remarks"] = reader["BuyerRemarks"] ?? DBNull.Value,
                    ["nfa_approval_status"] = reader["ApprovalStatus"] ?? DBNull.Value,
                    ["created_by"] = reader["CreatedBy"] ?? DBNull.Value,
                    ["created_date"] = reader["CreatedDate"] ?? DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(awardEventMainIdValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn, transaction);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn, transaction);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"NFA Header migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            // Export migration stats and skipped records to Excel
            MigrationStatsExporter.ExportToExcel(
                "NfaHeaderMigration_Stats.xlsx",
                totalRecords,
                migratedRecords,
                skippedRecords,
                _logger,
                skippedRecordList
            );

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during NFA Header migration");
            throw;
        }
    }

    private async Task<Dictionary<int, string?>> LoadPaymentTermCodesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, string?>();
        try
        {
            var query = "SELECT payment_term_id, payment_term_code FROM payment_term_master WHERE payment_term_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading payment term codes");
        }
        return map;
    }

    private async Task<Dictionary<int, string?>> LoadIncotermCodesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, string?>();
        try
        {
            var query = "SELECT incoterm_id, incoterm_code FROM incoterm_master WHERE incoterm_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading incoterm codes");
        }
        return map;
    }

    private async Task<Dictionary<string, int>> LoadPoDocTypesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<string, int>();
        try
        {
            var query = "SELECT po_doc_type_id, po_doc_type_code FROM po_doc_type_master WHERE po_doc_type_code IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetString(1)] = reader.GetInt32(0);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading po doc types");
        }
        return map;
    }

    private async Task<Dictionary<int, string?>> LoadPurchaseOrgCodesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, string?>();
        try
        {
            var query = "SELECT purchase_organization_id, purchase_organization_code FROM purchase_organization_master WHERE purchase_organization_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading purchase organization codes");
        }
        return map;
    }

    private async Task<Dictionary<int, string?>> LoadPurchaseGroupCodesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, string?>();
        try
        {
            var query = "SELECT purchase_group_id, purchase_group_code FROM purchase_group_master WHERE purchase_group_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading purchase group codes");
        }
        return map;
    }

    private async Task<Dictionary<int, string?>> LoadPlantCodesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, string?>();
        try
        {
            var query = "SELECT plant_id, plant_code FROM plant_master WHERE plant_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading plant codes");
        }
        return map;
    }

    private async Task<int> InsertBatchWithTransactionAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn, NpgsqlTransaction? parentTransaction = null)
    {
        int insertedCount = 0;

        // Use parent transaction if provided, otherwise create a new one
        NpgsqlTransaction? transaction = parentTransaction;
        bool ownTransaction = false;
        
        if (transaction == null)
        {
            transaction = await pgConn.BeginTransactionAsync();
            ownTransaction = true;
        }

        try
        {
            foreach (var record in batch)
            {
                using var cmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);

                foreach (var kvp in record)
                {
                    cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                await cmd.ExecuteNonQueryAsync();
                insertedCount++;
            }

            // Only commit if we created our own transaction
            if (ownTransaction)
            {
                await transaction.CommitAsync();
            }
        }
        catch (Exception ex)
        {
            // Only rollback if we created our own transaction
            if (ownTransaction)
            {
                await transaction.RollbackAsync();
            }
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records. Batch rolled back.");
            throw;
        }
        finally
        {
            // Only dispose if we created our own transaction
            if (ownTransaction && transaction != null)
            {
                await transaction.DisposeAsync();
            }
        }

        return insertedCount;
    }
}
