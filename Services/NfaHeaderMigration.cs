using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class NfaHeaderMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<NfaHeaderMigration> _logger;

    public NfaHeaderMigration(IConfiguration configuration, ILogger<NfaHeaderMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override string SelectQuery => @"
        SELECT
            AWARDEVENTMAINID,
            AwardNo,
            EventId,
            SupplierId,
            TotalLotCharges,
            TotalCSValue,
            TotalGSTAmount,
            TotalLineOtherCharges,
            TotalDiscount,
            GrandDiscountPer,
            ItemTotal,
            PaymentTermsId,
            IncotermId,
            POType,
            PONo,
            POHeaderText,
            PurchasingOrgId,
            PurchasingGroupId,
            PaymentTermsRemarks,
            IncotermRemarks,
            SONo,
            OrgVendorId,
            IsSilverAnglePO,
            AWARDEVENTMAINREFID,
            SummaryNote,
            ClosingNegotiationNote,
            ARCMainId,
            PRtoARCPOMAINId,
            HeaderNote,
            IsNFAChecked,
            AttachmentPath,
            AttachmentName,
            IsStandAlone,
            Purpose,
            ClientsAPId,
            PlantId,
            TechnicallyAppoved,
            CommercialTC,
            Meetingdeliverytimelinexpectation,
            Meetingqualityrequirement,
            Purchaseordershouldbeallottedto1supplier,
            StandaloneBriefNote,
            AdditionalDocument,
            SALES_PERSON,
            TELEPHONE,
            YOUR_REFERENCE,
            OUR_REFERENCE,
            CurrencyId,
            TypeofCategory,
            POCreatedDate,
            ME2mScreenshotName,
            ME2mScreenshotPath,
            OEMVendorId,
            CreatedBy,
            CreatedDate,
            BuyerRemarks,
            ApprovalStatus
        FROM TBL_AWARDEVENTMAIN
        ORDER BY AWARDEVENTMAINID";

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
            summary_note,
            closing_negotiation_note,
            arcpo_id,
            header_note,
            nfa_for_review,
            auction_chart_file_path,
            auction_chart_file_name,
            standalone_nfa_flag,
            company_id,
            plant_id,
            plant_code,
            technically_approved,
            commercial_tc,
            meeting_delivery_timeline_expectation,
            meeting_quality_requirement,
            purchase_order_should_be_allotted_to_i1_supplier,
            brief_note,
            nfa_primt_file_path,
            nfa_primt_file_name,
            sales_person_name,
            sales_phone_number,
            your_reference,
            our_reference,
            currency_id,
            type_of_category_id,
            po_post_date,
            price_variation_report_name,
            price_variation_report_path,
            original_supplier_id,
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
            @summary_note,
            @closing_negotiation_note,
            @arcpo_id,
            @header_note,
            @nfa_for_review,
            @auction_chart_file_path,
            @auction_chart_file_name,
            @standalone_nfa_flag,
            @company_id,
            @plant_id,
            @plant_code,
            @technically_approved,
            @commercial_tc,
            @meeting_delivery_timeline_expectation,
            @meeting_quality_requirement,
            @purchase_order_should_be_allotted_to_i1_supplier,
            @brief_note,
            @nfa_primt_file_path,
            @nfa_primt_file_name,
            @sales_person_name,
            @sales_phone_number,
            @your_reference,
            @our_reference,
            @currency_id,
            @type_of_category_id,
            @po_post_date,
            @price_variation_report_name,
            @price_variation_report_path,
            @original_supplier_id,
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
            summary_note = EXCLUDED.summary_note,
            closing_negotiation_note = EXCLUDED.closing_negotiation_note,
            arcpo_id = EXCLUDED.arcpo_id,
            header_note = EXCLUDED.header_note,
            nfa_for_review = EXCLUDED.nfa_for_review,
            auction_chart_file_path = EXCLUDED.auction_chart_file_path,
            auction_chart_file_name = EXCLUDED.auction_chart_file_name,
            standalone_nfa_flag = EXCLUDED.standalone_nfa_flag,
            company_id = EXCLUDED.company_id,
            plant_id = EXCLUDED.plant_id,
            plant_code = EXCLUDED.plant_code,
            technically_approved = EXCLUDED.technically_approved,
            commercial_tc = EXCLUDED.commercial_tc,
            meeting_delivery_timeline_expectation = EXCLUDED.meeting_delivery_timeline_expectation,
            meeting_quality_requirement = EXCLUDED.meeting_quality_requirement,
            purchase_order_should_be_allotted_to_i1_supplier = EXCLUDED.purchase_order_should_be_allotted_to_i1_supplier,
            brief_note = EXCLUDED.brief_note,
            nfa_primt_file_path = EXCLUDED.nfa_primt_file_path,
            nfa_primt_file_name = EXCLUDED.nfa_primt_file_name,
            sales_person_name = EXCLUDED.sales_person_name,
            sales_phone_number = EXCLUDED.sales_phone_number,
            your_reference = EXCLUDED.your_reference,
            our_reference = EXCLUDED.our_reference,
            currency_id = EXCLUDED.currency_id,
            type_of_category_id = EXCLUDED.type_of_category_id,
            po_post_date = EXCLUDED.po_post_date,
            price_variation_report_name = EXCLUDED.price_variation_report_name,
            price_variation_report_path = EXCLUDED.price_variation_report_path,
            original_supplier_id = EXCLUDED.original_supplier_id,
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
            "Direct",  // summary_note
            "Direct",  // closing_negotiation_note
            "Direct",  // arcpo_id
            "Direct",  // header_note
            "Direct",  // nfa_for_review
            "Direct",  // auction_chart_file_path
            "Direct",  // auction_chart_file_name
            "Direct",  // standalone_nfa_flag
            "Direct",  // company_id
            "Direct",  // plant_id
            "Lookup",  // plant_code
            "Direct",  // technically_approved
            "Direct",  // commercial_tc
            "Direct",  // meeting_delivery_timeline_expectation
            "Direct",  // meeting_quality_requirement
            "Direct",  // purchase_order_should_be_allotted_to_i1_supplier
            "Direct",  // brief_note
            "Direct",  // nfa_primt_file_path
            "Direct",  // nfa_primt_file_name
            "Direct",  // sales_person_name
            "Direct",  // sales_phone_number
            "Direct",  // your_reference
            "Direct",  // our_reference
            "Direct",  // currency_id
            "Direct",  // type_of_category_id
            "Direct",  // po_post_date
            "Direct",  // price_variation_report_name
            "Direct",  // price_variation_report_path
            "Direct",  // original_supplier_id
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
            new { source = "SupplierId", logic = "SupplierId -> supplier_id (Foreign key to supplier_master - SupplierId)", target = "supplier_id" },
            new { source = "TotalLotCharges", logic = "TotalLotCharges -> lot_total (SubTotal)", target = "lot_total" },
            new { source = "TotalCSValue", logic = "TotalCSValue -> total_before_tax (TotalNFAValue)", target = "total_before_tax" },
            new { source = "TotalGSTAmount", logic = "TotalGSTAmount -> total_tax_value (TotalTaxValue)", target = "total_tax_value" },
            new { source = "TotalLineOtherCharges", logic = "TotalLineOtherCharges -> total_after_tax (TotalItemOtherCharges)", target = "total_after_tax" },
            new { source = "TotalDiscount", logic = "TotalDiscount -> item_total (DiscountValue)", target = "item_total" },
            new { source = "ItemTotal", logic = "ItemTotal -> item_total (ItemTotal)", target = "item_total" },
            new { source = "PaymentTermsId", logic = "PaymentTermsId -> payment_term_id (Foreign key to payment_term_master - PaymentTermId)", target = "payment_term_id" },
            new { source = "-", logic = "payment_term_code -> Lookup from payment_term_master (PaymentTermCode)", target = "payment_term_code" },
            new { source = "IncotermId", logic = "IncotermId -> incoterm_id (Foreign key to incoterm_master - IncoTermId)", target = "incoterm_id" },
            new { source = "-", logic = "incoterm_code -> Lookup from incoterm_master (IncoTermCode)", target = "incoterm_code" },
            new { source = "-", logic = "po_doc_type_id -> Lookup from po_doc_type_master via POType (PODocTypeId)", target = "po_doc_type_id" },
            new { source = "POType", logic = "POType -> po_doc_type_code (PODocTypeCode)", target = "po_doc_type_code" },
            new { source = "PONo", logic = "PONo -> po_number (PONumber)", target = "po_number" },
            new { source = "POHeaderText", logic = "POHeaderText -> po_header_text (POHeaderText)", target = "po_header_text" },
            new { source = "PurchasingOrgId", logic = "PurchasingOrgId -> purchase_organization_id (Foreign key to purchase_organization_master - PurchaseOrganizationId)", target = "purchase_organization_id" },
            new { source = "-", logic = "purchase_organization_code -> Lookup from purchase_organization_master (PurchaseOrganizationCode)", target = "purchase_organization_code" },
            new { source = "PurchasingGroupId", logic = "PurchasingGroupId -> purchase_group_id (Foreign key to purchase_group_master - PurchaseGroupId)", target = "purchase_group_id" },
            new { source = "-", logic = "purchase_group_code -> Lookup from purchase_group_master (PurchaseGroupCode)", target = "purchase_group_code" },
            new { source = "IncotermRemarks", logic = "IncotermRemarks -> incoterm_remark (IncotermsRemarks)", target = "incoterm_remark" },
            new { source = "AWARDEVENTMAINREFID", logic = "AWARDEVENTMAINREFID -> repeat_poid (RepeatPOID)", target = "repeat_poid" },
            new { source = "SummaryNote", logic = "SummaryNote -> summary_note (SummeryNote)", target = "summary_note" },
            new { source = "ClosingNegotiationNote", logic = "ClosingNegotiationNote -> closing_negotiation_note (ClosingNegotiationNote)", target = "closing_negotiation_note" },
            new { source = "ARCMainId", logic = "ARCMainId -> arcpo_id (ARCId - Primary mapping)", target = "arcpo_id" },
            new { source = "PRtoARCPOMAINId", logic = "PRtoARCPOMAINId -> arcpo_id (ARCPOId - Alternative/Fallback)", target = "arcpo_id" },
            new { source = "HeaderNote", logic = "HeaderNote -> header_note (HeaderNote)", target = "header_note" },
            new { source = "IsNFAChecked", logic = "IsNFAChecked -> nfa_for_review (NFAforReview)", target = "nfa_for_review" },
            new { source = "AttachmentPath", logic = "AttachmentPath -> auction_chart_file_path (AuctionChartFilePath)", target = "auction_chart_file_path" },
            new { source = "AttachmentName", logic = "AttachmentName -> auction_chart_file_name (AuctionChartFileName)", target = "auction_chart_file_name" },
            new { source = "IsStandAlone", logic = "IsStandAlone -> standalone_nfa_flag (StandAloneNFAFlag)", target = "standalone_nfa_flag" },
            new { source = "ClientsAPId", logic = "ClientsAPId -> company_id (Foreign key to company_master - CompanyId)", target = "company_id" },
            new { source = "PlantId", logic = "PlantId -> plant_id (Foreign key to plant_master - PlantId)", target = "plant_id" },
            new { source = "-", logic = "plant_code -> Lookup from plant_master (PlantCode)", target = "plant_code" },
            new { source = "TechnicallyAppoved", logic = "TechnicallyAppoved -> technically_approved (TechnicallyApproved)", target = "technically_approved" },
            new { source = "CommercialTC", logic = "CommercialTC -> commercial_tc (CommercialJustification)", target = "commercial_tc" },
            new { source = "Meetingdeliverytimelinexpectation", logic = "Meetingdeliverytimelinexpectation -> meeting_delivery_timeline_expectation (Meetingdeliverytimelineexpectation)", target = "meeting_delivery_timeline_expectation" },
            new { source = "Meetingqualityrequirement", logic = "Meetingqualityrequirement -> meeting_quality_requirement (Meetingqualityrequirement)", target = "meeting_quality_requirement" },
            new { source = "Purchaseordershouldbeallottedto1supplier", logic = "Purchaseordershouldbeallottedto1supplier -> purchase_order_should_be_allotted_to_i1_supplier (Purchaseordershouldbeallottedto1supplier)", target = "purchase_order_should_be_allotted_to_i1_supplier" },
            new { source = "StandaloneBriefNote", logic = "StandaloneBriefNote -> brief_note (BriefNote)", target = "brief_note" },
            new { source = "-", logic = "nfa_primt_file_path -> Empty/NULL (NFAPrintFilePath)", target = "nfa_primt_file_path" },
            new { source = "AdditionalDocument", logic = "AdditionalDocument -> nfa_primt_file_name (NFAPrintFileName)", target = "nfa_primt_file_name" },
            new { source = "SALES_PERSON", logic = "SALES_PERSON -> sales_person_name (SalesPersonName)", target = "sales_person_name" },
            new { source = "TELEPHONE", logic = "TELEPHONE -> sales_phone_number (SalesPhoneNumber)", target = "sales_phone_number" },
            new { source = "YOUR_REFERENCE", logic = "YOUR_REFERENCE -> your_reference (YourReference)", target = "your_reference" },
            new { source = "OUR_REFERENCE", logic = "OUR_REFERENCE -> our_reference (OurReference)", target = "our_reference" },
            new { source = "CurrencyId", logic = "CurrencyId -> currency_id (Foreign key to currency_master - CurrencyId)", target = "currency_id" },
            new { source = "TypeofCategory", logic = "TypeofCategory -> type_of_category_id (Foreign key to type_of_category_master - TypeofCategoryId)", target = "type_of_category_id" },
            new { source = "POCreatedDate", logic = "POCreatedDate -> po_post_date (POPostDate)", target = "po_post_date" },
            new { source = "ME2mScreenshotName", logic = "ME2mScreenshotName -> price_variation_report_name (PriceVariationReportName)", target = "price_variation_report_name" },
            new { source = "ME2mScreenshotPath", logic = "ME2mScreenshotPath -> price_variation_report_path (PriceVariationReportPath)", target = "price_variation_report_path" },
            new { source = "OEMVendorId", logic = "OEMVendorId -> original_supplier_id (Foreign key to supplier_master - OriginalSupplierId)", target = "original_supplier_id" },
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
        _logger.LogInformation("Starting NFA Header migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load lookup data for codes
            var paymentTermCodes = await LoadPaymentTermCodesAsync(pgConn);
            var incotermCodes = await LoadIncotermCodesAsync(pgConn);
            var poDocTypes = await LoadPoDocTypesAsync(pgConn);
            var purchaseOrgCodes = await LoadPurchaseOrgCodesAsync(pgConn);
            var purchaseGroupCodes = await LoadPurchaseGroupCodesAsync(pgConn);
            var plantCodes = await LoadPlantCodesAsync(pgConn);

            _logger.LogInformation($"Loaded lookup data: PaymentTerms={paymentTermCodes.Count}, Incoterms={incotermCodes.Count}, PODocTypes={poDocTypes.Count}");

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
                    _logger.LogWarning("Skipping record - AWARDEVENTMAINID is NULL");
                    continue;
                }

                int awardEventMainIdValue = Convert.ToInt32(awardEventMainId);

                // Skip duplicates
                if (processedIds.Contains(awardEventMainIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Get lookup values
                var paymentTermId = reader["PaymentTermsId"];
                string? paymentTermCode = null;
                if (paymentTermId != DBNull.Value && paymentTermCodes.ContainsKey(Convert.ToInt32(paymentTermId)))
                {
                    paymentTermCode = paymentTermCodes[Convert.ToInt32(paymentTermId)];
                }

                var incotermId = reader["IncotermId"];
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

                var record = new Dictionary<string, object>
                {
                    ["nfa_header_id"] = awardEventMainIdValue,
                    ["nfa_number"] = reader["AwardNo"] ?? DBNull.Value,
                    ["event_id"] = reader["EventId"] ?? DBNull.Value,
                    ["supplier_id"] = reader["SupplierId"] ?? DBNull.Value,
                    ["lot_total"] = reader["TotalLotCharges"] ?? DBNull.Value,
                    ["total_before_tax"] = reader["TotalCSValue"] ?? DBNull.Value,
                    ["total_tax_value"] = reader["TotalGSTAmount"] ?? DBNull.Value,
                    ["total_after_tax"] = reader["TotalLineOtherCharges"] ?? DBNull.Value,
                    ["item_total"] = reader["ItemTotal"] ?? DBNull.Value,
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
                    ["incoterm_remark"] = reader["IncotermRemarks"] ?? DBNull.Value,
                    ["repeat_poid"] = reader["AWARDEVENTMAINREFID"] ?? DBNull.Value,
                    ["summary_note"] = reader["SummaryNote"] ?? DBNull.Value,
                    ["closing_negotiation_note"] = reader["ClosingNegotiationNote"] ?? DBNull.Value,
                    ["arcpo_id"] = reader["ARCMainId"] != DBNull.Value ? reader["ARCMainId"] : (reader["PRtoARCPOMAINId"] ?? DBNull.Value),
                    ["header_note"] = reader["HeaderNote"] ?? DBNull.Value,
                    ["nfa_for_review"] = reader["IsNFAChecked"] ?? DBNull.Value,
                    ["auction_chart_file_path"] = reader["AttachmentPath"] ?? DBNull.Value,
                    ["auction_chart_file_name"] = reader["AttachmentName"] ?? DBNull.Value,
                    ["standalone_nfa_flag"] = reader["IsStandAlone"] ?? DBNull.Value,
                    ["company_id"] = reader["ClientsAPId"] ?? DBNull.Value,
                    ["plant_id"] = plantId ?? DBNull.Value,
                    ["plant_code"] = plantCode ?? (object)DBNull.Value,
                    ["technically_approved"] = reader["TechnicallyAppoved"] ?? DBNull.Value,
                    ["commercial_tc"] = reader["CommercialTC"] ?? DBNull.Value,
                    ["meeting_delivery_timeline_expectation"] = reader["Meetingdeliverytimelinexpectation"] ?? DBNull.Value,
                    ["meeting_quality_requirement"] = reader["Meetingqualityrequirement"] ?? DBNull.Value,
                    ["purchase_order_should_be_allotted_to_i1_supplier"] = reader["Purchaseordershouldbeallottedto1supplier"] ?? DBNull.Value,
                    ["brief_note"] = reader["StandaloneBriefNote"] ?? DBNull.Value,
                    ["nfa_primt_file_path"] = DBNull.Value,
                    ["nfa_primt_file_name"] = reader["AdditionalDocument"] ?? DBNull.Value,
                    ["sales_person_name"] = reader["SALES_PERSON"] ?? DBNull.Value,
                    ["sales_phone_number"] = reader["TELEPHONE"] ?? DBNull.Value,
                    ["your_reference"] = reader["YOUR_REFERENCE"] ?? DBNull.Value,
                    ["our_reference"] = reader["OUR_REFERENCE"] ?? DBNull.Value,
                    ["currency_id"] = reader["CurrencyId"] ?? DBNull.Value,
                    ["type_of_category_id"] = reader["TypeofCategory"] ?? DBNull.Value,
                    ["po_post_date"] = reader["POCreatedDate"] ?? DBNull.Value,
                    ["price_variation_report_name"] = reader["ME2mScreenshotName"] ?? DBNull.Value,
                    ["price_variation_report_path"] = reader["ME2mScreenshotPath"] ?? DBNull.Value,
                    ["original_supplier_id"] = reader["OEMVendorId"] ?? DBNull.Value,
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
                    int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"NFA Header migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

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

    private async Task<int> InsertBatchWithTransactionAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn)
    {
        int insertedCount = 0;

        using var transaction = await pgConn.BeginTransactionAsync();
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

            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records. Batch rolled back.");
        }

        return insertedCount;
    }
}
