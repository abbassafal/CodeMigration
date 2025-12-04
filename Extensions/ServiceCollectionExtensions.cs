using Microsoft.Extensions.DependencyInjection;
using DataMigration.Services;

namespace Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMigrationServices(this IServiceCollection services)
    {
        services.AddScoped<SupplierOtherContactMigration>();
    
        services.AddScoped<SupplierInactiveMigration>();
        services.AddScoped<SupplierPaymentIncotermMigration>();
        services.AddScoped<TypeOfCategoryMasterMigration>();
        services.AddScoped<SupplierGroupMasterMigration>();
        services.AddScoped<SupplierMasterMigration>();
        services.AddScoped<ValuationTypeMasterMigration>();
        services.AddScoped<PurchaseOrganizationMasterMigration>();
        services.AddScoped<CompanyMasterMigration>();
        // services.AddScoped<ARCMainMigration>(); // Already registered above
        services.AddScoped<TaxCodeMasterMigration>();
        // Register migration services
        services.AddScoped<UOMMasterMigration>();
        services.AddScoped<PlantMasterMigration>();
        services.AddScoped<ARCMainMigration>();
        services.AddScoped<CurrencyMasterMigration>();
        services.AddScoped<CountryMasterMigration>();
        services.AddScoped<MaterialGroupMasterMigration>();
        services.AddScoped<PurchaseGroupMasterMigration>();
        services.AddScoped<PaymentTermMasterMigration>();
        services.AddScoped<IncotermMasterMigration>();
        services.AddScoped<MaterialMasterMigration>();
        services.AddScoped<EventMasterMigration>();
        services.AddScoped<TaxMasterMigration>();
        services.AddScoped<UsersMasterMigration>();
        services.AddScoped<ErpPrLinesMigration>();
        services.AddScoped<PODocTypeMasterMigration>();
        services.AddScoped<POConditionMasterMigration>();
        services.AddScoped<WorkflowMasterMigration>();
        services.AddScoped<WorkflowMasterHistoryMigration>();
        services.AddScoped<WorkflowHistoryMigration>();
        services.AddScoped<WorkflowAmountMigration>();
        services.AddScoped<WorkflowAmountHistoryMigration>();

        services.AddScoped<ARCSubMigration>();
        services.AddScoped<ARCPlantMigration>();
        services.AddScoped<ARCAttachmentMigration>();
        services.AddScoped<ARCApprovalAuthorityMigration>();
        services.AddScoped<WorkflowApprovalUserMigration>();
        services.AddScoped<WorkflowApprovalUserHistoryMigration>();
        services.AddTransient<EventSettingMigrationService>();
        services.AddTransient<EventScheduleMigrationService>();
        services.AddTransient<EventScheduleHistoryMigrationService>();
        services.AddTransient<ErpCurrencyExchangeRateMigration>();
        services.AddTransient<AuctionMinMaxTargetPriceMigration>();
        services.AddTransient<EventPriceBidColumnsMigration>();
        services.AddTransient<EventFreezeCurrencyMigration>();
        services.AddTransient<EventPublishMigration>();
        services.AddTransient<EventSupplierPriceBidMigration>();
        services.AddTransient<EventSupplierLineItemMigration>();
        services.AddTransient<SourceListMasterMigration>();
        services.AddTransient<PriceBidChargesMasterMigration>();
        services.AddTransient<PoHeaderMigration>();
        services.AddTransient<PoLineMigration>();
        services.AddTransient<SupplierBankDetailsMigration>();
        services.AddTransient<SupplierEventPriceBidColumnsMigration>();
        services.AddTransient<SupplierPriceBidLotChargesMigration>();
        services.AddTransient<SupplierPriceBidLotPriceMigration>();
        services.AddTransient<SupplierPriceBidDocumentMigration>();
        services.AddTransient<SupplierPriceBoqItemsMigration>();
        services.AddTransient<SupplierPriceBidNonPricingMigration>();
        services.AddTransient<EventCommunicationSenderMigration>();
        services.AddTransient<EventCommunicationReceiverMigration>();
        services.AddTransient<EventCommunicationAttachmentMigration>();
        services.AddTransient<NfaClarificationMigration>();
        services.AddTransient<NfaBoqItemsMigration>();
        services.AddTransient<NfaAttachmentsMigration>();
        services.AddTransient<NfaPoConditionMigration>();
        services.AddTransient<NfaWorkflowMigration>();

        services.AddScoped<PRAttachmentMigration>();
        services.AddScoped<PRBoqItemsMigration>();
        services.AddScoped<EventItemsMigration>();
        services.AddScoped<EventBoqItemsMigration>();
        services.AddScoped<AssignedEventVendorMigration>();
        services.AddScoped<EventCollaborationMigration>();
        services.AddScoped<UserTechnicalParameterMigration>();
        services.AddScoped<UserTermMigration>();
        services.AddScoped<TechnicalApprovalWorkflowMigration>();
        services.AddScoped<TechnicalApprovalScoreMigration>();
        services.AddScoped<TechnicalApprovalScoreHistoryMigration>();
        services.AddScoped<TechnicalDocumentsMigration>();
        services.AddScoped<TechnicalParameterTemplateMigration>();
        services.AddScoped<TermMasterMigration>();
        services.AddScoped<TermTemplateMasterMigration>();
        services.AddScoped<UserPriceBidLotChargesMigration>();
        services.AddScoped<UserPriceBidNonPricingMigration>();
        services.AddScoped<UserCompanyMasterMigration>();
        services.AddScoped<SupplierTermsMigration>();
        services.AddScoped<SupplierTermDeviationsMigration>();
        services.AddScoped<SupplierTechnicalParameterMigration>();
        services.AddScoped<NfaHeaderMigration>();
        services.AddScoped<NfaLineMigration>();
        services.AddScoped<NfaLotChargesMigration>();

        return services;
    }
}
