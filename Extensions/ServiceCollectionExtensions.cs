using Microsoft.Extensions.DependencyInjection;
using DataMigration.Services;

namespace Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMigrationServices(this IServiceCollection services)
    {
        // Register migration services
        services.AddScoped<UOMMasterMigration>();
        services.AddScoped<PlantMasterMigration>();
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

        return services;
    }
}
