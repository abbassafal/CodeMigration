using Microsoft.Extensions.DependencyInjection;

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

        services.AddScoped<PRAttachmentMigration>();
        services.AddScoped<PRBoqItemsMigration>();

        return services;
    }
}
