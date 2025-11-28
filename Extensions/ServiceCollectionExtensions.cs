using Microsoft.Extensions.DependencyInjection;

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
        services.AddScoped<WorkflowAmountMigration>();
        services.AddScoped<WorkflowAmountHistoryMigration>();
        services.AddScoped<WorkflowApprovalUserMigration>();
        services.AddScoped<WorkflowApprovalUserHistoryMigration>();

        return services;
    }
}
