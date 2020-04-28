namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using System.Linq;
    using Cloud.Core;
    using Cloud.Core.Storage.AzureTableStorage.Config;

    /// <summary>
    /// Class Service Collection extensions.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        #region Table Storage

        /// <summary>
        /// Adds an instance of Azure table storage as a singleton with a specific instance name, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="key">The key to use when looking up the instance from the factory.</param>
        /// <param name="instanceName">Name of the table storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddTableStorageSingletonNamed(this IServiceCollection services, string key, string instanceName, string tenantId, string subscriptionId)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId
            });

            if (!key.IsNullOrEmpty())
                instance.Name = key;

            services.AddSingleton<ITableStorage>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure table storage as a singleton, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="instanceName">Name of the table storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddTableStorageSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId)
        {
            services.AddTableStorageSingleton(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId
            });
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Table storage as a singleton, using managed user config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddTableStorageSingleton(this IServiceCollection services, MsiConfig config)
        {
            services.AddSingleton<ITableStorage>(new Cloud.Core.Storage.AzureTableStorage.TableStorage(config));
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Table storage as a singleton, using service principle config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddTableStorageSingleton(this IServiceCollection services, ServicePrincipleConfig config)
        {
            services.AddSingleton<ITableStorage>(new Cloud.Core.Storage.AzureTableStorage.TableStorage(config));
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Table storage as a singleton, using connection string config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddTableStorageSingleton(this IServiceCollection services, ConnectionConfig config)
        {
            services.AddSingleton<ITableStorage>(new Cloud.Core.Storage.AzureTableStorage.TableStorage(config));
            AddFactoryIfNotAdded(services);
            return services;
        }

        #endregion

        #region Audit Logging 

        /// <summary>
        /// Adds an instance of state storage as a singleton with a specific instance name, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="instanceName">Name of the state storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddAuditLogSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId
            });
            instance.Name = $"{instanceName}-AuditLog";
            services.AddSingleton<IAuditLogger>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds an instance of Azure Table storage as a singleton, using managed user config to setup.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="config">The configuration to initialise with.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddAuditLogSingleton(this IServiceCollection services, MsiConfig config)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config);
            instance.Name = $"{config.InstanceName}-AuditLog";
            services.AddSingleton<IAuditLogger>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds the audit log singleton.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddAuditLogSingleton(this IServiceCollection services, ServicePrincipleConfig config)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config);
            instance.Name = $"{config.InstanceName}-AuditLog";
            services.AddSingleton<IAuditLogger>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }
        
        /// <summary>
        /// Adds the audit log singleton.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddAuditLogSingleton(this IServiceCollection services, ConnectionConfig config)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config);
            instance.Name = $"{config.InstanceName}-AuditLog";
            services.AddSingleton<IAuditLogger>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        #endregion

        #region State Storage

        /// <summary>
        /// Adds an instance of state storage as a singleton with a specific instance name, using managed user config to setup.  Requires the instance 
        /// name, TenantId and SubscriptionId to be supplied.
        /// </summary>
        /// <param name="services">The services to extend.</param>
        /// <param name="instanceName">Name of the state storage instance to connect to.</param>
        /// <param name="tenantId">Tenant Id the instance lives in.</param>
        /// <param name="subscriptionId">Subscription Id for the tenant.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddStateStorageSingleton(this IServiceCollection services, string instanceName, string tenantId, string subscriptionId)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(new MsiConfig
            {
                InstanceName = instanceName,
                TenantId = tenantId,
                SubscriptionId = subscriptionId
            });
            instance.Name = $"{instanceName}-StateStorage";
            services.AddSingleton<IStateStorage>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds the state storage singleton.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddStateStorageSingleton(this IServiceCollection services, MsiConfig config)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config);
            instance.Name = $"{config.InstanceName}-StateStorage";
            services.AddSingleton<IStateStorage>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds the state storage singleton.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddStateStorageSingleton(this IServiceCollection services, ServicePrincipleConfig config)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config);
            instance.Name = $"{config.InstanceName}-StateStorage";
            services.AddSingleton<IStateStorage>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }

        /// <summary>
        /// Adds the state storage singleton.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddStateStorageSingleton(this IServiceCollection services, ConnectionConfig config)
        {
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config);
            instance.Name = $"{config.InstanceName}-StateStorage";
            services.AddSingleton<IStateStorage>(instance);
            AddFactoryIfNotAdded(services);
            return services;
        }
        #endregion

        /// <summary>
        /// Add the generic service factory from Cloud.Core for the ITableStorage type.  This allows multiple named instances of the same instance.
        /// </summary>
        /// <param name="services">Service collection to extend.</param>
        private static void AddFactoryIfNotAdded(IServiceCollection services)
        {
            if (!services.ContainsService(typeof(NamedInstanceFactory<ITableStorage>)))
            {
                // Service Factory doesn't exist, so we add it to ensure it's always available.
                services.AddSingleton<NamedInstanceFactory<ITableStorage>>();
            }
        }

        /// <summary>
        /// Search through the service collection for a particular object type.
        /// </summary>
        /// <param name="services">IServiceCollection to check.</param>
        /// <param name="objectTypeToFind">Type of object to find.</param>
        /// <returns>Boolean true if service exists and false if not.</returns>
        public static bool ContainsService(this IServiceCollection services, Type objectTypeToFind)
        {
            return services.Any(x => x.ServiceType == objectTypeToFind);
        }
    }
}
