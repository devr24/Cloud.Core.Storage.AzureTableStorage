namespace Microsoft.Extensions.DependencyInjection
{
    using System;
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
            services.AddFactoryIfNotAdded<ITableStorage>();
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
            services.AddFactoryIfNotAdded<ITableStorage>();
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
            services.AddFactoryIfNotAdded<ITableStorage>();
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
            services.AddFactoryIfNotAdded<ITableStorage>();
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
            services.AddFactoryIfNotAdded<ITableStorage>();
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
            })
            {
                Name = $"{instanceName}-AuditLog"
            };
            services.AddSingleton<IAuditLogger>(instance);
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
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config)
            {
                Name = $"{config.InstanceName}-AuditLog"
            };
            services.AddSingleton<IAuditLogger>(instance);
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
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config)
            {
                Name = $"{config.InstanceName}-AuditLog"
            };
            services.AddSingleton<IAuditLogger>(instance);
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
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config)
            {
                Name = $"{config.InstanceName}-AuditLog"
            };
            services.AddSingleton<IAuditLogger>(instance);
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
            })
            {
                Name = $"{instanceName}-StateStorage"
            };
            services.AddSingleton<IStateStorage>(instance);
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
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config)
            {
                Name = $"{config.InstanceName}-StateStorage"
            };
            services.AddSingleton<IStateStorage>(instance);
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
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config)
            {
                Name = $"{config.InstanceName}-StateStorage"
            };
            services.AddSingleton<IStateStorage>(instance);
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
            var instance = new Cloud.Core.Storage.AzureTableStorage.TableStorage(config)
            {
                Name = $"{config.InstanceName}-StateStorage"
            };
            services.AddSingleton<IStateStorage>(instance);
            return services;
        }
        #endregion
    }
}
