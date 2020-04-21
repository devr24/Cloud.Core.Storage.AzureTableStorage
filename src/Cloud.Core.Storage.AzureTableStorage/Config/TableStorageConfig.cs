namespace Cloud.Core.Storage.AzureTableStorage.Config
{
    using System;
    using System.Linq;

    /// <summary>
    /// Msi Configuration for Azure Table storage.
    /// </summary>
    public class MsiConfig
    {
        /// <summary>
        /// Gets or sets the name of the table storage instance.
        /// </summary>
        /// <value>
        /// The name of the table storage instance.
        /// </value>
        public string InstanceName { get; set; }
        
        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        public string SubscriptionId { get; set; }
        
        /// <summary>Ensure mandatory properties are set.</summary>
        public void Validate()
        {
            if (InstanceName.IsNullOrEmpty())
                throw new ArgumentException("Table storage InstanceName must be set");

            if (TenantId.IsNullOrEmpty())
                throw new ArgumentException("TenantId must be set");

            if (SubscriptionId.IsNullOrEmpty())
                throw new ArgumentException("SubscriptionId must be set");
        }
    }

    /// <summary>Connection string config.</summary>
    public class ConnectionConfig
    {
        /// <summary>
        /// Gets or sets the connection string for connecting to storage.
        /// </summary>
        /// <value>
        /// Storage connection string.
        /// </value>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Table storage instance name taken from the connection string.
        /// </summary>
        public string InstanceName
        {
            get
            {
                if (ConnectionString.IsNullOrEmpty())
                    return null;

                const string replaceStr = "AccountName=";

                var parts = ConnectionString.Split(';');

                if (parts.Length <= 1)
                    return null;

                // Account name is used as the indentifier.
                return parts.Where(p => p.StartsWith(replaceStr))
                    .FirstOrDefault()?.Replace(replaceStr, string.Empty);
            }
        }

        /// <summary>Ensure mandatory properties are set.</summary>
        public void Validate()
        {
            if (ConnectionString.IsNullOrEmpty())
                throw new ArgumentException("ConnectionString must be set");
        }
    }

    /// <summary>
    /// Service Principle Configuration for Azure Table storage.
    /// </summary>
    public class ServicePrincipleConfig
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>
        /// The application identifier.
        /// </value>
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>
        /// The application secret string.
        /// </value>
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the storage instance.
        /// </summary>
        /// <value>
        /// The name of the storage instance.
        /// </value>
        public string InstanceName { get; set; } 
        
        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return $"AppId: {AppId}, TenantId: {TenantId}, Table storage InstanceName: {InstanceName}";
        }

        /// <summary>Ensure mandatory properties are set.</summary>
        public void Validate()
        {
            if (InstanceName.IsNullOrEmpty())
                throw new ArgumentException("Table storage InstanceName must be set");

            if (AppId.IsNullOrEmpty())
                throw new ArgumentException("AppId must be set");

            if (AppSecret.IsNullOrEmpty())
                throw new ArgumentException("AppSecret must be set");

            if (TenantId.IsNullOrEmpty())
                throw new ArgumentException("TenantId must be set");

            if (SubscriptionId.IsNullOrEmpty())
                throw new ArgumentException("SubscriptionId must be set");
        }
    }
}
