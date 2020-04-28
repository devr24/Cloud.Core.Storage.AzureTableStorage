namespace Cloud.Core.Storage.AzureTableStorage.Config
{
    using System;
    using System.Linq;
    using Validation;
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Msi Configuration for Azure Table storage.
    /// </summary>
    public class MsiConfig : AttributeValidator
    {
        /// <summary>
        /// Gets or sets the name of the table storage instance.
        /// </summary>
        /// <value>
        /// The name of the table storage instance.
        /// </value>
        [Required]
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        [Required]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        [Required]
        public string SubscriptionId { get; set; }
    }

    /// <summary>Connection string config.</summary>
    public class ConnectionConfig : AttributeValidator
    {
        /// <summary>
        /// Gets or sets the connection string for connecting to storage.
        /// </summary>
        /// <value>
        /// Storage connection string.
        /// </value>
        [Required]
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

                // Account name is used as the identifier.
                return parts
                    .FirstOrDefault(p => p.StartsWith(replaceStr))?.Replace(replaceStr, string.Empty);
            }
        }
    }

    /// <summary>
    /// Service Principle Configuration for Azure Table storage.
    /// </summary>
    public class ServicePrincipleConfig : AttributeValidator
    {
        /// <summary>
        /// Gets or sets the application identifier.
        /// </summary>
        /// <value>
        /// The application identifier.
        /// </value>
        [Required]
        public string AppId { get; set; }

        /// <summary>
        /// Gets or sets the application secret.
        /// </summary>
        /// <value>
        /// The application secret string.
        /// </value>
        [Required]
        public string AppSecret { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        /// <value>
        /// The tenant identifier.
        /// </value>
        [Required]
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the subscription identifier.
        /// </summary>
        /// <value>
        /// The subscription identifier.
        /// </value>
        [Required]
        public string SubscriptionId { get; set; }

        /// <summary>
        /// Gets or sets the name of the storage instance.
        /// </summary>
        /// <value>
        /// The name of the storage instance.
        /// </value>
        [Required]
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
    }
}
