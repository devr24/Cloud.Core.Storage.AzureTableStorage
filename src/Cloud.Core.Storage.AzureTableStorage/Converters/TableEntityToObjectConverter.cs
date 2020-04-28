namespace Cloud.Core.Storage.AzureTableStorage.Converters
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Collections.Generic;
    using Extensions;
    using Microsoft.Azure.Cosmos.Table;

    /// <summary>
    /// Class TableEntityToObjectConverter.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TableEntityToObjectConverter<T> where T : class, ITableItem, new()
    {
        private readonly T _resultObject;
        private readonly PropertyInfo[] _reflectedProperties;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableEntityToObjectConverter{T}"/> class.
        /// </summary>
        public TableEntityToObjectConverter()
        {
            _resultObject = new T();
            _reflectedProperties = _resultObject.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableEntityToObjectConverter{T}"/> class.
        /// </summary>
        /// <param name="tableEntity">The table entity.</param>
        public TableEntityToObjectConverter(DynamicTableEntity tableEntity) : this()
        {
            SetKey(tableEntity.PartitionKey, tableEntity.RowKey);
            SetETag(tableEntity.ETag);
            SetTimestamp(tableEntity.Timestamp);
            SetProperties(tableEntity.Properties);
        }

        /// <summary>
        /// Sets the key.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="rowKey">The row key.</param>
        public void SetKey(string partitionKey, string rowKey)
        {
            _resultObject.Key = $"{partitionKey}/{rowKey}";
        }

        /// <summary>
        /// Sets the e tag.
        /// </summary>
        /// <param name="value">The value.</param>
        public void SetETag(string value)
        {
            var property = _reflectedProperties.FirstOrDefault(n => n.Name == "ETag");
            if (property != default)
            {
                property.SetValue(_resultObject, value);
            }
        }

        /// <summary>
        /// Sets the timestamp.
        /// </summary>
        /// <param name="value">The value.</param>
        public void SetTimestamp(DateTimeOffset value)
        {
            var property = _reflectedProperties.FirstOrDefault(n => n.Name == "Timestamp");
            if (property != default)
            {
                property.SetValue(_resultObject, value);
            }
        }

        /// <summary>
        /// Sets the properties.
        /// </summary>
        /// <param name="properties">The properties.</param>
        public void SetProperties(IDictionary<string, EntityProperty> properties)
        {
            foreach (var tableProperty in properties)
            {
                var objectProperty = _reflectedProperties.FirstOrDefault(p => p.Name.Equals(tableProperty.Key));
                if (objectProperty != default)
                {
                    objectProperty.SetTableEntityValue(_resultObject, tableProperty.Value);
                }
            }
        }

        /// <summary>
        /// Gets the object.
        /// </summary>
        /// <returns>T.</returns>
        public T GetObject()
        {
            return _resultObject;
        }
    }
}
