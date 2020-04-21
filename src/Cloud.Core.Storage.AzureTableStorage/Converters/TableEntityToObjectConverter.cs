namespace Cloud.Core.Storage.AzureTableStorage.Converters
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Collections.Generic;
    using Extensions;
    using Microsoft.Azure.Cosmos.Table;

    public class TableEntityToObjectConverter<T> where T : class, ITableItem, new()
    {
        private readonly T _resultObject;
        private readonly PropertyInfo[] _reflectedProperties;
        
        public TableEntityToObjectConverter()
        {
            _resultObject = new T();
            _reflectedProperties = _resultObject.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        }

        public TableEntityToObjectConverter(DynamicTableEntity tableEntity) : this()
        {
            SetKey(tableEntity.PartitionKey, tableEntity.RowKey);
            SetETag(tableEntity.ETag);
            SetTimestamp(tableEntity.Timestamp);
            SetProperties(tableEntity.Properties);
        }

        public void SetKey(string partitionKey, string rowKey)
        {
            _resultObject.Key = $"{partitionKey}/{rowKey}";
        }
        
        public void SetETag(string value)
        {
            var property = _reflectedProperties.FirstOrDefault(n => n.Name == "ETag");
            if (property != default(PropertyInfo))
            {
                property.SetValue(_resultObject, value);
            }
        }

        public void SetTimestamp(DateTimeOffset value)
        {
            var property = _reflectedProperties.FirstOrDefault(n => n.Name == "Timestamp");
            if (property != default(PropertyInfo))
            {
                property.SetValue(_resultObject, value);
            }
        }

        public void SetProperties(IDictionary<string, EntityProperty> properties)
        {
            foreach (var tableProperty in properties)
            {
                var objectProperty = _reflectedProperties.FirstOrDefault(p => p.Name.Equals(tableProperty.Key));
                if (objectProperty != default(PropertyInfo))
                {
                    objectProperty.SetTableEntityValue(_resultObject, tableProperty.Value);
                }
            }
        }

        public T GetObject()
        {
            return _resultObject;
        }
    }
}
