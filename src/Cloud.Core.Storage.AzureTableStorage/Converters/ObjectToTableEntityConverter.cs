namespace Cloud.Core.Storage.AzureTableStorage.Converters
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Collections.Generic;
    using Extensions;
    using Microsoft.Azure.Cosmos.Table;

    internal class ObjectToTableEntityConverter
    {
        private readonly string[] _keyParts;
        private readonly object _sourceObject;
        private readonly PropertyInfo[] _reflectedProperties;

        public ObjectToTableEntityConverter(object value)
        {
            _sourceObject = value;
            _reflectedProperties = value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

            var property = _reflectedProperties.FirstOrDefault(r => r.Name == "Key");
            var key = property?.GetValue(_sourceObject);
            _keyParts = key == null ? new string[]{ } : ((string)key).Split('/');

            if (_keyParts.Length != 2)
            {
                throw new ArgumentException("Key must be splittable into two parts using the \"/\" delimiter.  Part 0 is Partition key, part 1 is Row key");
            }
        }
        
        public IDictionary<string, EntityProperty> GetEntityProperties()
        {
            var result = new Dictionary<string, EntityProperty>();

            foreach (var property in _reflectedProperties)
            {
                if (property.Name != "Key")
                {
                    var value = property.GetValueAsEntityProperty(_sourceObject);
                    result.Add(property.Name, value);
                }
            }

            return result;
        }

        public ITableEntity GetTableEntity()
        {
            return new DynamicTableEntity
            {
                PartitionKey = _keyParts[0],
                RowKey = _keyParts[1],
                Properties = GetEntityProperties()
            };
        }
    }
}
