namespace Cloud.Core.Storage.AzureTableStorage.Converters
{
    using System;
    using Microsoft.Azure.Cosmos.Table;

    /// <summary>
    /// Table entity conversion class.  Converts an object to and from a table entity.
    /// </summary>
    internal static class TableEntityConvert
    {
        /// <summary>
        /// Converts to table entity.
        /// </summary>
        /// <param name="obj">The poco class to convert.</param>
        /// <returns>ITableEntity converted object.</returns>
        public static ITableEntity ToTableEntity(object obj)
        {
            var converter = new ObjectToTableEntityConverter(obj);
            return converter.GetTableEntity();
        }

        /// <summary>
        /// Converts from the table entity to a generic object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableEntity">The table entity to convert.</param>
        /// <returns>Instance of generic type T object.</returns>
        public static T FromTableEntity<T>(DynamicTableEntity tableEntity) where T : class, ITableItem, new()
        {
            var converter = new TableEntityToObjectConverter<T>(tableEntity);
            return converter.GetObject();
        }

        /// <summary>
        /// From table entity object to generic object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableEntity">The table entity.</param>
        /// <returns>Instance of generic type T object.</returns>
        /// <exception cref="System.ArgumentException">Parameter has to be of type DynamicTableEntity - tableEntity</exception>
        public static T FromTableEntity<T>(object tableEntity) where T : class, ITableItem, new()
        {
            var dynamicTableEntity = tableEntity as DynamicTableEntity;

            if (dynamicTableEntity == default(DynamicTableEntity))
            {
                throw new ArgumentException("Parameter has to be of type DynamicTableEntity", nameof(tableEntity));
            }

            return FromTableEntity<T>(dynamicTableEntity);
        }
    }
}
