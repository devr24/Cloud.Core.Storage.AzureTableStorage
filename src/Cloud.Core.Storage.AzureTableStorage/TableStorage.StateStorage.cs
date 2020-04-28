namespace Cloud.Core.Storage.AzureTableStorage
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// State storage partial section of the TableStorage class.
    /// Implements the <see cref="IStateStorage" />
    /// Implements the <see cref="TableStorageBase" />
    /// Implements the <see cref="ITableStorage" />
    /// Implements the <see cref="IAuditLogger" />
    /// </summary>
    /// <seealso cref="TableStorageBase" />
    /// <seealso cref="ITableStorage" />
    /// <seealso cref="IAuditLogger" />
    /// <seealso cref="IStateStorage" />
    public partial class TableStorage : IStateStorage
    {
        internal readonly string StateStorageTableName = "appState";

        /// <summary>
        /// Gets the state object from table storage.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <returns>Task&lt;T&gt;.</returns>
        /// <exception cref="System.NotImplementedException"></exception>
        public async Task<T> GetState<T>(string key)
        {
            // If the state storage table does not exist - create it.  This isn't expensive as state storage isn't a regularly called method.
            if (!CheckForTable(StateStorageTableName))
            {
                await CreateTable(StateStorageTableName);
            }

            var state = await GetEntity<StateData<T>>(StateStorageTableName, GetFullKeyName(key));
            return state.Data;
        }

        /// <summary>
        /// Determines whether [is state stored] using [the specified key].
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> IsStateStored(string key)
        {
            // If the state storage table does not exist - create it.  This isn't expensive as state storage isn't a regularly called method.
            if (!CheckForTable(StateStorageTableName))
            {
                await CreateTable(StateStorageTableName);
            }

            return await Exists(StateStorageTableName, GetFullKeyName(key));
        }

        /// <summary>
        /// Removes object from state using the given key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>Task.</returns>
        public async Task RemoveState(string key)
        {
            // If the state storage table does not exist - create it.  This isn't expensive as state storage isn't a regularly called method.
            if (!CheckForTable(StateStorageTableName))
            {
                await CreateTable(StateStorageTableName);
            }

            await DeleteEntity(StateStorageTableName, GetFullKeyName(key));
        }

        /// <summary>
        /// Sets the state using the key.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="storeObj">The store object.</param>
        /// <returns>Task.</returns>
        public async Task SetState<T>(string key, T storeObj)
        {
            // If the state storage table does not exist - create it.  This isn't expensive as state storage isn't a regularly called method.
            if (!CheckForTable(StateStorageTableName))
            {
                await CreateTable(StateStorageTableName);
            }

            if (storeObj == null)
            {
                await DeleteEntity(StateStorageTableName, GetFullKeyName(key));
            }
            else
            {
                var wrappedObj = new StateData<T>() { Key = GetFullKeyName(key), Data = storeObj };
                await UpsertEntity(StateStorageTableName, wrappedObj);
            }
        }

        /// <summary>
        /// Gets the full name of the state storage item using the key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>System.String.</returns>
        private string GetFullKeyName(string key)
        {
            return $"{AppDomain.CurrentDomain.FriendlyName}/{key}";
        }

        /// <summary>
        /// The state data object returned. Implements ITableItem.
        /// Implements the <see cref="ITableItem" />
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <seealso cref="ITableItem" />
        private class StateData<T> : ITableItem
        {
            public string Key { get; set; }
            public T Data { get; set; }
        }
    }
}
