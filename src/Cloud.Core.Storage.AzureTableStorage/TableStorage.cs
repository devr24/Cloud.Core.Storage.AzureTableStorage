namespace Cloud.Core.Storage.AzureTableStorage
{
    using System.Net;
    using System.Reactive.Linq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Management.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
    using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
    using Microsoft.IdentityModel.Clients.ActiveDirectory;
    using Microsoft.Rest;
    using Microsoft.Rest.TransientFaultHandling;
    using Microsoft.Azure.Services.AppAuthentication;
    using Microsoft.Extensions.Logging;
    using Config;
    using Converters;
    using Exceptions;
    using Newtonsoft.Json;
    using Microsoft.Azure.Cosmos.Table;
    using System.Threading;
    using System.Collections.Concurrent;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Azure specific implementation of cloud table storage.
    /// Class can also be used for State Storage and Auditing.
    /// Implements the <see cref="TableStorageBase" />
    /// Implements the <see cref="ITableStorage" />
    /// Implements the <see cref="IStateStorage" />
    /// Implements the <see cref="IAuditLogger" />
    /// </summary>
    /// <seealso cref="IStateStorage" />
    /// <seealso cref="IAuditLogger" />
    /// <seealso cref="TableStorageBase" />
    /// <seealso cref="ITableStorage" />
    public partial class TableStorage : TableStorageBase, ITableStorage
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TableStorage" /> with Service Principle authentication.
        /// </summary>
        /// <param name="config">The Service Principle configuration settings for connecting to storage.</param>
        /// <param name="logger">The logger to log information to.</param>
        /// <inheritdoc />
        public TableStorage([NotNull]ServicePrincipleConfig config, [MaybeNull] ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorage" /> class with a Connection String.
        /// </summary>
        /// <param name="config">The Connection String information for connecting to Storage.</param>
        /// <param name="logger">The Logger?.</param>
        public TableStorage([NotNull]ConnectionConfig config, [MaybeNull] ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorage" /> class with Managed Service Identity (MSI) authentication.
        /// </summary>
        /// <param name="config">The Managed Service Identity (MSI) configuration for connecting to storage.</param>
        /// <param name="logger">The Logger?.</param>
        public TableStorage([NotNull]MsiConfig config, [MaybeNull]ILogger logger = null)
            : base(config, logger) { }

        /// <summary>
        /// Gets the entity from the requested table, using the key identifier.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="key">The key, used to find the entity.</param>
        /// <returns>Task of type T.</returns>
        /// <inheritdoc cref="ITableStorage.GetEntity{T}" />
        public async Task<T> GetEntity<T>(string tableName, string key) where T : class, ITableItem, new()
        {
            try
            {
                var parts = GetKeySegments(key);
                var table = CloudTableClient.GetTableReference(tableName);

                // Load the DynamicTableEntity object from Storage using the keys.
                var operation = TableOperation.Retrieve(parts[0], parts[1]);
                var result = await table.ExecuteAsync(operation);
                if (result.Result == null)
                {
                    return null;
                }

                // Return the converted generic object.
                return TableEntityConvert.FromTableEntity<T>(result.Result);
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred retrieving, tablename: {tableName}, key: {key}");
                throw;
            }
        }

        /// <summary>
        /// Checks if an entity exists within the table name, using the key given.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">The key to search for.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> Exists(string tableName, string key)
        {
            try
            {
                var parts = GetKeySegments(key);
                var table = CloudTableClient.GetTableReference(tableName);

                // Load the DynamicTableEntity object from Storage using the keys
                var operation = TableOperation.Retrieve(parts[0], parts[1]);
                var result = await table.ExecuteAsync(operation);

                // Return if the result was null or not.
                return result.Result != null;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred checking exists, table name: {tableName}, key: {key}");
                throw;
            }
        }

        /// <summary>
        /// Deletes the entity with the given key from the requested table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">The key to search for.</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntity(string tableName, string key)
        {
            try
            {
                var parts = GetKeySegments(key);
                var table = CloudTableClient.GetTableReference(tableName);

                // Load the DynamicTableEntity object from Storage using the keys
                var operation = TableOperation.Retrieve(parts[0], parts[1]);
                var result = await table.ExecuteAsync(operation);

                // If record is found, delete.
                if (result.Result != null)
                {
                    var entity = (ITableEntity)result.Result;
                    var deleteOperation = TableOperation.Delete(entity);
                    await table.ExecuteAsync(deleteOperation);
                }
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred deleting, table name: {tableName}, key: {key}");
                throw;
            }
        }

        /// <summary>
        /// Deletes multiple entities with the list of keys (done in batches), from the supplied table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="keys">The keys for deletion.</param>
        /// <param name="batchSize">Size of the batch to delete at any one time (defaults to 10).</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntities(string tableName, List<string> keys, int batchSize = 10)
        {
            try
            {
                var table = CloudTableClient.GetTableReference(tableName);
                var batchOperation = new TableBatchOperation();
                var rowCount = 0;

                for (int i = 0; i < keys.Count; i++)
                {
                    rowCount++;
                    var parts = GetKeySegments(keys[i]);

                    // Load the DynamicTableEntity object from Storage using the keys
                    var operation = TableOperation.Retrieve(parts[0], parts[1]);
                    var result = await table.ExecuteAsync(operation);

                    // Only batch the operation if the entity can be found
                    if (result.Result != null)
                    {
                        var entity = (ITableEntity)result.Result;
                        batchOperation.Delete(entity);
                    }

                    // Execute the batch operation if we reach our batch limit or its the end of the list.
                    if (rowCount >= batchSize || i == keys.Count - 1)
                    {
                        await table.ExecuteBatchAsync(batchOperation);
                        batchOperation = new TableBatchOperation();
                        rowCount = 0;
                    }
                }
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred deleting multiple, table name: {tableName}, keys: {string.Concat(",", keys)}");
                throw;
            }
        }

        /// <summary>
        /// Inserts or updates (upserts) the passed entity into the given table.
        /// </summary>
        /// <typeparam name="T">Type of object upserted.</typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="data">The data to insert.</param>
        /// <returns>Task.</returns>
        /// <exception cref="ConflictException">conflict while trying to upsert entity</exception>
        /// <exception cref="InvalidOperationException">Upsert data is badly formed and resulted in a 400 Bad Request response from TableStorage</exception>
        [ExcludeFromCodeCoverage]
        public async Task UpsertEntity<T>(string tableName, T data) where T : class, ITableItem
        {
            try
            {
                // Convert POCO to ITableEntity object using TableEntityConvert.ToTableEntity()
                var tableEntity = TableEntityConvert.ToTableEntity(data);

                // Save the new or updated entity in the Storage
                var operation = TableOperation.InsertOrReplace(tableEntity);

                var table = CloudTableClient.GetTableReference(tableName);
                var result = await table.ExecuteAsync(operation);

                // Getting the HTTP result to detect if a conflict has occurred when trying to upsert the file to allow the client to specifically deal with this. 
                if (result.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    Logger?.LogWarning("Warning: Conflict error while upserting entity.");
                    throw new ConflictException("conflict while trying to upsert entity");
                }
            }
            catch (StorageException st)
                when (st.RequestInformation.HttpStatusCode == 400)
            {
                Logger?.LogError($"Bad Request to table storage ({tableName}) - check modal properties are all set: {JsonConvert.SerializeObject(data)}");
                throw new InvalidOperationException("Upsert data is badly formed and resulted in a 400 Bad Request response from TableStorage");
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error: {e.Message} occurred, table name: {tableName} with generic data");
                throw;
            }
        }

        /// <summary>
        /// Upserts multiple entities into the given table.
        /// </summary>
        /// <typeparam name="T">Type of object to upsert.</typeparam>
        /// <param name="tableName">Name of the table the items will be added to.</param>
        /// <param name="data">The data.</param>
        /// <param name="batchSize">Size of the batch to update at any one time (defaults to 10).</param>
        /// <returns>Task.</returns>
        /// <exception cref="InvalidOperationException">Upsert data is badly formed and resulted in a 400 Bad Request response from TableStorage</exception>
        [ExcludeFromCodeCoverage]
        public async Task UpsertEntities<T>(string tableName, List<T> data, int batchSize = 100) where T : class, ITableItem
        {
            try
            {
                var table = CloudTableClient.GetTableReference(tableName);
                var batchOperation = new TableBatchOperation();
                batchSize = batchSize > 100 ? 100 : batchSize;

                var lastUsedPartitionKey = string.Empty;

                for (int i = 0; i < data.Count; i++)
                {
                    var entry = TableEntityConvert.ToTableEntity(data[i]);
                    var newBatch = (lastUsedPartitionKey != entry.PartitionKey && lastUsedPartitionKey.Length > 0);

                    if (newBatch)
                    {
                        await table.ExecuteBatchAsync(batchOperation);
                        batchOperation.Clear();
                        batchOperation.InsertOrReplace(entry);
                    }
                    else
                    {
                        batchOperation.InsertOrReplace(entry);

                        // Execute the batch operation if we reach our batch limit or its the end of the list.
                        if (batchOperation.Count >= batchSize || i == data.Count - 1)
                        {
                            await table.ExecuteBatchAsync(batchOperation);
                            batchOperation.Clear();
                        }
                    }

                    lastUsedPartitionKey = entry.PartitionKey;
                }

                if (batchOperation.Count > 0)
                {
                    await table.ExecuteBatchAsync(batchOperation);
                    batchOperation.Clear();
                }
            }
            catch (StorageException st)
                when (st.RequestInformation.HttpStatusCode == 400)
            {
                Logger?.LogError($"Bad Request to table storage ({tableName}) - check modal properties are all set: {JsonConvert.SerializeObject(data)}");
                throw new InvalidOperationException("Upsert data is badly formed and resulted in a 400 Bad Request response from TableStorage");
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred upserting multiple, table name: {tableName} with generic data");
                throw;
            }
        }

        /// <summary>
        /// Lists all the entities within a given table, using the supplied query as an enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="selectColumns">List of columns to select (if required).</param>
        /// <param name="filterQuery">The query to use when finding items.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>IEnumerable&lt;T&gt;.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, List<string> selectColumns = null, string filterQuery = null, CancellationTokenSource token = default) 
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            if (!selectColumns.IsNullOrDefault())
            {
                tblQuery = tblQuery.Select(selectColumns);
            }

            if (!filterQuery.IsNullOrDefault())
            {
                tblQuery = tblQuery.Where(filterQuery);
            }

            return ListEntitiesObservable<T>(tableName, tblQuery, token).ToEnumerable();
        }

        /// <summary>
        /// Lists all the entities within a given table, using the supplied query as an enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>IEnumerable&lt;T&gt;.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            return ListEntitiesObservable<T>(tableName, tblQuery, token).ToEnumerable();
        }


        /// <summary>
        /// List the entities of a given table name, with a supplied query.  Results returned as an Enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="selectColumns">The columns to select (if required).</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, List<string> selectColumns, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            if (!selectColumns.IsNullOrDefault())
            {
                tblQuery = tblQuery.Select(selectColumns);
            }

            return ListEntitiesObservable<T>(tableName, tblQuery, token).ToEnumerable();
        }

        /// <summary>
        /// List the entities of a given table name, with a supplied query.  Results returned as an Enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="filterQuery">The query to execute.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list.</returns>
        public IEnumerable<T> ListEntities<T>(string tableName, string filterQuery, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            if (!filterQuery.IsNullOrDefault())
            {
                tblQuery = tblQuery.Where(filterQuery);
            }

            return ListEntitiesObservable<T>(tableName, tblQuery, token).ToEnumerable();
        }

        /// <summary>
        /// Lists all the entities within a given table, using the supplied query as an enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="selectColumns">List of columns to select (if required).</param>
        /// <param name="filterQuery">The query to use when finding items.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>IObservable&lt;T&gt;.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, List<string> selectColumns, string filterQuery, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            if (!selectColumns.IsNullOrDefault())
            {
                tblQuery = tblQuery.Select(selectColumns);
            }

            if (!filterQuery.IsNullOrDefault())
            {
                tblQuery = tblQuery.Where(filterQuery);
            }

            return ListEntitiesObservable<T>(tableName, tblQuery, token);
        }

        /// <summary>
        /// Lists all the entities within a given table, using the supplied query as an enumerable.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>IObservable&lt;T&gt;.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            return ListEntitiesObservable<T>(tableName, tblQuery, token);
        }

        /// <summary>
        /// Internal list all the entities within a given table, using the supplied query as an observable.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="tableName">Name of the table to search.</param>
        /// <param name="tblQuery">The query to use when finding items.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>IObservable&lt;T&gt;.</returns>
        private IObservable<T> ListEntitiesObservable<T>(string tableName, TableQuery<DynamicTableEntity> tblQuery, CancellationTokenSource token) 
            where T : class, ITableItem, new()
        {
            try
            {
                var table = CloudTableClient.GetTableReference(tableName);

                return Observable.Create<T>(async obs =>
                {
                    TableContinuationToken continuationToken = null;

                    do
                    {
                        // Stop when cancellation requested.
                        if (token != null && token.IsCancellationRequested)
                        {
                            continuationToken = null;
                        }
                        else 
                        { 
                            var response = await table.ExecuteQuerySegmentedAsync(tblQuery, continuationToken, new TableRequestOptions
                            {
                                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5)
                            }, null);

                            continuationToken = response.ContinuationToken;

                            // Raise the observable OnNext for each result to be processed.
                            foreach (var item in response.Results)
                            {
                                obs.OnNext(TableEntityConvert.FromTableEntity<T>(item));
                            }
                        }

                    } while (continuationToken != null);

                    obs.OnCompleted();
                });
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred listing entities, tablename: {tableName}");
                throw;
            }
        }

        /// <summary>
        /// List the entities of a given table name, with a supplied query.  Results returned as an Observable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="filterQuery">The query to execute.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, string filterQuery, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            if (!filterQuery.IsNullOrDefault())
            {
                tblQuery = tblQuery.Where(filterQuery);
            }

            return ListEntitiesObservable<T>(tableName, tblQuery, token);
        }
        
        /// <summary>
        /// List the entities of a given table name, with a supplied query.  Results returned as an Observable.
        /// </summary>
        /// <typeparam name="T">Type of object returned in the Enumerable.</typeparam>
        /// <param name="tableName">Name of the table to search within.</param>
        /// <param name="selectColumns">The columns to select (if required).</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Returns enumerable list.</returns>
        public IObservable<T> ListEntitiesObservable<T>(string tableName, List<string> selectColumns, CancellationTokenSource token = default)
            where T : class, ITableItem, new()
        {
            var tblQuery = new TableQuery<DynamicTableEntity>();

            if (!selectColumns.IsNullOrDefault())
            {
                tblQuery = tblQuery.Select(selectColumns);
            }

            return ListEntitiesObservable<T>(tableName, tblQuery, token);
        }

        /// <summary>
        /// Lists all tables within the storage account.
        /// </summary>
        /// <returns>IEnumerable string of table names.</returns>
        public async Task<IEnumerable<string>> ListTableNames()
        {
            var tableNames = new List<string>();
            TableContinuationToken continuationToken = null;
            do
            {
                var listResults = await CloudTableClient.ListTablesSegmentedAsync(continuationToken);

                foreach (var table in listResults.Results)
                {
                    tableNames.Add(table.Name);
                }
                continuationToken = listResults.ContinuationToken;
            } while (continuationToken != null);

            return tableNames;
        }

        /// <summary>
        /// Deletes the specified table from the storage account.
        /// </summary>
        /// <param name="tableName">Table to be deleted.</param>
        /// <returns>Task.</returns>
        public async Task DeleteTable(string tableName)
        {
            try
            {
                var table = CloudTableClient.GetTableReference(tableName);

                await table.DeleteIfExistsAsync();
            }
            catch (StorageException st)
                when (st.Message.ToLowerInvariant().Contains("not implemented"))
            {
                throw new NotImplementedException("Featured not implemented - check you are using a fully featured storage account", st);
            }
            catch (StorageException st)
                when (st.Message.ToLowerInvariant().Contains("Conflict"))
            {
                // Do nothing on conflict.
            }
        }

        /// <summary>
        /// Creates a table in the storage account.
        /// </summary>
        /// <param name="tableName">The name of the table to be created.</param>
        /// <returns>Task.</returns>
        public async Task CreateTable(string tableName)
        {
            try
            {
                var table = CloudTableClient.GetTableReference(tableName);
                await table.CreateIfNotExistsAsync();
            }
            catch (StorageException st)
                when (st.Message.ToLowerInvariant().Contains("not implemented"))
            {
                throw new NotImplementedException("Featured not implemented - check you are using a fully featured storage account", st);
            }
            catch (StorageException st)
                when (st.Message.ToLowerInvariant().Contains("Conflict"))
            {
                // Do nothing on conflict.
            }
        }

        /// <summary>
        /// Checks the table exists in table storage.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> CheckTableExists(string tableName)
        {
            return await Task.FromResult(CheckForTable(tableName));
        }

        /// <summary>
        /// Counts the items in a given partition.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="key">This can be partion key only or partition key AND row key (like so "partitionKey/rowKey").</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Task&lt;System.Int64&gt; count of items.</returns>
        public async Task<long> CountItems(string tableName, string key, CancellationTokenSource token = default)
        {
            var parts = key.Split('/');

            var tblQuery = new TableQuery<DynamicTableEntity>().Select(new[] { "PartitionKey" });

            tblQuery = tblQuery.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, parts[0]));

            // Add row key if its been passed.
            if (parts.Length == 2)
            {
                tblQuery = tblQuery.Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, parts[1]));
            }

            return await Counter(tableName, tblQuery, null, token);
        }

        /// <summary>
        /// Count items in a table, intercept the increment event.
        /// </summary>
        /// <param name="tableName">Name of the table to count items from.</param>
        /// <param name="countIncrement">Action, called every time an increment happens.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Number of items found.</returns>
        public async Task<long> CountItems(string tableName, Action<long> countIncrement, CancellationTokenSource token = default)
        {
            var tblQuery = new TableQuery<DynamicTableEntity>().Select(new[] { "PartitionKey" });

            return await Counter(tableName, tblQuery, countIncrement, token);
        }

        /// <summary>
        /// Counts the items in a given table, filtering using the passed in query.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="query">String query to run.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Task&lt;System.Int64&gt; count of items.</returns>
        public async Task<long> CountItemsQuery(string tableName, string query, CancellationTokenSource token = default)
        {
            var tblQuery = new TableQuery<DynamicTableEntity>().Select(new[] { "PartitionKey" }).Where(query);

            return await Counter(tableName, tblQuery, null, token);
        }

        /// <summary>
        /// Counts the items in a given table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Task&lt;System.Int64&gt; count of items.</returns>
        public async Task<long> CountItems(string tableName, CancellationTokenSource token = default)
        {
            var tblQuery = new TableQuery<DynamicTableEntity>().Select(new[] { "PartitionKey" });

            return await Counter(tableName, tblQuery, null, token);
        }

        /// <summary>
        /// Common count mechanism used on the public facing count methods.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="tblQuery">Query for looking up table.</param>
        /// <param name="countIncrement">Callback for every count increment.</param>
        /// <param name="token">Cancellation token source.</param>
        /// <returns>Task&lt;System.Int64&gt; count of items.</returns>
        private async Task<long> Counter(string tableName, TableQuery<DynamicTableEntity> tblQuery, Action<long> countIncrement, CancellationTokenSource token)
        {
            try
            {
                var table = CloudTableClient.GetTableReference(tableName);

                TableContinuationToken continuationToken = null;
                long counter = 0;
                do
                {
                    // Stop when cancellation requested.
                    if (!token.IsNullOrDefault() && token.IsCancellationRequested)
                    {
                        continuationToken = null;
                    }
                    else
                    {
                        var response = await table.ExecuteQuerySegmentedAsync(tblQuery, continuationToken, new TableRequestOptions
                        {
                            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5)
                        }, null);

                        continuationToken = response.ContinuationToken;
                        counter += response.Results.Count;
                        countIncrement?.Invoke(counter);
                    }

                } while (continuationToken != null);

                return counter;
            }
            catch (Exception e)
            {
                Logger?.LogError(e, $"Error {e.Message} occurred counting entities, tablename: {tableName}");
                throw;
            }
        }

        /// <summary>
        /// Gets the key segments.  Expects the key to come in two separate parts, separated by a forward slash e.g.\"partitionKey/rowKey\".
        /// First part is partition key, second part is the row key.
        /// </summary>
        /// <param name="key">The key to parse.</param>
        /// <returns>System.String[].</returns>
        /// <exception cref="ArgumentException">Key must be defined as \"partitionKey/rowKey\"</exception>
        /// <exception cref="System.ArgumentException">Key must be defined as \"partitionKey/rowKey\"</exception>
        private string[] GetKeySegments(string key)
        {
            key.ThrowIfNullOrDefault();

            var parts = key.Split('/');

            if (parts.Length != 2)
            {
                throw new ArgumentException("Key must be defined as 'partitionKey/rowKey'");
            }

            return parts;
        }

        /// <summary>
        /// Checks for tables existence.
        /// </summary>
        /// <param name="tableName">Name of the table to check for.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        private bool CheckForTable(string tableName)
        {
            // Create the state storage table if not exists already.
            var table = CloudTableClient.GetTableReference(tableName);
            return table.ExistsAsync().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Base class for Azure specific implementation of cloud table storage.
    /// </summary>
    [ExcludeFromCodeCoverage]
    public abstract class TableStorageBase
    {
        /// <summary>
        /// Holds a list of cached connection strings.
        /// </summary>
        internal static readonly ConcurrentDictionary<string, string> ConnectionStrings = new ConcurrentDictionary<string, string>();
        /// <summary>
        /// The logger
        /// </summary>
        internal readonly ILogger Logger;
        /// <summary>
        /// The service principle configuration
        /// </summary>
        internal readonly ServicePrincipleConfig ServicePrincipleConfig;
        /// <summary>
        /// The msi configuration
        /// </summary>
        internal readonly MsiConfig MsiConfig;
        /// <summary>
        /// The connection string
        /// </summary>
        internal string ConnectionString;

        /// <summary>
        /// The cloud client
        /// </summary>
        private CloudTableClient _cloudClient;
        /// <summary>
        /// The expiry time
        /// </summary>
        private DateTimeOffset? _expiryTime;
        /// <summary>
        /// The instance name
        /// </summary>
        private readonly string _instanceName;
        /// <summary>
        /// The subscription identifier
        /// </summary>
        private readonly string _subscriptionId;

        /// <summary>
        /// Gets the cloud table client.
        /// </summary>
        /// <value>The cloud table client.</value>
        internal CloudTableClient CloudTableClient
        {
            get
            {
                if (_cloudClient == null || _expiryTime <= DateTime.UtcNow)
                {
                    InitializeClient();
                }

                return _cloudClient;
            }
        }

        /// <summary>Name of the object instance.</summary>
        public string Name { get; set; } 

        /// <summary>
        /// Initializes the client.
        /// </summary>
        /// <exception cref="InvalidOperationException">Cannot find storage account using connection string</exception>
        private void InitializeClient()
        {
            if (ConnectionString.IsNullOrEmpty())
            {
                ConnectionString = BuildStorageConnection().GetAwaiter().GetResult();
            }

            CloudStorageAccount.TryParse(ConnectionString, out var storageAccount);

            if (storageAccount == null)
            {
                throw new InvalidOperationException("Cannot find storage account using connection string");
            }

            // Create the CloudTableClient that represents the Table storage endpoint for the storage account.
            _cloudClient = storageAccount.CreateCloudTableClient();
            CloudTableClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromMilliseconds(500), 3);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorageBase"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected TableStorageBase(ConnectionConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.ThrowIfInvalid();

            Logger = logger;
            ConnectionString = config.ConnectionString;
            Name = config.InstanceName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorageBase"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected TableStorageBase(MsiConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.ThrowIfInvalid();

            Logger = logger;
            MsiConfig = config;
            Name = config.InstanceName;

            _instanceName = config.InstanceName;
            _subscriptionId = config.SubscriptionId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorageBase"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        protected TableStorageBase(ServicePrincipleConfig config, ILogger logger = null)
        {
            // Ensure all mandatory fields are set.
            config.ThrowIfInvalid();

            Logger = logger;
            ServicePrincipleConfig = config;
            Name = config.InstanceName;

            _instanceName = config.InstanceName;
            _subscriptionId = config.SubscriptionId;
        }

        /// <summary>
        /// Builds a connection string for the storage account when none was specified during initialisation.
        /// </summary>
        /// <returns>Connection <see cref="string" /></returns>
        /// <exception cref="InvalidOperationException">If the Storage Namespace can not be resolved or access keys are not configured.</exception>
        internal async Task<string> BuildStorageConnection()
        {
            try
            {
                // If we already have the connection string for this instance - don't go get it again.
                if (ConnectionStrings.TryGetValue(_instanceName, out var connStr))
                {
                    return connStr;
                }

                const string azureManagementAuthority = "https://management.azure.com/";
                const string windowsLoginAuthority = "https://login.windows.net/";
                string token;

                // Use Msi Config if it's been specified, otherwise, use Service principle.
                if (MsiConfig != null)
                {
                    // Managed Service Identity (MSI) authentication.
                    var provider = new AzureServiceTokenProvider();
                    token = provider.GetAccessTokenAsync(azureManagementAuthority, MsiConfig.TenantId).GetAwaiter().GetResult();

                    if (string.IsNullOrEmpty(token)) {
                        throw new InvalidOperationException("Could not authenticate using Managed Service Identity, ensure the application is running in a secure context");
                    }

                    _expiryTime = DateTime.Now.AddDays(1);
                }
                else
                {
                    // Service Principle authentication
                    // Grab an authentication token from Azure.
                    var context = new AuthenticationContext($"{windowsLoginAuthority}{ServicePrincipleConfig.TenantId}");

                    var credential = new ClientCredential(ServicePrincipleConfig.AppId, ServicePrincipleConfig.AppSecret);
                    var tokenResult = context.AcquireTokenAsync(azureManagementAuthority, credential).GetAwaiter().GetResult();

                    if (tokenResult == null || tokenResult.AccessToken == null) {
                        throw new InvalidOperationException($"Could not authenticate to {windowsLoginAuthority}{ServicePrincipleConfig.TenantId} using supplied AppId: {ServicePrincipleConfig.AppId}");
                    }

                    _expiryTime = tokenResult.ExpiresOn;
                    token = tokenResult.AccessToken;
                }

                // Set credentials and grab the authenticated REST client.
                var tokenCredentials = new TokenCredentials(token);

                var client = RestClient.Configure()
                    .WithEnvironment(AzureEnvironment.AzureGlobalCloud)
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.BodyAndHeaders)
                    .WithCredentials(new AzureCredentials(tokenCredentials, tokenCredentials, string.Empty, AzureEnvironment.AzureGlobalCloud))
                    .WithRetryPolicy(new RetryPolicy(new HttpStatusCodeErrorDetectionStrategy(), new FixedIntervalRetryStrategy(3, TimeSpan.FromMilliseconds(500))))
                    .Build();

                // Authenticate against the management layer.
                var azureManagement = Azure.Authenticate(client, string.Empty).WithSubscription(_subscriptionId);

                // Get the storage namespace for the passed in instance name.
                var storageNamespace = azureManagement.StorageAccounts.List().FirstOrDefault(n => n.Name == _instanceName);

                // If we cant find that name, throw an exception.
                if (storageNamespace == null)
                {
                    throw new InvalidOperationException($"Could not find the storage instance {_instanceName} in the subscription Id specified");
                }

                // Storage accounts use access keys - this will be used to build a connection string.
                var accessKeys = await storageNamespace.GetKeysAsync();

                // If the access keys are not found (not configured for some reason), throw an exception.
                if (accessKeys == null)
                {
                    throw new InvalidOperationException($"Could not find access keys for the storage instance {_instanceName}");
                }

                // We just default to the first key.
                var key = accessKeys[0].Value;

                // Build the connection string.
                var connectionString = $"DefaultEndpointsProtocol=https;AccountName={_instanceName};AccountKey={key};EndpointSuffix=core.windows.net";

                // Cache the connection string off so we don't have to reauthenticate.
                if (!ConnectionStrings.ContainsKey(_instanceName))
                {
                    ConnectionStrings.TryAdd(_instanceName, connectionString);
                }

                // Return connection string.
                return connectionString;
            }
            catch (Exception e)
            {
                _expiryTime = null;
                Logger?.LogError(e, "An exception occured during connection to Table storage");
                throw new InvalidOperationException("An exception occurred during service connection, see inner exception for more detail", e);
            }
        }
    }
}
