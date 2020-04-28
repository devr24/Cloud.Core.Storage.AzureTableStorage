namespace Cloud.Core.Storage.AzureTableStorage
{
    using System;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Microsoft.Azure.Cosmos.Table;

    /// <summary>
    /// Auditing partial section of the TableStorage class.
    /// Implements the <see cref="IAuditLogger" />
    /// Implements the <see cref="TableStorageBase" />
    /// Implements the <see cref="IStateStorage" />
    /// Implements the <see cref="ITableStorage" />
    /// Implements the <see cref="IAuditLogger" />
    /// </summary>
    /// <seealso cref="TableStorageBase" />
    /// <seealso cref="IStateStorage" />
    /// <seealso cref="ITableStorage" />
    /// <seealso cref="IAuditLogger" />
    /// <seealso cref="IAuditLogger" />
    public partial class TableStorage : IAuditLogger
    {
        private static readonly string _appName = AppDomain.CurrentDomain.FriendlyName;
        private static readonly string _cleanAppName = _appName.Replace("AI.DL.", string.Empty);
        internal readonly string AuditTableName = $"Audit{_cleanAppName.ReplaceAll(new[] { ' ', ';', '.', '-' }, string.Empty) }";

        /// <summary>
        /// Writes a message to the audit log table.
        /// No information about the action taking place is logged here (value being set, previous value etc).
        /// Environment.UserName is taken as the name making the change as no username is passed.
        /// </summary>
        /// <param name="eventName">Name of the event (partition key) to group by.</param>
        /// <param name="message">The audit message to log.</param>
        /// <returns>Async Task that's been executed.</returns>
        public async Task WriteLog(string eventName, string message)
        {
            await WriteLogInternal(eventName, message, Environment.UserName, null, null, null);
        }

        /// <summary>
        /// Writes the audit message with a source and value.
        /// </summary>
        /// <param name="eventName">Name of the event (partition key) to group by.</param>
        /// <param name="message">The audit message to log.</param>
        /// <param name="source">The source object name.</param>
        /// <param name="currentValue">The current value to store in the audit log.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
        public async Task WriteLog(string eventName, string message, string source, object currentValue)
        {
            await WriteLogInternal(eventName, message, Environment.UserName, source, null, currentValue);
        }

        /// <summary>
        /// Writes a message to the audit log table, along with the name of the object being changed and its new and old values.
        /// Environment.UserName is taken as the name making the change as no username is passed.
        /// </summary>
        /// <param name="eventName">Name of the event (partition key) to group by.</param>
        /// <param name="message">The audit message to log.</param>
        /// <param name="source">The source object name.</param>
        /// <param name="previousValue">The previous value of the object.</param>
        /// <param name="currentValue">The current value of the object.</param>
        /// <returns>Async Task thats been executed.</returns>
        public async Task WriteLog(string eventName, string message, string source, object previousValue, object currentValue)
        {
            await WriteLogInternal(eventName, message, Environment.UserName, source, previousValue, currentValue);
        }

        /// <summary>
        /// Writes a log to the audit log table. Log includes message, username, source and current value.
        /// </summary>
        /// <param name="eventName">Name of the event (partition key) to group by.</param>
        /// <param name="message">The audit log message.</param>
        /// <param name="userIdentifier">The user identifier.</param>
        /// <param name="source">The source object name.</param>
        /// <param name="currentValue">The current value of the object.</param>
        /// <returns></returns>
        public async Task WriteLog(string eventName, string message, string userIdentifier, string source, object currentValue)
        {
            await WriteLogInternal(eventName, message, userIdentifier, source, null, currentValue);
        }

        /// <summary>
        /// Writes a log to the audit log table.  Log includes, message, username and source value information.
        /// </summary>
        /// <param name="eventName">Name of the event (partition key) to group by.</param>
        /// <param name="message">The audit log message.</param>
        /// <param name="userIdentifier">The user identifier.</param>
        /// <param name="source">The source object name.</param>
        /// <param name="previousValue">The previous value of the object.</param>
        /// <param name="currentValue">The current value of the object.</param>
        /// <returns>Async Task thats been executed.</returns>
        public async Task WriteLog(string eventName, string message, string userIdentifier, string source, object previousValue, object currentValue)
        {
            await WriteLogInternal(eventName, message, userIdentifier, source, previousValue, currentValue);
        }

        /// <summary>
        /// Writes a log to the Audit Log table.
        /// No information about the object being changed is captured.
        /// </summary>
        /// <param name="eventName">Name of the event (partition key) to group by.</param>
        /// <param name="message">The audit log message.</param>
        /// <param name="userIdentifier">The user identifier.</param>
        /// <returns>Async Task thats been executed.</returns>
        public async Task WriteLog(string eventName, string message, string userIdentifier)
        {
            await WriteLogInternal(eventName, message, userIdentifier, null, null, null);
        }

        private string GetFullAuditTableKey(string givenName = null)
        {
            if (givenName == null)
                givenName = AppDomain.CurrentDomain.FriendlyName;

            string dt = DateTime.Now.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff");
            return $"{givenName}/{dt}";
        }

        private async Task WriteLogInternal(string eventName, string message, string userIdentifier, string source, object previousValue, object currentValue)
        {
            // Store the objects (if they are set) as a json string.
            string prevVal = previousValue != null ? JsonConvert.SerializeObject(previousValue) : null;
            string currentVal = currentValue != null ? JsonConvert.SerializeObject(currentValue) : null;

            // Create the audit log record.
            var wrappedObj = new AuditLog()
            {
                Key = GetFullAuditTableKey(eventName),
                UserIdentifier = userIdentifier,
                SourceName = source,
                PreviousValue = prevVal,
                CurrentValue = currentVal,
                Message = message,
                AppName = _appName
            };

            try
            {
                // Add the audit log into the audit table.
                await UpsertEntity(AuditTableName, wrappedObj);
            }
            catch (StorageException ex) when (ex.Message == "Not Found")
            {
                //Create table if it does not exist
                await CreateTable(AuditTableName);
                await UpsertEntity(AuditTableName, wrappedObj);
            }
        }
    }

    /// <summary>
    /// Audit Log class - the information that is stored in the audit log table.
    /// Implements the <see cref="ITableItem" />
    /// </summary>
    /// <seealso cref="ITableItem" />
    internal class AuditLog : ITableItem
    {
        public string Key { get; set; }
        public string Message { get; set; }
        public string UserIdentifier { get; set; }
        public string SourceName { get; set; }
        public string PreviousValue { get; set; }
        public string CurrentValue { get; set; }
        public string AppName { get; set; }
    }
}

