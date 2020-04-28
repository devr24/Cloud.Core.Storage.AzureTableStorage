using System;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Storage.AzureTableStorage.Config;
using Cloud.Core.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Storage.AzureTableStorage.Tests.IntegrationTests
{
    [IsIntegration]
    public class AuditLogIntegrationTests
    {
        private readonly TableStorage _tableStorage;
        private readonly IAuditLogger _auditLogger;

        public AuditLogIntegrationTests()
        {
            var readConfig = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            var config = new ServicePrincipleConfig
            {
                InstanceName = readConfig.GetValue<string>("InstanceName"),
                TenantId = readConfig.GetValue<string>("TenantId"),
                SubscriptionId = readConfig.GetValue<string>("SubscriptionId"),
                AppId = readConfig.GetValue<string>("AppId"),
                AppSecret = readConfig.GetValue<string>("AppSecret"),
            };
            _tableStorage = new TableStorage(config);
            _tableStorage.CreateTable(_tableStorage.AuditTableName).GetAwaiter().GetResult();
            Thread.Sleep(5000);

            _auditLogger = _tableStorage;
        }

        /// <summary>Ensure audit logs are created as expected.</summary>
        [Fact]
        public async Task Test_AuditLogger_WriteLog()
        {
            // Arrange - Ensure the audit table exists before starting.
            var tableExists = _tableStorage.CheckTableExists(_tableStorage.AuditTableName).GetAwaiter().GetResult();
            Assert.True(tableExists);
            Thread.Sleep(2000);

            // Act - write audit logs.
            _auditLogger.WriteLog("eventName1", "message1").GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName2", "message2", "source1", 1).GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName3", "message3", "source2", 0, 2).GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName4", "message4", "useridentifier1").GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName5", "message5", "userIdentifier2", "source3", 3).GetAwaiter().GetResult();
            Thread.Sleep(1000);

            var count = await _tableStorage.CountItemsQuery(_tableStorage.AuditTableName, $"Timestamp ge datetime'{DateTime.Now.AddDays(-1):yyyy-MM-ddThh:mm:ss}'");
            var countEvent1 = await _tableStorage.CountItems(_tableStorage.AuditTableName, "eventName1");

            // Assert
            Assert.True(count >= 0);
            Assert.True(countEvent1 > 0);
        }

        /// <summary>Ensure the table is created if it does not exist.</summary>
        [Fact]
        public void Test_AuditLogger_CreatesTableIfNotExists()
        {
            // Arrange/Act - check table exists.
            if (_tableStorage.CheckTableExists(_tableStorage.AuditTableName).GetAwaiter().GetResult())
            {
                // Delete the table if it does.
                _tableStorage.DeleteTable(_tableStorage.AuditTableName).GetAwaiter().GetResult();
            }
            Thread.Sleep(40000);

            // Write the log message - should create the table again.
            _auditLogger.WriteLog("event", "message").GetAwaiter().GetResult();

            // Assert - verify the table exists.
            Assert.True(_tableStorage.CheckTableExists(_tableStorage.AuditTableName).GetAwaiter().GetResult());
        }
    }

    public class AuditLog : ITableItem
    {
        public string AppName { get; set; }
        public string CurrentValue { get; set; }
        public string Message { get; set; }
        public string UserIdentifier { get; set; }
        public string PreviousValue { get; set; }
        public string Key { get; set; }
    }
}
