using System;
using System.Linq;
using System.Threading;
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
                InstanceName = readConfig.GetValue<string>("StorageInstanceName"),
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

        [Fact]
        public void Test_AuditLogger_WriteLog()
        {
            Assert.True(_tableStorage.CheckTableExists(_tableStorage.AuditTableName).GetAwaiter().GetResult());
            Thread.Sleep(2000);

            _auditLogger.WriteLog("eventName1", "message1").GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName2", "message2", "source1", 1).GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName3", "message3", "source2", 0, 2).GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName4", "message4", "useridentifier1").GetAwaiter().GetResult();
            _auditLogger.WriteLog("eventName5", "message5", "userIdentifier2", "source3", 3).GetAwaiter().GetResult();

            Thread.Sleep(2000);
            Assert.True(_tableStorage.CheckTableExists(_tableStorage.AuditTableName).GetAwaiter().GetResult());

            Thread.Sleep(1000);
            var records = _tableStorage.ListEntities<AuditLog>(_tableStorage.AuditTableName, null, $"Timestamp ge datetime'{DateTime.Now.AddDays(-1).ToString("yyyy-MM-ddThh:mm:ss")}'").ToList();

            Assert.True(records.Count() >= 0);

            var count = _tableStorage.CountItems(_tableStorage.AuditTableName, "eventName1").GetAwaiter().GetResult();
            Assert.True(count > 0);
        }

        [Fact]
        public void Test_AuditLogger_CreatesTableIfNotExists()
        {
            if (_tableStorage.CheckTableExists(_tableStorage.AuditTableName).GetAwaiter().GetResult())
            {
                _tableStorage.DeleteTable(_tableStorage.AuditTableName).GetAwaiter().GetResult();
            }
            Thread.Sleep(40000);

            _auditLogger.WriteLog("event", "message").GetAwaiter().GetResult();

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
