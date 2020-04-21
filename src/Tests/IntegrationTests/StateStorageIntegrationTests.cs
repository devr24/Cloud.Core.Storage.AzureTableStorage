using System.Threading.Tasks;
using Cloud.Core.Storage.AzureTableStorage.Config;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Storage.AzureTableStorage.Tests.IntegrationTests
{
    [IsIntegration]
    public class StateStorageIntegrationTests
    {
        private readonly TableStorage _tableStorage;
        private readonly IStateStorage _stateStorage;

        public StateStorageIntegrationTests()
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
            _tableStorage.CreateTable(_tableStorage.StateStorageTableName).GetAwaiter().GetResult();
            _stateStorage = _tableStorage;
        }

        [Fact]
        public async Task Test_StateStorage_SetAndGetState()
        {
            var testStorageObj = new TestStorage { Name = "Tester" };

            await _stateStorage.SetState<TestStorage>("myobj", testStorageObj);

            Assert.True(await _stateStorage.IsStateStored("myobj"));

            var retrieved = await _stateStorage.GetState<TestStorage>("myobj");

            retrieved.Name.Should().Be(testStorageObj.Name);
        }
    }

    public class TestStorage
    {
        public string Name { get; set; }
    }
}
