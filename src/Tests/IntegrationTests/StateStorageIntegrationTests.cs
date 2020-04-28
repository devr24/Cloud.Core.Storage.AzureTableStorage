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
                InstanceName = readConfig.GetValue<string>("InstanceName"),
                TenantId = readConfig.GetValue<string>("TenantId"),
                SubscriptionId = readConfig.GetValue<string>("SubscriptionId"),
                AppId = readConfig.GetValue<string>("AppId"),
                AppSecret = readConfig.GetValue<string>("AppSecret"),
            };
            _tableStorage = new TableStorage(config);
            _tableStorage.CreateTable(_tableStorage.StateStorageTableName).GetAwaiter().GetResult();
            _stateStorage = _tableStorage;
        }

        /// <summary>Verify an object is stored in state as expected.</summary>
        [Fact]
        public async Task Test_StateStorage_SetAndGetState()
        {
            // Arrange - object that should be stored.
            var testStorageObj = new TestStorage { Name = "Tester" };

            // Act - Store in state.
            await _stateStorage.SetState("myobj", testStorageObj);
            var retrieved = await _stateStorage.GetState<TestStorage>("myobj");
            var stateStored = await _stateStorage.IsStateStored("myobj");

            // Assert
            Assert.True(stateStored);
            retrieved.Name.Should().Be(testStorageObj.Name);
        }
    }

    public class TestStorage
    {
        public string Name { get; set; }
    }
}
