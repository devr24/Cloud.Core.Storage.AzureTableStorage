﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Storage.AzureTableStorage.Config;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Storage.AzureTableStorage.Tests.IntegrationTests
{
    [IsIntegration]
    public class TableStorageIntegrationTests
    {
        private readonly TableStorage _tableStorageClient;
        private const string TestTableName = "Testing";

        public TableStorageIntegrationTests()
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
            _tableStorageClient = new TableStorage(config);
            _tableStorageClient.Name.Should().Be(config.InstanceName);
            _tableStorageClient.CreateTable(TestTableName).GetAwaiter().GetResult();
        }

        [Fact]
        public void Test_TableStorage_CreateAndDeleteTable()
        {
            _tableStorageClient.CreateTable("Test").GetAwaiter().GetResult();
            _tableStorageClient.CheckTableExists("Test").GetAwaiter().GetResult().Should().BeTrue();
            _tableStorageClient.DeleteTable("Test").GetAwaiter().GetResult();
        }

        [Fact]
        public async Task Test_TableStorage_UpsertSingle()
        {
            var key = "partition1/key1";
            var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            // Act - ensure there's an object to check for.
            var exists = await _tableStorageClient.Exists(TestTableName, key);

            // Assert
            exists.Should().Be(true);

            var tables = (await _tableStorageClient.ListTableNames()).ToList();
            tables.Count().Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task Test_TableStorage_UpsertMultiple()
        {
            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });

            // Act - ensure there's an object to check for.
            var exists1 = await _tableStorageClient.Exists(TestTableName, key1);
            var exists2 = await _tableStorageClient.Exists(TestTableName, key2);
            
            // Assert
            exists1.Should().Be(true);
            exists2.Should().Be(true);
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveSingle()
        {
            // Arrange - create test entity.
            var key = "partition1/key1";
            var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            // Act - ensure there's an object to check for.
            var result = await _tableStorageClient.GetEntity<SampleEntity>(TestTableName, key);
            await _tableStorageClient.DeleteEntity(TestTableName, key);

            // Assert
            result.Key.Should().Be(entity.Key);
            result.Name.Should().Be(entity.Name);
            result.OtherField.Should().Be(entity.OtherField);
        }

        [Fact]
        public async Task Test_TableStorage_CountItemsWithCallback()
        {
            var key = "partition1/key1";
            var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            var calls = 0;
            var count = await _tableStorageClient.CountItems(TestTableName, (i) => {
                calls++;
            });
            count.Should().BeGreaterThan(0);
            calls.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task Test_TableStorage_CountItems()
        {
            var key = "partition1/key1";
            var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            var count = await _tableStorageClient.CountItems(TestTableName);
            count.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task Test_TableStorage_CountItemsUsingKey()
        {
            var key1 = "partition1/key12345";
            var entity1 = new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity1);

            var key2 = "partition1/key2";
            var entity2 = new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity2);

            var key3 = "partition2/key2";
            var entity3 = new SampleEntity() { Key = key3, Name = "name3", OtherField = "other3" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity3);

            var countMatchingKey1 = await _tableStorageClient.CountItems(TestTableName, key1);
            countMatchingKey1.Should().Be(1);

            var countTableEntries = await _tableStorageClient.CountItems(TestTableName);
            countTableEntries.Should().BeGreaterThan(1);

            var countPartitioned = await _tableStorageClient.CountItems(TestTableName, "partition1");
            countPartitioned.Should().BeGreaterThan(countMatchingKey1).And.BeLessThan(countTableEntries);
        }

        [Fact]
        public async Task Test_TableStorage_CountItemsUsingQuery()
        {
            var key = "partition1/key12345";
            var entity = new SampleEntity() { Key = key, Name = "name", OtherField = "other" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            var count = await _tableStorageClient.CountItemsQuery(TestTableName, "PartitionKey eq 'partition1' and RowKey eq 'key12345'");
            count.Should().Be(1);
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveMultiple_EnumerableWithCols()
        {
            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            // Act
            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });
            var items = _tableStorageClient.ListEntities<SampleEntitySlim>(TestTableName, new List<string> { "Name" }, "PartitionKey eq 'partition1'").ToList();

            // Assert
            items.Count.Should().BeGreaterOrEqualTo(2);
            items.First().Name.Should().NotBeNullOrEmpty();
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveMultiple_EnumerableFiltered()
        {
            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            // Act
            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });
            var items = _tableStorageClient.ListEntities<SampleEntity>(TestTableName, "PartitionKey eq 'partition1'").ToList();

            // Assert
            items.Count.Should().BeGreaterOrEqualTo(2);
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveMultiple_Enumerable()
        {
            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            // Act
            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });
            var items = _tableStorageClient.ListEntities<SampleEntity>(TestTableName, new List<string> { "Name", "OtherField" }, "PartitionKey eq 'partition1'").ToList();

            // Assert
            items.Count.Should().BeGreaterOrEqualTo(2);
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveObservable_Filtered()
        {
            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            // Act
            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });

            var count = 0;
            var loops = 0;
            _tableStorageClient.ListEntitiesObservable<SampleEntity>(TestTableName, "PartitionKey eq 'partition1' and RowKey eq 'key1'").Subscribe(e =>
                {
                    count++;
                });

            // Wait for subscription.
            do
            {
                Thread.Sleep(500);
                loops++;
            } while (loops < 5  || (loops < 10 && count == 0));

            // Assert
            count.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveObservable_SelectCols()
        {

            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            // Act
            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });

            var count = 0;
            var loops = 0;

            _tableStorageClient.ListEntitiesObservable<SampleEntitySlim>(TestTableName, new List<string> { "Name" }).Subscribe(e =>
            {
                e.Name.Should().Contain("name");
                count++;
            });

            // Wait for subscription.
            do
            {
                Thread.Sleep(500);
                loops++;
            } while (loops < 5 || (loops < 10 && count == 0));

            // Assert
            count.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task Test_TableStorage_RetrieveObservable_SelectFilteredWithCols()
        {

            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            // Act
            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });

            var count = 0;
            var loops = 0;

            _tableStorageClient.ListEntitiesObservable<SampleEntitySlim>(TestTableName, 
                new List<string> { "Name" }, "PartitionKey eq 'partition1' and RowKey eq 'key1'").Subscribe(e =>
            {
                e.Name.Should().Contain("name");
                count++;
            });

            // Wait for subscription.
            do
            {
                Thread.Sleep(500);
                loops++;
            } while (loops < 5 || (loops < 10 && count == 0));

            // Assert
            count.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task Test_TableStorage_DeleteSingle()
        {
            // Arrange - create test entity.
            var key = "partition1/key1";
            var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            // Act - ensure there's an object to check for.
            var exists = await _tableStorageClient.Exists(TestTableName, key);
            await _tableStorageClient.DeleteEntity(TestTableName, key);
            var doesntExist = await _tableStorageClient.Exists(TestTableName, key);

            // Assert
            exists.Should().Be(true);
            doesntExist.Should().Be(false);
        }

        [Fact]
        public async Task Test_TableStorage_DeleteMultiple()
        {
            // Arrange - create test entity.
            var key1 = "partition1/key1";
            var key2 = "partition1/key2";

            await _tableStorageClient.UpsertEntities(TestTableName, new List<SampleEntity>
            {
                new SampleEntity() { Key = key1, Name = "name1", OtherField = "other1" },
                new SampleEntity() { Key = key2, Name = "name2", OtherField = "other2" }
            });

            // Act - ensure there's an object to check for.
            var exists1 = await _tableStorageClient.Exists(TestTableName, key1);
            var exists2 = await _tableStorageClient.Exists(TestTableName, key2);

            await _tableStorageClient.DeleteEntities(TestTableName, new List<string>(){ key1, key2 });

            var doesntExist1 = await _tableStorageClient.Exists(TestTableName, key1);
            var doesntExist2 = await _tableStorageClient.Exists(TestTableName, key2);

            // Assert
            exists1.Should().Be(true);
            exists2.Should().Be(true);
            doesntExist1.Should().Be(false);
            doesntExist2.Should().Be(false);
        }

        [Fact]
        public async Task Test_TableStorage_CheckExists()
        {
            // Arrange - create test entity.
            var key = "partition1/key1";
            var entity = new SampleEntity() { Key = key, Name = "name1", OtherField = "other1" };
            await _tableStorageClient.UpsertEntity(TestTableName, entity);

            // Act - ensure there's an object to check for.
            var exists = await _tableStorageClient.Exists(TestTableName, key);
            await _tableStorageClient.DeleteEntity(TestTableName, key);
            var doesntExist = await _tableStorageClient.Exists(TestTableName, key);

            // Assert
            exists.Should().Be(true);
            doesntExist.Should().Be(false);
        }

        public class SampleEntitySlim: ITableItem
        {
            public string Key { get; set; }
            public string Name { get; set; }
        }
        
        private class SampleEntity : ITableItem
        {
            public string Key { get; set; }
            public string Name { get; set; }
            public string OtherField { get; set; }
            public int? OtherField2 { get; set; }
            public Guid TestGuid { get; set; } = new Guid();
            public long TestLong { get; set; } = 1;
            public double TestDouble { get; set; } = 1.1;
            public DateTimeOffset TestOffset { get; set; } = DateTime.Now;
            public DateTime TestDateTime { get; set; } = DateTime.Now;
            public bool TestBool { get; set; } = true;
            public List<string> TestArray { get; set; } = new List<string> { "test" };
            public object TestObject { get; set; } = "test";
        }
    }
}
