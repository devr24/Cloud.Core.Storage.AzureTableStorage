using System;
using System.Collections.Generic;
using Cloud.Core.Exceptions;
using Cloud.Core.Storage.AzureTableStorage.Config;
using Cloud.Core.Storage.AzureTableStorage.Converters;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Cloud.Core.Storage.AzureTableStorage.Tests.IntegrationTests
{
    [IsUnit]
    public class TableStorageUnitTests
    {
        /// <summary>Add multiple instances and ensure table storage named instance factory resolves as expected.</summary>
        [Fact]
        public void Test_ServiceCollection_NamedInstances()
        {
            // Arrange
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeFalse();
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeFalse();
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeFalse();

            serviceCollection.AddTableStorageSingletonNamed("TS1", "tableStorageInstance1", "test", "test");
            serviceCollection.AddTableStorageSingletonNamed("TS2", "tableStorageInstance2", "test", "test");
            serviceCollection.AddTableStorageSingleton("tableStorageInstance3", "test", "test");
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();

            serviceCollection.AddStateStorageSingleton("stateStorageInstance1", "test", "test");
            serviceCollection.AddStateStorageSingleton("stateStorageInstance2", "test", "test");
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeTrue();

            serviceCollection.AddAuditLogSingleton("auditInstance1", "test", "test");
            serviceCollection.AddAuditLogSingleton("auditInstance2", "test", "test");
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeTrue();

            var provider = serviceCollection.BuildServiceProvider();
            var namedInstanceProv = provider.GetService<NamedInstanceFactory<ITableStorage>>();
            namedInstanceProv.Should().NotBeNull();

            namedInstanceProv["TS1"].Should().NotBeNull();
            namedInstanceProv["TS2"].Should().NotBeNull(); 
            namedInstanceProv["tableStorageInstance3"].Should().NotBeNull();
        }

        /// <summary>Check the IStateStorage is added to the service collection when using the new extension method.</summary>
        [Fact]
        public void Test_ServiceCollection_AddStateStorageSingleton()
        {
            // Arrange
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.AddStateStorageSingleton(new MsiConfig { InstanceName = "test", SubscriptionId = "test", TenantId = "test" });
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddStateStorageSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test" });
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddStateStorageSingleton(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddStateStorageSingleton("test", "test", "test");
            serviceCollection.ContainsService(typeof(IStateStorage)).Should().BeTrue();
        }

        /// <summary>Ensure config instance name is setup as expected.</summary>
        [Fact]
        public void Test_ConnectionConfig_InstanceName()
        {
            // Arrange
            var config1 = new ConnectionConfig();
            var config2 = new ConnectionConfig();
            var config3 = new ConnectionConfig();
            var config4 = new ConnectionConfig();

            // Act
            config2.ConnectionString = "AB";
            config3.ConnectionString = "A;B";
            config4.ConnectionString = "A;AccountName=B;C";

            // Assert
            config1.InstanceName.Should().BeNull();
            config2.InstanceName.Should().Be(null);
            config3.InstanceName.Should().Be(null);
            config4.InstanceName.Should().Be("B");
        }

        /// <summary>Check the ITableStorage is added to the service collection when using the new extension methods.</summary>
        [Fact]
        public void Test_ServiceCollection_AddTableStorageSingleton()
        {
            // Arrange
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.AddTableStorageSingleton("test", "test", "test");
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<ITableStorage>)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(object)).Should().BeFalse();
            serviceCollection.Clear();

            serviceCollection.AddTableStorageSingletonNamed("key1", "test", "test", "test");
            serviceCollection.AddTableStorageSingletonNamed("key2", "test", "test", "test");
            serviceCollection.AddTableStorageSingleton("test1", "test", "test");
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<ITableStorage>)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();

            var prov = serviceCollection.BuildServiceProvider();

            var resolvedFactory = prov.GetService<NamedInstanceFactory<ITableStorage>>();

            resolvedFactory["key1"].Should().NotBeNull();
            resolvedFactory["key2"].Should().NotBeNull();
            resolvedFactory["test1"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddTableStorageSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test" });
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddTableStorageSingleton(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddTableStorageSingleton("test", "test", "test");
            serviceCollection.ContainsService(typeof(ITableStorage)).Should().BeTrue();
        }

        /// <summary>Check the IAuditLog is added to the service collection when using the new extension method.</summary>
        [Fact]
        public void Test_ServiceCollection_AddAuditLogSingleton()
        {
            // Arrange
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.AddAuditLogSingleton(new MsiConfig { InstanceName = "test", SubscriptionId = "test", TenantId = "test" });
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddAuditLogSingleton(new ServicePrincipleConfig { InstanceName = "test", AppId = "test", AppSecret = "test", TenantId = "test", SubscriptionId = "test" });
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddAuditLogSingleton(new ConnectionConfig { ConnectionString = "test" });
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddAuditLogSingleton("test", "test", "test");
            serviceCollection.ContainsService(typeof(IAuditLogger)).Should().BeTrue();
        }

        /// <summary>Ensure validation is carried out as expected for MsiConfig.</summary>
        [Fact]
        public void Test_MsiConfig_Validation()
        {
            // Arrange
            var msiConfig = new MsiConfig();

            // Check the msi config validation.
            Assert.Throws<ValidateException>(() => msiConfig.ThrowIfInvalid());
            msiConfig.InstanceName = "test";
            Assert.Throws<ValidateException>(() => msiConfig.ThrowIfInvalid());
            msiConfig.TenantId = "test";
            Assert.Throws<ValidateException>(() => msiConfig.ThrowIfInvalid());
            msiConfig.SubscriptionId = "test";
            AssertExtensions.DoesNotThrow(() => msiConfig.ThrowIfInvalid());
        }

        /// <summary>Ensure validation is carried out as expected for ConnectionConfig.</summary>
        [Fact]
        public void Test_ConnectionConfig_Validation()
        {
            var connectionConfig = new ConnectionConfig();
            // Check connection string config validation.
            Assert.Throws<ValidateException>(() => connectionConfig.ThrowIfInvalid());
            connectionConfig.ConnectionString = "test";
            AssertExtensions.DoesNotThrow(() => connectionConfig.ThrowIfInvalid());
        }

        /// <summary>Ensure validation is carried out as expected for ServicePrincipleConfig.</summary>
        [Fact]
        public void Test_ServicePrincipleConfig_Validation()
        {
            var spConfig = new ServicePrincipleConfig();
            // Check the service Principle config validation.
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.InstanceName = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.AppId = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.AppSecret = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.TenantId = "test";
            Assert.Throws<ValidateException>(() => spConfig.ThrowIfInvalid());
            spConfig.SubscriptionId = "test";
            AssertExtensions.DoesNotThrow(() => spConfig.ThrowIfInvalid());
        }

        /// <summary>Ensure the table entity converter is convering objects as expected.</summary>
        [Fact]
        public void Test_TableEntityConverter_ConvertObject()
        {
            // Arrange
            var auditLog = new FakeAuditLog
            {
                Key = "testPartition/testRow",
                Message = "testMessage",
                UserIdentifier = "testUserIdentifier",
                AppName = "testAppName",
                CurrentValue = "testCurrentValue",
                PreviousValue = "testPreviousValue",
                TestArray = new List<string> { "test" },
                TestBool = true,
                TestDateTime = new DateTime(),
                TestDouble = 1.0,
                TestGuid = Guid.Empty,
                TestLong = 1,
                TestObject = "other",
                TestOffset = new DateTimeOffset()
            };

            // Act
            var objectConversion = new ObjectToTableEntityConverter(auditLog);
            var props = objectConversion.GetEntityProperties();
            var table = objectConversion.GetTableEntity();

            // Assert
            props.Count.Should().Be(13);
            props.ContainsKey("AppName").Should().BeTrue();
            props.ContainsKey("Message").Should().BeTrue();
            props.ContainsKey("UserIdentifier").Should().BeTrue();
            props.ContainsKey("CurrentValue").Should().BeTrue();
            props.ContainsKey("PreviousValue").Should().BeTrue();
            table.RowKey.Should().Be("testRow");
            table.PartitionKey.Should().Be("testPartition");
            table.ETag.Should().BeNull();
        }
    }

    internal class FakeAuditLog : AuditLog
    {
        public Guid TestGuid { get; set; }
        public long TestLong { get; set; }
        public double TestDouble { get; set; }
        public DateTimeOffset TestOffset { get; set; }
        public DateTime TestDateTime { get; set; }
        public bool TestBool { get; set; }
        public List<string> TestArray { get; set; }
        public object TestObject { get; set; }
    }
}

