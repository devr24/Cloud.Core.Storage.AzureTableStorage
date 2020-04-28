# **Cloud.Core.Storage.AzureTableStorage** 
[![Build status](https://dev.azure.com/cloudcoreproject/CloudCore/_apis/build/status/Cloud.Core%20Packages/Cloud.Core.Storage.AzureTableStorage_Package)](https://dev.azure.com/cloudcoreproject/CloudCore/_build/latest?definitionId=22)
![Code Coverage](https://cloud1core.blob.core.windows.net/codecoveragebadges/Cloud.Core.Storage.AzureTableStorage-LineCoverage.png) 
[![Cloud.Core.Configuration package in Cloud.Core feed in Azure Artifacts](https://feeds.dev.azure.com/cloudcoreproject/dfc5e3d0-a562-46fe-8070-7901ac8e64a0/_apis/public/Packaging/Feeds/8949198b-5c74-42af-9d30-e8c462acada6/Packages/e71ddf20-f66a-45da-b672-c32798cf1e51/Badge)](https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=package&feed=8949198b-5c74-42af-9d30-e8c462acada6&package=e71ddf20-f66a-45da-b672-c32798cf1e51&preferRelease=true)


<div id="description">

Azure specific implementation of table storage interface.  Uses the ITableStorage interface from _Cloud.Core_.

</div>

# **Usage**

## **Initialisation and Authentication Usage**

There are three ways you can instantiate the Blob Storage Client.  Each way dictates the security mechanism the client uses to connect.  The three mechanisms are:

1. Connection String
2. Service Principle
3. Managed Service Identity

Below are examples of instantiating each type.

#### 1. Connection String
Create an instance of the Table Storage client with ConnectionConfig for connection string as follows:

```csharp
var tableConfig = new ConnectionConfig
    {
        ConnectionString = "<connectionstring>"
    };

// Table client.
var tablestorage = new TableStorage(blobConfig);	
```
Note: Instance name not required to be specified anywhere in configuration here as it is taken from the connection string itself.

#### 2. Service Principle
Create an instance of the Table Storage client with TableStorageConfig for Service Principle as follows:

```csharp
var tableConfig = new ServicePrincipleConfig
    {
        AppId = "<appid>",
        AppSecret = "<appsecret>",
        TenantId = "<tenantid>",
        InstanceName = "<storageinstancename>",
        SubscriptionId = subscriptionId
    };

// Table client.
var tablestorage = new TableStorage(blobConfig);	
```

Usually the AppId, AppSecret (both of which are setup when creating a new service principle within Azure) and TenantId are specified in 
Configuration (environment variables/AppSetting.json file/key value pair files [for Kubernetes secret store] or command line arguments).

SubscriptionId can be accessed through the secret store (this should not be stored in config for security reasons).

#### 3. Management Service Idenity (MSI) 
This authentication also works for Managed User Identity.  Create an instance of the Table Storage client with MSI authentication as follows:

```csharp
var tableConfig = new MsiConfig
    {
        TenantId = "<tenantid>",
        InstanceName = "<storageinstancename>",
        SubscriptionId = subscriptionId
    };

// Table client.
var tablestorage = new TableStorage(blobConfig);	
```

All that's required is the instance name to connect to.  Authentication runs under the context the application is running.

## Dependency Injection

Inserting into dependency container:

```csharp
// Add multiple instances of state storage.
services.AddTableStorageSingletonNamed("TS1", "tableStorageInstance1", "test", "test"); // add to factory using a key
services.AddTableStorageSingletonNamed("TS1", "tableStorageInstance2", "test", "test"); // add to factory using a key
serviceCollection.AddTableStorageSingleton("tableStorageInstance3", "test", "test");    // add to factory using instance name

// Add state storage.
services.AddStateStorageSingleton("stateStorageInstance1", "test", "test");

// Add auditing.
services.AddAuditLogSingleton("auditInstance1", "test", "test");

// Sample consuming class.
services.AddTransient<MyClass>();
```

Using the dependencies:

```csharp
public class MyClass {

	private readonly ITableStorage _tableInstance1;
	private readonly ITableStorage _tableInstance2;
	private readonly ITableStorage _tableInstance3;
	private readonly IStateStorage _stateStorage;
	private readonly IAuditLogger _auditLogger;

	public MyClass(NamedInstanceFactory<ITableStorage> tableFactory, IStateStorage stateStorage, IAuditLogger auditLogger) 
	{	
		_tableInstance1 = tableFactory["TS1"];
		_tableInstance2 = tableFactory["TS2"];
		_tableInstance3 = tableFactory["tableStorageInstance3"];
		_stateStorage = stateStorage;
		_auditLogger = auditLogger;
	}
	
	...
}
```

## **Tabel Storage Usage**

### Insert or Update (Upsert)
The following code shows how to insert a new record, or replace an existing record:

```csharp
// Insert a single item.
await tablestorage.UpsertEntity("tableName1", new SampleEntity() { Key ="partitionKey1/rowKey1", Name = "TEST1" });

// Insert mutliple items.
await tablestorage.UpsertEntites("tableName1", new List<Test>()
{
    new SampleEntity() { Key ="partitionKey1/rowKey2", Name = "TEST2"},
    new SampleEntity() { Key ="partitionKey1/rowKey3",  Name = "TEST3"},
    new SampleEntity() { Key ="partitionKey1/rowKey4",  Name = "TEST4"},
});
```

You can plass any generic type into the UpsertEntity call, as long as it implemented ITableItem interface, ensuring it has a Key.  Sample class (used in this example) is defined as:

```csharp
public class SampleClass: ITableItem 
{
    public string Key { get; set }
    public string Name { get; set; }
    // You can add as many properties as you need to the class, as long as it has ITableItem, that's all that is needed.
}
```

### Retrieve
The following code shows how to retrieve an entity:

```csharp
// Retrieve a single entity.
var entity = await tablestorage.GetEntity<SampleEntity>("tableName1", "partitionKey1/rowKey1");

// You retrieve multiple within a given table or partition as follows...
// Observable:
var items = tablestorage.ListEntitiesObservable<SampleEntity>("tableName1", "PartitionKey eq 'partitionKey1'").Subscribe(e =>
{
    // Do some processing here.
    Console.WriteLine(e.Key);
});

// Enumerable:
var items = tablestorage.ListEntities<SampleEntity>("tableName1", "PartitionKey eq 'partitionKey1'");

foreach(var item in items)
{
    // Do some processing here.
    Console.WriteLine(e.Key);
}
```
Read more on the types of query you can use [here](https://docs.microsoft.com/en-us/dotnet/api/microsoft.windowsazure.storage.table.tablequery?view=azure-dotnet);

### Delete
You can delete a single or multiple entities as follows:

```csharp
// Delete single.
await tablestorage.DeleteEntity("tableName1", "partitionKey1/rowKey1");

// Delete multiple.
await tablestorage.DeleteEntites("tableName1", new List<string>() { "partitionKey1/rowKey1", "partitionKey1/rowKey2", "partitionKey1/rowKey3" })
```


### Exists
Check to see if an entity exists with the supplied table and key as follows:

```csharp
var exists = await tablestorage.Exists("tableName1", "partitionKey1/rowKey1");
```

If you need anything more specific you can use the ListEntities method with a query containing exactly what you need.



## **State Storage Usage**
Table Storage can be used to store application state due to the implementation of IStateStorage interface from the Cloud.Core package. Any data type can be stored, all's needed is a key to look the data up.
If we take this sample class as the data that needs to be stored:

```csharp
public class SampleClass
{
    public string Prop1 { get; set; }
    public int Prop2 { get; set; }
    public object Prop3 { get; set; }
}
```

It can be used in the calling application as follows:

```csharp
// Initialise the store client.
IStateStorage _stateStore = new TableStorage(new MsiConfig()
{
    InstanceName = "{storageInstance}",
    SubscriptionId = "{subscriptionId}",
    TenantId = "{tenantId}"
});

// Check if you already have state stored.
bool isStateStored = await _stateStore.IsStateStored("MyKey");

// Set the value in the store as follows.
await _stateStore.SetState("MyKey", new SampleClass
{
    Prop1 = "propVal1", Prop2 = 12345, Prop3 = new object()
});

// Get the value from store as follows.
var storedData = await _stateStore.GetState<SampleClass>("MyKey");
```


## **Audit Logging Usage**
Table Storage can be used for audit logging due to its implementation of the IAuditLogging interface from the Cloud.Core package.  It can be utilised in the following way:

```csharp
IAuditLogger _auditLogger = new TableStorage(new MsiConfig()
{
    InstanceName = "{storageInstance}",
    SubscriptionId = "{subscriptionId}",
    TenantId = "{tenantId}"
});

// Audit message only (ignores user and changed object).
await _auditLogger.WriteLog("Some audit message");

// Audit message and object being changed (ignores user making the change).
await _auditLogger.WriteLog("Some audit message", "MyObject", 12345, 56789);

// Audit message and user making the change (ignores object being changed).
await _auditLogger.WriteLog("Some audit message", "robert.mccabe@cloudcore.com");

// Most commonly used - audit message, user making the change and object being changed.
await _auditLogger.WriteLog("Some audit message", "robert.mccabe@cloudcore.com", "MyObject", 12345, 56789);
```



**Note** - Do not update the Microsoft.IdentityModel.Clients.ActiveDirectory package.  It should be set to version 3.19.8.  This is the only package which overlaps between other Cloud.Core packages and must be kept inline (either update all or leave all as is currently).



## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the core repositories to gurantee a satisfactory level of testing), then the build will fail.

## Compatibility
This package has has been written in .net Standard and can be therefore be referenced from a .net Core or .net Framework application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on 
Windows (or Linux using Mono).
 
## Setup
This package is built using .net Standard 2.1 and requires the .net Core 3.1 SDK, it can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to a internal NuGet feed.  To consume this on your local development machine, please add the following feed to your feed sources in Visual Studio:
https://pkgs.dev.azure.com/cloudcoreproject/CloudCore/_packaging/Cloud.Core/nuget/v3/index.json
 
For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts


<img src="https://cloud1core.blob.core.windows.net/icons/cloud_core_small.PNG" />
