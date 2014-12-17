# Linq upgrade guide to 2.5

The purpose of this guide is to detail the changes made on the Linq component by the version 2.5 of the DataStax C# Driver that are relevant to an upgrade from version 2.1 or below.

Even though there are no breaking changes regarding Linq in 2.5, it is important that you read this guide in order to understand the new ways to use Linq and the Mapper components.

## Fluent mapping definitions

Previously, the only way to provide mapping information to the Linq component was using class and method attribute decoration.
Now, you can define mapping using a fluent interface.

```csharp
MappingConfiguration.Global.
  Define(new Map<User>().TableName("users"));
```

Additionally, you can now share the same mapping configuration between the new Mapper and Linq.

## Case sensitivity

Prior to version 2.5, Linq used case-sensitive identifiers when generating CQL code. Now, the case sensitivity can be specified on the mapping information.

Using fluent configuration:

```csharp
var map = new Map<User>.TableName("Users").CaseSensitive();
MappingConfiguration.Global.Define(map);
```

## Mapping attributes

Even though using the fluent interface over attribute decoration for mapping definition is encouraged, there are a new set of attributes declared in the `Cassandra.Mapping.Attributes` namespace that can be used to define mapping between your objects and Cassandra tables. These attributes can provide the Mapper and Linq components with all the necessary information.

The former set of attributes located in the `Cassandra.Data.Linq` namespace are still present in version 2.5 of the driver but they are only valid for the Linq component and they are now deprecated.

## Linq IQueryable instance

There is a new way to obtain IQueryable instances that can be used to query Cassandra, that is using the Table<T> constructor. Using the `Table<T>` constructor is an inexpensive operation and `Table<T>` instances can be created and dereferenced without much overhead.

Example 1:
Creates a new instance of the Linq IQueryProvider using the global mapping configuration.
```csharp
var users = new Table<User>(session);
```

Example 2:
Creates a new instance of the Linq IQueryProvider using the mapping configuration provided.
```csharp
var config = new MappingConfiguration();
var users = new Table<User>(session, config);
```

The ISession extension method  `GetTable<T>()` used to obtain a IQueryable instance prior to 2.5 is still available, internally it will call the new `Table<T>` constructors.
