# CqlPoco - an object mapper (micro-ORM) for Cassandra
CqlPoco is a lightweight object mapper (micro-ORM) for Apache Cassandra.  You write queries with [CQL](http://www.datastax.com/documentation/cql/3.1/cql/cql_intro_c.html), and CqlPoco will take care of mapping rows returned from Cassandra to your classes.  It was inspired by [PetaPoco](https://github.com/toptensoftware/PetaPoco), [NPoco](https://github.com/schotime/NPoco), [Dapper.NET](https://github.com/StackExchange/dapper-dot-net) and the [cqlengine](https://github.com/cqlengine/cqlengine) project.

### A simple query
```csharp
public class User
{
    public Guid UserId { get; set; }
    public string Name { get; set; }
}

// Get a list of users from Cassandra
List<User> users = client.Fetch<User>("SELECT userid, name FROM users");
```
This works by mapping the column names in your CQL statement to the property names on the `User` class (using a case-insensitive match).  Simple scenarios like this are possible without doing any mapping configuration.

### Configuring CqlPoco
CqlPoco uses the [DataStax .NET driver](https://github.com/datastax/csharp-driver) for Apache Cassandra to execute queries.  When you install CqlPoco via the NuGet package, the driver will be installed as well.  As such, you'll want to start by getting an `ISession` instance from the driver using the Cluster builder.

```csharp
// Use the Cluster builder to connect to your Cassandra cluster
Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
ISession session = cluster.Connect("mykeyspace");
```
Once you've got your `ISession` instance, you can configure CqlPoco pretty easily.
```csharp
ICqlClient client = CqlClientConfiguration.ForSession(session).BuildCqlClient();
```
This `ICqlClient` instance is thread-safe and you'll want to take it and reuse it everywhere (maybe, for example, by registering it as a Singleton in your IoC container of choice).

### Defining mappings
In many scenarios, you'll need more control over how your class is mapped to Cassandra.  You have two options for telling CqlPoco how to map your classes: decorate your classes with attributes or define mappings in code using the fluent interface.

##### Attribute Example
```csharp
[TableName("users")]
[PrimaryKey("userid")]
public class User
{
    [Column("userid")]
    public Guid Id { get; set; }
    public string Name { get; set; }
}
```
##### Fluent Interface Example
```csharp
public class MyMappings : Mappings
{
    public MyMappings()
    {
        // Just define mappings in the constructor of your class that inherits from Mappings
        For<User>().TableName("users")
                   .PrimaryKey("userid")
                   .Column(u => u.Id, cm => cm.WithName("userid"));
    }
}
```
If you decide to go the fluent interface route, you can tell CqlPoco about your mappings when you configure it.  For example:
```csharp
ICqlClient client = CqlClientConfiguration.ForSession(session).UseMappings<MyMappings>().BuildCqlClient();
```

### Some other API examples
The simple query example is great, but CqlPoco's `ICqlClient` has a lot of other methods for doing things like Inserts, Updates, Deletes, selecting a single record and more.  And all methods come with their `async` counterparts.  Here's a quick sampling.

```csharp
// All query methods (Fetch, Single, First, etc.) will auto generate the SELECT and FROM clauses if not specified
List<User> users = client.Fetch<User>();
List<User> users = client.Fetch<User>("FROM users WHERE name = ?", someName);
List<User> users = client.Fetch<User>("WHERE name = ?", someName);

// Single and SingleOrDefault for getting a single record
var user = client.Single<User>("WHERE userid = ?", userId);
var user = client.SingleOrDefault<User>("WHERE userid = ?", userId);

// First and FirstOrDefault for getting first record
var user = client.First<User>("SELECT * FROM users");
var user = client.FirstOrDefault<User>("SELECT * FROM users");

// All query methods also support "flattening" to just the column's value type when selecting a single column
Guid userId = client.First<Guid>("SELECT userid FROM users");
List<string> names = client.Fetch<string>("SELECT name FROM users");

// Insert a POCO
var newUser = new User { UserId = Guid.NewGuid(), Name = "SomeNewUser" };
client.Insert(newUser);

// Update with POCO
someUser.Name = "A new name!";
client.Update(someUser);

// Update with CQL (will prepend table name to CQL)
client.Update<User>("SET name = ? WHERE userid = ?", someNewName, userId);

// Delete with POCO
client.Delete(someUser);

// Delete with CQL (will prepend table name to CQL)
client.Delete<User>("WHERE userid = ?", userId);
```


### Available on NuGet
CqlPoco is available on the [NuGet gallery](https://www.nuget.org/packages/CqlPoco).
```
PM> Install-Package CqlPoco
```

### License
Copyright 2014, Luke Tillman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
