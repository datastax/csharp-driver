# User-defined types

Cassandra 2.1 introduced support for [User-defined types (UDT)][cql-udt]. A user-defined type simplifies handling a group of
related properties.

A quick example is a user account table that contains address details described through a set of columns:
street, city, zip code. With the addition of UDTs, you can define this group of properties as a type and access
them as a single entity or separately.

User-defined types are declared at the keyspace level.

In your application, you can map your UDTs to application entities. For example, given the following UDT:

```sql
CREATE TYPE address (
   street text,
   city text,
   zip int,
   phones list<text>
);
```

You create a C# class that maps to the UDT:

```csharp
public class Address
{
   public string Street { get; set; }
   public string City { get; set; }
   public int Zip { get; set; }
   public IEnumerable<string> Phones { get; set;}
}
```

You declare the mapping once at the session level:

```csharp
await session.UserDefinedTypes.DefineAsync(
      UdtMap.For<Phone>()
            .Map(v => v.Alias, "alias")
            .Map(v => v.CountryCode, "country_code")
            .Map(v => v.Number, "number")).ConfigureAwait(false);
```

You can also provide the keyspace when declaring a UDT. This is useful for these situations:
- If the UDT is defined on a keyspace which is not the default - which can be set via `Session.Connect(string)` or `Builder.WithDefaultKeyspace(string)`
- if you don't declare a default keyspace with the methods mentioned above

To provide the keyspace when declaring a UDT:
```csharp
await session.UserDefinedTypes.DefineAsync(
      UdtMap.For<Phone>(keyspace: "keyspace")
            .Map(v => v.Alias, "alias")
            .Map(v => v.CountryCode, "country_code")
            .Map(v => v.Number, "number")).ConfigureAwait(false);
```

Once declared the mapping will be available for the lifetime of the application:

```csharp
var results = session.Execute("SELECT id, name, address FROM users where id = 756716f7-2e54-4715-9f00-91dcbea6cf50");
var row = results.First();
// You retrieve the field as a value of type Address
var userAddress = row.GetValue<Address>("address");
Console.WriteLine("The user lives on {0} Street", userAddress.Street);
```

## CQL column and C# class properties mismatch

For the automatic mapping to work, the table column names and the class properties must match (note that
column-to-field matching is case-insensitive). For example, in the UDT and the C# class examples above were changed:

```sql
CREATE TYPE address (
   street text,
   city text,
   zip_code int,
   phones list<text>
);
```

```csharp
public class Address
{
   public string Street { get; set; }
   public string City { get; set; }
   public int ZipCode { get; set; }
   public IEnumerable<string> Phones { get; set;}
}
```

You can also define the properties manually:

```csharp
session.UserDefinedTypes.Define(
   UdtMap.For<Address>()
      .Map(a => a.Street, "street")
      .Map(a => a.City, "city")
      .Map(a => a.Zip, "zip")
      .Map(a => a.Phones, "phones")
);
```

You can still use automatic mapping, but you must add a call to the Map method. For example:

```csharp
session.UserDefinedTypes.Define(
   UdtMap.For<Address>()
      .Automap()
      .Map(a => a.Zipcode, "zip_code")
);
```

## Nesting User-defined types In CQL

UDTs can be nested relatively arbitrarily. For the C# driver you have to define the mapping to all the user-defined
types used.

Based on the previous example, let's change the phones column from `set<text>` to a `set<phone>`, where phone
contains an alias, a number and a country code.

Phone UDT

```sql
CREATE TYPE phone ( 
   alias text,
   number text, 
   country_code int
);
```

Address UDT

```sql
CREATE TYPE address ( 
   street text, 
   city text, 
   zip_code int, 
   phones list<phone>
);
```

Now we can update the `Address` class to use the `Phone` class:

```csharp
public class Address 
{ 
   public string Street { get; set; } 
   public string City { get; set; } 
   public int ZipCode { get; set; } 
   public IEnumerable<Phone> Phones { get; set;} 
}
```

You have to define the mapping for both classes

```csharp
session.UserDefinedTypes.Define( 
   UdtMap.For<Phone>(), 
   UdtMap.For<Address>()
      .Automap()
      .Map(a => a.ZipCode, "zip_code")
);
```

After that, you can reuse the mapping within your application.

```csharp
var userAddress = row.GetValue<Address>("address"); 
var mainPhone = userAddress.Phones.First(); 
Console.WriteLine("User main phone is {0}", mainPhone.Alias);
```

[cql-udt]: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlRefUDType.html