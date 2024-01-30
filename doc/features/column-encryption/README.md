# Column Encryption

## Overview

Support for client-side encryption of data was added in version 3.20.0 of the C# driver. When using this feature data will be encrypted on-the-fly according to a provided implementation of `IColumnEncryptionPolicy`. This policy is also used to decrypt data in returned rows.  If a prepared statement is used, this decryption is transparent to the user; retrieved data will be decrypted and converted into the original
type according to definitions in the encryption policy. Support for simple (i.e. non-prepared) statements is also available, although in this case values must be wrapped using the `EncryptedValue` type.

Client-side encryption and decryption should work against all versions of Cassandra, DSE, and Astra. It does not utilize any server-side functionality to do its work.

## Configuration

Client-side encryption is enabled by creating an instance of an implementation of the `IColumnEncryptionPolicy` interface and adding information about columns to be encrypted to it. This policy is then supplied to the `Builder` via the `WithColumnEncryptionPolicy` method.

```csharp
var policy = new AesColumnEncryptionPolicy();
var cluster =
    Cluster.Builder()
        .AddContactPoint("127.0.0.1")
        .WithColumnEncryptionPolicy(policy)
        .Build();
```

### AESColumnEncryptionPolicy

`AESColumnEncryptionPolicy` is an implementation of `IColumnEncryptionPolicy` which provides encryption and decryption via AES-128, AES-192, or AES-256 according to the size of the key that is provided. This class is currently the only available column encryption policy implementation, although users can certainly implement their own by implementing `IColumnEncryptionPolicy` or subclassing the abstract `BaseColumnEncryptionPolicy` class which provides some out of the box functionality to manage the encrypted column metadata.

To mark a column as "encrypted" users need to call the `AddColumn` method. This method has two overloads, one of them has an additional `IColumnInfo` parameter which should only be used if the column has a type of `list`, `set`, `map`, `udt`, `tuple` or `custom`. If you are using one of these six types on that particular column then you need to provide a `IColumnInfo` object with a type that matches the cql type (e.g. `list` requires a `ListColumnInfo` object to be provided in the `AddColumn` method).

You need to provide the keyspace, table, and column names using the `AddColumn` method. Additionally, you need to provide a key for each column.

The key type for this specific policy (i.e. `AESColumnEncryptionPolicy`) is `AesKeyAndIV`. You can create objects of this type using the constructors. The key is mandatory but the IV is optional:

```csharp
var keyOnly = new AesColumnEncryptionPolicy.AesKeyAndIV(key, iv);
var keyAndIv = new AesColumnEncryptionPolicy.AesKeyAndIV(key);
```

If you don't provide an IV then a new one will be generated each time an encryption operation occurs. The final encrypted value contains the IV so that the driver can read this IV during read requests (before decrypting). This also means that the driver can discard the newly generated IV as soon as the encryption operation is done.

Using a newly generated IV on each encryption operation also means that the same input will result in a different server side value everytime an encryption occurs. This is important if the column is part of the primary key or an index for example.

If the encrypted column is part of a primary key, index, or `WHERE` clause of a statement, ensure that the encrypted value at the server side is always the same for the same input value. This requires you to provide a "static" IV instead of relying on the policy to generate a new one for every encryption.

This is an example of a table with two encrypted columns:

```
CREATE TABLE IF NOT EXISTS ks.table(id blob, address blob, public_notes text, PRIMARY KEY(id))
```

Our two encrypted columns are `id` and `address` (note the `blob` types, the server only sees encrypted bytes).

```
var base64key = "__BASE64_ENCODED_KEY__";
var base64IV = "__BASE64_ENCODED_IV__";

// key for the id column which is the primary key (static IV) 
var idKey = new AesColumnEncryptionPolicy.AesKeyAndIV(Convert.FromBase64String(base64key), Convert.FromBase64String(base64IV));

// key for the address column which is not used in any WHERE clause, primary key or index (no IV is provided - more secure)
var addressKey = new AesColumnEncryptionPolicy.AesKeyAndIV(Convert.FromBase64String(base64key));
```

Then, add these columns to the policy:

```csharp
var policy = new AesColumnEncryptionPolicy();
policy.AddColumn("ks", "table", "id", idKey, ColumnTypeCode.Uuid);
policy.AddColumn("ks", "table", "address", addressKey, ColumnTypeCode.Text);

var cluster =
    Cluster.Builder()
        .AddContactPoint("127.0.0.1")
        .WithColumnEncryptionPolicy(policy)
        .Build();
```

We added the `id` column with type `uuid` and `address` with type `text`. Avoid altering the types specified in the policy once you begin writing data to the columns. Changing the types afterward could result in a column containing two different types of encrypted data, creating compatibility problems for the driver. There is no server validation of these types because this is a client side feature. These types only have meaning at the client side, and the server only sees the `blob` data. The encryption key is also never sent to the server.

## Usage

### Encryption

#### Prepared Statements

Client-side encryption is most effective when used with prepared statements. A prepared statement is aware of information about the columns in the query it was built from. We can use this information to transparently encrypt any supplied parameters. For example, we can create a prepared statement to insert a value into `id` by executing the following code after creating a `Cluster` in the manner described above:

```csharp
var insertPs = await _session.PrepareAsync("INSERT INTO ks.table (id, address, public_notes) VALUES (?, ?, ?)").ConfigureAwait(false);

var userId = Guid.NewGuid();
var address = "Street X";
var publicNotes = "Public notes 1.";

var boundInsert = insertPs.Bind(userId, address, publicNotes);
await _session.ExecuteAsync(boundInsert).ConfigureAwait(false);
```

Our encryption policy will detect that "id" is an encrypted column and take appropriate action.

#### Simple Statements

Client-side encryption can also be used with simple queries, although such use cases are certainly not transparent. The driver provides an `EncryptedValue` struct that you can use to mark a specific parameter as "encrypted":

```csharp
var userId = Guid.NewGuid();
var address = "Street X";
var publicNotes = "Public notes 1.";

// using encrypted columns with SimpleStatements require the parameters to be wrapped with the EncryptedValue type
var insert = new SimpleStatement(
    "INSERT INTO ks.table (id, address, public_notes) VALUES (?, ?, ?)", 
    new EncryptedValue(userId, idKey), 
    new EncryptedValue(address, addressKey), 
    publicNotes);
await _session.ExecuteAsync(insert).ConfigureAwait(false);
```

Note that creating an instance of type `EncryptedValue` requires the key to be provided. This key is the same that was added to the policy via the `AddColumn` method for that particular column.

### Decryption

Decryption of values returned from the server is always transparent.  Whether we're executing a simple or prepared statement, encrypted columns will be decrypted automatically and made available via rows just like any other result.

## Example

The examples directory at the driver's Github repository [has an example for column encryption](https://github.com/datastax/csharp-driver/blob/master/examples/ColumnEncryption/ColumnEncryptionExample/Program.cs).

## Limitations

### Aliases

Using aliases when selecting encrypted columns is not supported because the driver will see the alias as the column name. This causes it not to match the column name that was configured in the encryption policy.

E.g.:

```
CREATE TABLE ks.table (encrypted blob PRIMARY KEY)

// this works normally
SELECT encrypted FROM ks.table;

// this will not work, the driver will not attempt to decrypt the column
SELECT encrypted as e FROM ks.table;
```

```csharp
var policy = new AesColumnEncryptionPolicy();
policy.AddColumn("ks", "table", "encrypted", KEY, TYPECODE);
```

### Named parameters

If you use a named parameter instead of `?` on a prepared statement, the driver will only be able to detect that the parameter should be encrypted if the name of the parameter matches the name of the column. This is essentially the same limitation as the "aliases" one above but there's a work around for this one: you can treat it as a Simple Statement and wrap the parameter value in an instance of the `EncryptedValue` type to tell the driver that the parameter should be encrypted.

```
CREATE TABLE ks.table (encrypted blob PRIMARY KEY)

// both of these 2 work normally
INSERT INTO ks.table (encrypted) VALUES (?);
INSERT INTO ks.table (encrypted) VALUES (:encrypted);

// this requires the value to be wrapped by an instance of the EncryptedValue class
INSERT INTO ks.table (encrypted) VALUES (:e);
```

```csharp
var insert = await session.PrepareAsync("INSERT INTO ks.table (encrypted) VALUES (:e)").ConfigureAwait(false);
var insertBoundStatement = insert.Bind(new EncryptedValue(VALUE, KEY));
await session.ExecuteAsync(insertBoundStatement).ConfigureAwait(false);
```

## Custom column encryption policy

If the provided `AesColumnEncryptionPolicy` does not suit your needs, use a custom implementation of `IColumnEncryptionPolicy`. There are two ways of doing this:

1. Sub class `BaseColumnEncryptionPolicy`
2. Implement `IColumnEncryptionPolicy` directly

`BaseColumnEncryptionPolicy` is an abstract class that provides some out of the box utility code to manage the encrypted column metadata (e.g. `AddColumn` method). It implements `IColumnEncryptionPolicy.GetColumnEncryptionMetadata(string ks, string table, string col)` so you only have to implement the actual encryption and decryption logic by overriding the `EncryptWithKey` and `DecryptWithKey` methods. 

Note that `BaseColumnEncryptionPolicy` has a type parameter. This will be the type of the key that has to be provided when adding columns with the `AddColumn` method. It is also the type of the key that is provided in `EncryptWithKey` and `DecryptWithKey`. This provides some type safety on top of the base `IColumnEncryptionPolicy` interface which declares the key type to be just `object`. For example, the `AesColumnEncryptionPolicy` uses the custom type `AesKeyAndIV` as the key type.

If `BaseColumnEncryptionPolicy` does not suit your needs, you can implement `IColumnEncryptionPolicy` directly. However, you will have to implement a way to manage the column encryption metadata in order to implement the `IColumnEncryptionPolicy.GetColumnEncryptionMetadata` method.