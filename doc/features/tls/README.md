# TLS/SSL

You can secure traffic between the driver and Apache Cassandra or DataStax Enterprise (DSE) with TLS/SSL. There are two aspects to that:

- Client-to-node encryption, where the traffic is encrypted and the client verifies the identity of the nodes it connects to.
- Optional client certificate authentication, where server nodes also verify the identity of the client.

This section describes the driver-side configuration, it assumes that you've already configured SSL encryption in the server. If you're using DSE, you can checkout the [server documentation that covers the basic procedures to setup SSL with DSE][client-to-node-dse]. If you're using Apache Cassandra, you can checkout the [documentation that covers the setup with Apache Cassandra][client-to-node].

You can find SSL examples on how to configure the driver for both server and client auth in the driver's [Github repository].

## Driver configuration

Use `Builder.WithSSL()` method to enable client TLS/SSL encryption:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoints(...)
    .WithSSL()
    .Build();
```

`Builder.WithSSL()` and `Builder.WithSSL(new SSLOptions())` enable TLS/SSL with the default configuration. The default configuration includes a `RemoteCertificateValidationCallback` that logs any errors returned by .NET's SSL API.

To customize this configuration check out the several SSL options available on the `SSLOptions` class:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoints(...)
    .WithSSL(new SSLOptions().SetCertificateRevocationCheck(true))
    .Build();
```

The .NET SSL API will return a `RemoteCertificateNameMismatch` error when the `HostName` does not match the name on the server certificate. By default the driver performs reverse DNS resolution to obtain the `HostName` from the server node's IP address. You can change this by providing a custom hostname resolver in the `SSLOptions.SetHostNameResolver(...)` method:

```csharp
var cluster = Cluster.Builder()
    .AddContactPoints(...)
    .WithSSL(new SSLOptions().SetHostNameResolver(...))
    .Build();
```

Alternatively, you can provide your own `RemoteCertificateValidationCallback` to handle the SSL errors and perform your own validation logic (see below for an example of this). Note that you can introduce security vulnerabilities with this so you should avoid it if you can.

### Enabling server authentication with a custom root certificate

If you have a custom (untrusted) root certificate, then there are two ways to provide it: using the system/user store or loading it manually in code.

#### Certificate store

On Windows, you can add this certificate to the `Trusted Root Certification Authorities` logical store. This can be done with `certmgr.msc` for example, which is a MMC (Microsoft Management Console) snap-in tool to manage certificates on Windows systems. On non Windows platforms, support for system/user certificate stores is limited and the location of this store depends on which SSL library is used so you might prefer loading the certificate manually in code.

#### Loading the certificate in code

An alternative to the certificate store is to load the certificate in code and provide a custom certificate validator using `SSLOptions.SetRemoteCertValidationCallback(RemoteCertificateValidationCallback)` that compares the root certificate of the chain returned by the server with your previously loaded root certificate.

Here is an example (`CustomValidator` here is a class that you would have to implement):

```csharp
// custom validator
var certificateValidator = new CustomValidator(new X509Certificate2(@"C:\path\to\ca.crt"));

var cluster = Cluster.Builder()
    .AddContactPoints("...")
    .WithSSL(new SSLOptions().SetRemoteCertValidationCallback(
        (sender, certificate, chain, errors) => certificateValidator.Validate(sender, certificate, chain, errors)))
    .Build();
```

The examples on the [driver's repository][Github repository] have a basic custom certificate validator which can be used as a starting point.

### Enabling client authentication

To enable client authentication, you need to provide the driver with the client certificate(s):

```csharp
var cluster = Cluster.Builder()
    .AddContactPoints("...")
    .WithSSL(new SSLOptions()
        // set client certificate collection
        .SetCertificateCollection(new X509Certificate2Collection
        {
            // use the following constructor if the certificate is password protected
            new X509Certificate2(@"C:\path\to\client_cert.pfx", "cert_password"),

            // use the following constructor if the certificate is not password protected
            //new X509Certificate2(@"C:\path\to\client_cert.pfx")
        )
    )
    .Build();
```

If you prefer to use the certificate store you can use `X509Store` to obtain a collection of all installed certificates (optionally you can filter this collection before providing it to the driver so that it only contains the relevant certificate). On Windows, the following example works if you add the client certificate to the `Personal` logical store (using `certmgr.msc` for example as [described previously][certstore]).

```csharp
X509Certificate2Collection collection;
using (var store = new X509Store(StoreLocation.LocalMachine))
{
    store.Open(OpenFlags.ReadOnly);
    collection = store.Certificates;
}

var cluster = Cluster.Builder()
    .AddContactPoints("...")
    .WithSSL(new SSLOptions().SetCertificateCollection(collection))
    .Build();
```

[certstore]: #certificate-store
[client-to-node-dse]: https://docs.datastax.com/en/security/6.7/security/encryptClientNodeSSL.html
[client-to-node]: https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureSSLClientToNode.html
[Github repository]: https://github.com/datastax/csharp-driver/tree/master/examples/Ssl