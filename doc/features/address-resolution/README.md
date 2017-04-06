# Address resolution

The driver auto-detects new DSE nodes when they are added to the cluster by means of server-side push
notifications and checking the system tables.

For each node, the address the driver receives the address set as [`rpc_address` in the node’s cassandra.yaml file][rpc]
(or `broadcast_rpc_address` when defined). In most cases, this is the correct value, however, sometimes the
addresses received in this manner are either not reachable directly by the driver or are not the preferred address
to use. A common such scenario is a multi-datacenter deployment with a client connecting using the private IP
address to the local datacenter (to reduce network costs) and the public IP address for the remote datacenter nodes.

## The AddressTranslator interface

The `IAddressTranslator` interface allows you to deal with such cases, by transforming the address sent by a
DSE node to another address to be used by the driver for connection.

```csharp
namespace Dse
{
    public interface IAddressTranslator
    {
        IPEndPoint Translate(IPEndPoint address);
    }
}
```

You then configure the driver to use your `IAddressTranslator` implementation in the client options.

```csharp
var cluster = Cluster.Builder()
    .AddContactPoint("1.2.3.4")
    .WithAddressTranslator(new MyAddressTranslator())
    .Build();
```

Note: The contact points provided while building the `Cluster` are not translated, only addresses retrieved from or
sent by DSE nodes are.

[rpc]: https://docs.datastax.com/en/cassandra/2.1/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__rpc_address