# Upgrade Guide to 2.5

The purpose of this guide is to detail the changes made by the version 2.5 of the DataStax C# Driver that are relevant to an upgrade from version 2.0.

## API changes

1. `Host.Address` field is now an `IPEndPoint` (IP address and port number) instead of an `IPAddress`.

1. There is one assembly delivered in the package (Cassandra.dll) and it is now strong-named.

1. [Linq API changes][linq-upgrade].

_If you have any question or comment, please [post it on the mailing list][mailing]._

  [mailing]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user "DataStax C# driver for Cassandra mailing list"
  [linq-upgrade]: https://github.com/datastax/csharp-driver/blob/master/doc/linq-upgrade-guide-2.5.md