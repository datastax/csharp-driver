namespace Cassandra
{
    /// <summary>
    ///  The distance to a Cassandra node as assigned by a
    ///  <link>com.datastax.driver.core.policies.LoadBalancingPolicy</link> (through
    ///  its <c>* distance</c> method). The distance assigned to an host
    ///  influence how many connections the driver maintains towards this host. If for
    ///  a given host the assigned <c>HostDistance</c> is <c>Local</c> or
    ///  <c>Remote</c>, some connections will be maintained by the driver to
    ///  this host. More active connections will be kept to <c>Local</c> host
    ///  than to a <c>Remote</c> one (and thus well behaving
    ///  <c>LoadBalancingPolicy</c> should assign a <c>Remote</c> distance
    ///  only to hosts that are the less often queried). <p> However, if an host is
    ///  assigned the distance <c>Ignored</c>, no connection to that host will
    ///  maintained active. In other words, <c>Ignored</c> should be assigned to
    ///  hosts that should not be used by this driver (because they are in a remote
    ///  datacenter for instance).</p>
    /// </summary>
    public enum HostDistance
    {
        Local,
        Remote,
        Ignored
    }
}