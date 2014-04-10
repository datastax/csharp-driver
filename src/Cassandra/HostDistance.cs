namespace Cassandra
{
    /// <summary>
    ///  The distance to a Cassandra node as assigned by a
    ///  <link>com.datastax.driver.core.policies.LoadBalancingPolicy</link> (through
    ///  its <code>* distance</code> method). The distance assigned to an host
    ///  influence how many connections the driver maintains towards this host. If for
    ///  a given host the assigned <code>HostDistance</code> is <code>Local</code> or
    ///  <code>Remote</code>, some connections will be maintained by the driver to
    ///  this host. More active connections will be kept to <code>Local</code> host
    ///  than to a <code>Remote</code> one (and thus well behaving
    ///  <code>LoadBalancingPolicy</code> should assign a <code>Remote</code> distance
    ///  only to hosts that are the less often queried). <p> However, if an host is
    ///  assigned the distance <code>Ignored</code>, no connection to that host will
    ///  maintained active. In other words, <code>Ignored</code> should be assigned to
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