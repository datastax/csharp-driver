//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    /// <summary>
    /// The distance to a Cassandra node as assigned by a <see cref="ILoadBalancingPolicy"/> relative to the
    /// <see cref="ICluster"/> instance.
    /// <para>
    /// The distance assigned to a host influences how many connections the driver maintains towards this host.
    /// </para>
    /// </summary>
    public enum HostDistance
    {
        Local = 0,
        Remote = 1,
        Ignored = 2
    }
}