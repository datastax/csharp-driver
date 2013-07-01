//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
﻿using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    ///  The policy that decides which Cassandra hosts to contact for each new query.
    ///  For efficiency purposes, the policy is expected to exclude down hosts from query plans.
    /// </summary>
    public interface ILoadBalancingPolicy
    {
        /// <summary>
        ///  Initialize this load balancing policy. <p> Note that the driver guarantees
        ///  that it will call this method exactly once per policy object and will do so
        ///  before any call to another of the methods of the policy.</p>
        /// </summary>
        /// <param name="cluster"> the  information about the session instance for which the policy is created.
        ///  </param>
        void Initialize(Cluster cluster);

        /// <summary>
        ///  Returns the distance assigned by this policy to the provided host. <p> The
        ///  distance of an host influence how much connections are kept to the node (see
        ///  <link>HostDistance</link>). A policy should assign a <code>* LOCAL</code>
        ///  distance to nodes that are susceptible to be returned first by
        ///  <code>newQueryPlan</code> and it is useless for <code>newQueryPlan</code> to
        ///  return hosts to which it assigns an <code>IGNORED</code> distance. </p><p> The
        ///  host distance is primarily used to prevent keeping too many connections to
        ///  host in remote datacenters when the policy itself always picks host in the
        ///  local datacenter first.</p>
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// 
        /// <returns>the HostDistance to <code>host</code>.</returns>
        HostDistance Distance(Host host);

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> Each new query will call this
        ///  method. The first host in the result will then be used to perform the query.
        ///  In the event of a connection problem (the queried host is down or appear to
        ///  be so), the next host will be used. If all hosts of the returned
        ///  <code>Iterator</code> are down, the query will fail.</p>
        /// </summary>
        /// <param name="query"> the query for which to build a plan. </param>
        /// 
        /// <returns>an iterator of Host. The query is tried against the hosts returned
        ///  by this iterator in order, until the query has been sent successfully to one
        ///  of the host.</returns>
        IEnumerable<Host> NewQueryPlan(Query query);
    }
}
