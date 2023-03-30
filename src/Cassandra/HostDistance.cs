//
//      Copyright (C) DataStax Inc.
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