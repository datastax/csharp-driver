//
//      Copyright (C) 2012-2014 DataStax Inc.
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

namespace Cassandra.Connections
{
    internal interface IHostConnectionPool
    {
        /// <summary>
        /// Gets the total amount of open connections. 
        /// </summary>
        int OpenConnections { get; }

        /// <summary>
        /// Gets the total of in-flight requests on all connections. 
        /// </summary>
        int InFlight { get; }
    }
}