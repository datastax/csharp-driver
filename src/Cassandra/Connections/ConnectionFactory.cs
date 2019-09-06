// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Net;
using Cassandra.Observers;
using Cassandra.Observers.Abstractions;
using Cassandra.Serialization;

namespace Cassandra.Connections
{
    internal class ConnectionFactory : IConnectionFactory
    {
        public IConnection Create(Serializer serializer, IConnectionEndPoint endPoint, Configuration configuration, IConnectionObserver connectionObserver)
        {
            return new Connection(serializer, endPoint, configuration, connectionObserver);
        }

        public IConnection CreateWithoutMetrics(Serializer serializer, IConnectionEndPoint endPoint, Configuration configuration)
        {
            return Create(serializer, endPoint, configuration, new NullConnectionObserver());
        }
    }
}