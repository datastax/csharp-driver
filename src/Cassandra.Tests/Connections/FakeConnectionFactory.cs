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

using System;
using System.Collections.Concurrent;
using System.Net;
using Cassandra.Connections;
using Cassandra.Serialization;
using Moq;

namespace Cassandra.Tests.Connections
{
    internal class FakeConnectionFactory : IConnectionFactory
    {
        private readonly Func<IPEndPoint, IConnection> _func;
        public event Action<IConnection> OnCreate;

        public ConcurrentDictionary<IPEndPoint, ConcurrentQueue<IConnection>> CreatedConnections { get; } = new ConcurrentDictionary<IPEndPoint, ConcurrentQueue<IConnection>>();
        
        public FakeConnectionFactory() : this(Mock.Of<IConnection>)
        {
        }

        public FakeConnectionFactory(Func<IConnection> func)
        {
            _func = _ => func();
        }
        
        public FakeConnectionFactory(Func<IPEndPoint, IConnection> func)
        {
            _func = func;
        }

        public IConnection Create(Serializer serializer, IPEndPoint endpoint, Configuration configuration)
        {
            var connection = _func(endpoint);
            var queue = CreatedConnections.GetOrAdd(endpoint, _ => new ConcurrentQueue<IConnection>());
            queue.Enqueue(connection);
            OnCreate?.Invoke(connection);
            return connection;
        }
    }
}