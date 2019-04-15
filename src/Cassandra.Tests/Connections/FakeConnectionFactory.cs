// 
//       Copyright (C) 2019 DataStax Inc.
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
using System.Net;
using Cassandra.Connections;
using Cassandra.Serialization;

namespace Cassandra.Tests.Connections
{
    internal class FakeConnectionFactory : IConnectionFactory
    {
        private readonly Func<IConnection> _func;

        public FakeConnectionFactory(Func<IConnection> func)
        {
            _func = func;
        }
        public IConnection Create(Serializer serializer, IPEndPoint endpoint, Configuration configuration)
        {
            return _func();
        }
    }
}