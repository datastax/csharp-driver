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

using Cassandra.Connections;
using Cassandra.ProtocolEvents;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Moq;

namespace Cassandra.Tests.Connections
{
    internal class FakeControlConnectionFactory : IControlConnectionFactory
    {
        public IControlConnection Create(IProtocolEventDebouncer protocolEventDebouncer, ProtocolVersion initialProtocolVersion, Configuration config, Metadata metadata)
        {
            var cc = Mock.Of<IControlConnection>();
            Mock.Get(cc).Setup(c => c.Init()).Returns(TaskHelper.Completed);
            Mock.Get(cc).Setup(c => c.Serializer).Returns(new Serializer(ProtocolVersion.V3));
            return cc;
        }
    }
}