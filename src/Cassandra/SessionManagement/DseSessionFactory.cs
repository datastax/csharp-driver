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

using System.Threading.Tasks;
using Cassandra.Serialization;

namespace Cassandra.SessionManagement
{
    internal class DseSessionFactory : ISessionFactory<IInternalDseSession>
    {
        private readonly IInternalDseCluster _dseCluster;
        private readonly ISessionFactory<IInternalSession> _sessionFactory;

        public DseSessionFactory(IInternalDseCluster dseCluster, ISessionFactory<IInternalSession> sessionFactory)
        {
            _dseCluster = dseCluster;
            _sessionFactory = sessionFactory;
        }

        public async Task<IInternalDseSession> CreateSessionAsync(string keyspace, Serializer serializer, string sessionName)
        {
            return new DseSession(await _sessionFactory.CreateSessionAsync(keyspace, serializer, sessionName).ConfigureAwait(false), _dseCluster);
        }
    }
}