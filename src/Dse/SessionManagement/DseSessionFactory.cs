// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Threading.Tasks;
using Dse.Serialization;

namespace Dse.SessionManagement
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