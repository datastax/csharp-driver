// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

namespace Dse.SessionManagement
{
    internal class DseSessionFactoryBuilder : ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession>
    {
        private readonly ISessionFactoryBuilder<IInternalCluster, IInternalSession> _sessionFactoryBuilder;

        public DseSessionFactoryBuilder(ISessionFactoryBuilder<IInternalCluster, IInternalSession> sessionFactoryBuilder)
        {
            _sessionFactoryBuilder = sessionFactoryBuilder;
        }

        public ISessionFactory<IInternalDseSession> BuildWithCluster(IInternalDseCluster cluster)
        {
            return new DseSessionFactory(cluster, _sessionFactoryBuilder.BuildWithCluster(cluster));
        }
    }
}