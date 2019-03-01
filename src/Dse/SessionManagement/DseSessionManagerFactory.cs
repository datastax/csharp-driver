//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.SessionManagement;

namespace Dse.SessionManagement
{
    internal class DseSessionManagerFactory : IDseSessionManagerFactory
    {

        public ISessionManager Create(IInternalDseCluster dseCluster, IInternalDseSession dseSession)
        {
            return new DseSessionManager();
        }
    }
}