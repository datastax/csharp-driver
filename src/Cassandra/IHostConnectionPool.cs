//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
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