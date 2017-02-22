//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    public class ToManyConnectionsPerHost : DriverException
    {
        public ToManyConnectionsPerHost() : base("Maximum number of connections per host reached")
        {
        }
    }
}