//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    internal class OutputIsBootstrapping : OutputError
    {
        public override DriverException CreateException()
        {
            return new IsBootstrappingException(Message);
        }

        protected override void Load(FrameReader reader)
        {
        }
    }
}