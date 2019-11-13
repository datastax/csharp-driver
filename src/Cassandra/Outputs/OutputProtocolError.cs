//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    internal class OutputProtocolError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ProtocolErrorException(Message);
        }

        protected override void Load(FrameReader reader)
        {
        }
    }
}