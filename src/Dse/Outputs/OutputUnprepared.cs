//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    internal class OutputUnprepared : OutputError
    {
        private readonly PreparedQueryNotFoundInfo _info = new PreparedQueryNotFoundInfo();

        protected override void Load(FrameReader cb)
        {
            short len = cb.ReadInt16();
            _info.UnknownId = new byte[len];
            cb.Read(_info.UnknownId, 0, len);
        }

        public override DriverException CreateException()
        {
            return new PreparedQueryNotFoundException(Message, _info.UnknownId);
        }
    }
}