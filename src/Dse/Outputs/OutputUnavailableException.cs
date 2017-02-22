//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    internal class OutputUnavailableException : OutputError
    {
        private ConsistencyLevel _consistency;
        private int _required;
        private int _alive;

        protected override void Load(FrameReader cb)
        {
            _consistency = (ConsistencyLevel) cb.ReadInt16();
            _required = cb.ReadInt32();
            _alive = cb.ReadInt32();
        }

        public override DriverException CreateException()
        {
            return new UnavailableException(_consistency, _required, _alive);
        }
    }
}