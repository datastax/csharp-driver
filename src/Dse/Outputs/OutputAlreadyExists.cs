//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    internal class OutputAlreadyExists : OutputError
    {
        private readonly AlreadyExistsInfo _info = new AlreadyExistsInfo();

        protected override void Load(FrameReader cb)
        {
            _info.Ks = cb.ReadString();
            _info.Table = cb.ReadString();
        }

        public override DriverException CreateException()
        {
            return new AlreadyExistsException(_info.Ks, _info.Table);
        }
    }
}