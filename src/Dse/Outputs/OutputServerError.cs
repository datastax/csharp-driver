//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    internal class OutputServerError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ServerErrorException(Message);
        }

        protected override void Load(FrameReader reader)
        {
        }
    }
}