//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    internal class OutputBadCredentials : OutputError
    {
        public override DriverException CreateException()
        {
            return new AuthenticationException(Message);
        }

        protected override void Load(FrameReader reader)
        {
        }
    }
}