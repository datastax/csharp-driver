//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
#if !NETCORE

namespace Dse.Auth
{
    internal static class GssapiClientFactory
    {
        internal static IGssapiClient CreateNew()
        {
            return new SspiClient();
        }
    }
}
#endif