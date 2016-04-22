using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Auth.Sspi;
using SSPI;

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
