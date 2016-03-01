using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Auth
{
    internal interface IGssapiClient : IDisposable
    {
        void Init(string host);
        byte[] EvaluateChallenge(byte[] challenge);
    }
}
