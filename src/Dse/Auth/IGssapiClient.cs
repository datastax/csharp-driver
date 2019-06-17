//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Auth
{
    internal interface IGssapiClient : IDisposable
    {
        void Init(string service, string host);
        byte[] EvaluateChallenge(byte[] challenge);
    }
}
