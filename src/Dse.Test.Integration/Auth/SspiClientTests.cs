//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using Dse.Auth;
using NUnit.Framework;

namespace Dse.Test.Integration.Auth
{
    public class SspiClientTests : BaseIntegrationTest
    {
        [Explicit(
            "It can only run when the host running the test is authenticated against the KDC")]
        [Test]
        public void Test_First_Step()
        {
//            var sspi = new SspiClient();
//            //For the server principal: "dse/cassandra1.datastax.com@DATASTAX.COM"
//            //the expected Uri is: "dse/cassandra1.datastax.com"
//            //sspi.Init("dse", "172.16.56.1");
//            //sspi.Init("host", "jorge-win.datastaxrealm.com");
//            sspi.Init("krbtgt", "DATASTAXREALM.COM");
//            sspi.EvaluateChallenge(null);
        }
    }
}
