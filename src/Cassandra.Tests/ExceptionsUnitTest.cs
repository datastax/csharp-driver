// 
//    Copyright (C) 2017 DataStax Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class ExceptionsUnitTest
    {
        [Test]
        public void WriteTimeoutException_BatchLog_Message_Test()
        {
            var ex = new WriteTimeoutException(ConsistencyLevel.LocalOne, 0, 1, "BATCH_LOG");
            StringAssert.Contains("Server timeout during batchlog write at consistency LOCALONE", ex.Message);
            StringAssert.Contains(" (0 peer(s) acknowledged the write over 1 required)", ex.Message);
        }
        
        [Test]
        public void WriteTimeoutException_Query_Message_Test()
        {
            var ex = new WriteTimeoutException(ConsistencyLevel.Quorum, 8, 10, "SIMPLE");
            StringAssert.Contains("Server timeout during write query at consistency QUORUM", ex.Message);
            StringAssert.Contains(" (8 peer(s) acknowledged the write over 10 required)", ex.Message);
        }
        
        [Test]
        public void WriteFailureException_Message_Test()
        {
            var ex = new WriteFailureException(ConsistencyLevel.All, 8, 10, "SIMPLE", 2);
            StringAssert.Contains("Server failure during write query at consistency ALL", ex.Message);
            StringAssert.Contains(" (10 responses were required but only 8 replica responded, 2 failed)", ex.Message);
        }

        [Test]
        public void NoHostAvailableException_Message_Includes_No_Host_Tried()
        {
            var ex = new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>());
            Assert.AreEqual("No host is available to be queried (no host tried)", ex.Message);
        }

        [Test]
        public void NoHostAvailableException_Message_Includes_First_Host()
        {
            var ex = new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>
            {
                { new IPEndPoint(IPAddress.Parse("10.10.0.1"), 9042), new AuthenticationException("Bad credentials") }
            });
            Assert.AreEqual(
                "All hosts tried for query failed (tried 10.10.0.1:9042: AuthenticationException 'Bad credentials')", 
                ex.Message);
        }

        [Test]
        public void NoHostAvailableException_Message_Includes_Message_To_See_Errors_Property()
        {
            var ex = new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>
            {
                { new IPEndPoint(IPAddress.Parse("10.10.0.1"), 9042), new AuthenticationException("Bad credentials") },
                { new IPEndPoint(IPAddress.Parse("10.10.0.2"), 9042), new AuthenticationException("No credentials") },
                { new IPEndPoint(IPAddress.Parse("10.10.0.3"), 9042), new AuthenticationException("No credentials") }
            });
            Assert.AreEqual(
                "All hosts tried for query failed (tried 10.10.0.1:9042: AuthenticationException 'Bad credentials';" +
                " 10.10.0.2:9042: AuthenticationException 'No credentials'; ...), see Errors property for more info", 
                ex.Message);
        }

        [Test]
        public void NoHostAvailableException_Message_Can_Contain_Host_Without_Info()
        {
            var ex = new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>
            {
                { new IPEndPoint(IPAddress.Parse("10.10.0.1"), 9042), null },
                { new IPEndPoint(IPAddress.Parse("10.10.0.2"), 9042), new AuthenticationException("No credentials") }
            });
            Assert.AreEqual(
                "All hosts tried for query failed (tried 10.10.0.1:9042; 10.10.0.2:9042: AuthenticationException " +
                "'No credentials')", 
                ex.Message);
        }

        [Test]
        [TestCase(ConsistencyLevel.LocalQuorum, 2, 3, true,
            "LocalQuorum (3 response(s) were required but only 2 replica(s) responded, 1 failed)", 1)]
        [TestCase(ConsistencyLevel.LocalQuorum, 1, 2, true,
            "LocalQuorum (2 response(s) were required but only 1 replica(s) responded, 1 failed)", 1)]
        [TestCase(ConsistencyLevel.LocalOne, 1, 1, false, "LocalOne (the replica queried for data didn't respond)", 1)]
        [TestCase(ConsistencyLevel.LocalQuorum, 3, 3, true,
            "LocalQuorum (failure while waiting for repair of inconsistent replica)", 0)]
        public void ReadFailureException_Message_Includes_Amount_Of_Failures(ConsistencyLevel consistencyLevel,
                                                                             int received, int required,
                                                                             bool dataPresent,
                                                                             string expectedMessageEnd, int failures)
        {
            const string baseMessage = "Server failure during read query at consistency ";
            var expectedMessage = baseMessage + expectedMessageEnd;
            var ex = new ReadFailureException(consistencyLevel, received, required, dataPresent, failures);
            Assert.AreEqual(expectedMessage, ex.Message);
        }
    }
}