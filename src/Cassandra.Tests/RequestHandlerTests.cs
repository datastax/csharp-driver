//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cassandra.Requests;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class RequestHandlerTests
    {
        [Test]
        public void RequestHandlerRetryDecisionTest()
        {
            var policy = Cassandra.Policies.DefaultRetryPolicy;
            var statement = new SimpleStatement("SELECT WILL FAIL");
            //Using default retry policy the decision will always be to rethrow on read/write timeout
            var expected = RetryDecision.RetryDecisionType.Rethrow;
            var decision = RequestExecution<RowSet>.GetRetryDecision(new ReadTimeoutException(ConsistencyLevel.Quorum, 1, 2, true), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = RequestExecution<RowSet>.GetRetryDecision(new WriteTimeoutException(ConsistencyLevel.Quorum, 1, 2, "SIMPLE"), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = RequestExecution<RowSet>.GetRetryDecision(new UnavailableException(ConsistencyLevel.Quorum, 2, 1), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = RequestExecution<RowSet>.GetRetryDecision(new Exception(), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            //Expecting to retry when a Cassandra node is Bootstrapping/overloaded
            expected = RetryDecision.RetryDecisionType.Retry;
            decision = RequestExecution<RowSet>.GetRetryDecision(new OverloadedException(null), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);
            decision = RequestExecution<RowSet>.GetRetryDecision(new IsBootstrappingException(null), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);
            decision = RequestExecution<RowSet>.GetRetryDecision(new TruncateException(null), policy, statement, 0);
            Assert.AreEqual(expected, decision.DecisionType);
        }
    }
}
