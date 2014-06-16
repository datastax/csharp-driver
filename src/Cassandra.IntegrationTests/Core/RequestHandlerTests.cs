using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Core
{
    public class RequestHandlerTests : SingleNodeClusterTest
    {
        private new Session Session
        {
            get
            {
                //We will need the solid session for the internal methods.
                return (Session)base.Session;
            }
        }

        [Test]
        public void RequestHandlerRetryDecisionTest()
        {
            var statement = new SimpleStatement("SELECT WILL FAIL");
            var request = Session.GetRequest(statement);
            var requestHandler = new RequestHandler<RowSet>(Session, request, statement);

            //Using default retry policy the decision will always be to rethrow on read/write timeout
            var expected = RetryDecision.RetryDecisionType.Rethrow;
            var decision = requestHandler.GetRetryDecision(new ReadTimeoutException(ConsistencyLevel.Quorum, 1, 2, true));
            Assert.AreEqual(expected, decision.DecisionType);
            
            decision = requestHandler.GetRetryDecision(new WriteTimeoutException(ConsistencyLevel.Quorum, 1, 2, "SIMPLE"));
            Assert.AreEqual(expected, decision.DecisionType);

            decision = requestHandler.GetRetryDecision(new UnavailableException(ConsistencyLevel.Quorum, 2, 1));
            Assert.AreEqual(expected, decision.DecisionType);

            decision = requestHandler.GetRetryDecision(new Exception());
            Assert.AreEqual(expected, decision.DecisionType);

            //Expecting to retry when a Cassandra node is Bootstrapping/overloaded
            expected = RetryDecision.RetryDecisionType.Retry;
            decision = requestHandler.GetRetryDecision(new OverloadedException(null));
            Assert.AreEqual(expected, decision.DecisionType);
            decision = requestHandler.GetRetryDecision(new IsBootstrappingException(null));
            Assert.AreEqual(expected, decision.DecisionType);
            decision = requestHandler.GetRetryDecision(new TruncateException(null));
            Assert.AreEqual(expected, decision.DecisionType);
        }

        [Test]
        public void RequestHandlerRetriesTest()
        {
            //This statement will fail, then we will fake the syntax error as a ReadTimeout
            var statement = new SimpleStatement("SELECT WILL FAIL").SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var request = Session.GetRequest(statement);
            //We will need a mock to fake the responses of Cassandra
            var mock = new Moq.Mock<RequestHandler<RowSet>>(Session, request, statement);
            var requestHandler = mock.Object;
            //Expect Retry method to be called with a lower consistency level
            mock.Setup(r => r.Retry(It.Is<ConsistencyLevel?>(c => c == ConsistencyLevel.Two))).Verifiable();
            //Fake a Error Result
            requestHandler.ResponseHandler(new ReadTimeoutException(ConsistencyLevel.Three, 2, 3, false), null);

            mock.Verify();
        }
    }
}
