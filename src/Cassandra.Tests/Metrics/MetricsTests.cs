//
//      Copyright (C) DataStax Inc.
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

using Cassandra.Metrics;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests.Metrics
{
    [TestFixture]
    public class MetricsTests
    {
        private class TestMetric : IMetric
        {
            public string Name { get; set; }
        }

        [Test]
        public void Should_BeEqual_When_NodeMetricEqualsNodeMetric()
        {
            var nodeMetric = NodeMetric.Counters.AuthenticationErrors;
            IMetric metric = nodeMetric;
            IMetric metric2 = nodeMetric;

            Assert.IsTrue(nodeMetric.Equals(metric));
            Assert.IsTrue(metric.Equals(nodeMetric));
            Assert.IsTrue(metric.Equals(metric2));
            Assert.IsTrue(nodeMetric.Equals(NodeMetric.Counters.AuthenticationErrors));
        }

        [Test]
        public void Should_BeEqual_When_SessionMetricEqualsSessionMetric()
        {
            var sessionMetric = SessionMetric.Counters.CqlClientTimeouts;
            IMetric metric = sessionMetric;
            IMetric metric2 = sessionMetric;

            Assert.IsTrue(sessionMetric.Equals(metric));
            Assert.IsTrue(metric.Equals(sessionMetric));
            Assert.IsTrue(metric.Equals(metric2));
            Assert.IsTrue(sessionMetric.Equals(SessionMetric.Counters.CqlClientTimeouts));
        }

        [Test]
        public void Should_NotBeEqual_WhenNodeMetricEqualsSessionMetric()
        {
            var sessionMetric = new SessionMetric(NodeMetric.Counters.AuthenticationErrors.Name);
            IMetric sessionMetricBase = sessionMetric;
            IMetric sessionMetricBase2 = sessionMetric;

            var nodeMetric = NodeMetric.Counters.AuthenticationErrors;
            IMetric nodeMetricBase = nodeMetric;
            IMetric nodeMetricBase2 = nodeMetric;

            Assert.IsFalse(nodeMetric.Equals(sessionMetric));
            Assert.IsFalse(sessionMetric.Equals(nodeMetric));

            Assert.IsFalse(nodeMetric.Equals(sessionMetricBase));
            Assert.IsFalse(sessionMetricBase.Equals(nodeMetric));

            Assert.IsFalse(nodeMetric.Equals(sessionMetricBase2));
            Assert.IsFalse(sessionMetricBase2.Equals(nodeMetric));

            Assert.IsFalse(nodeMetricBase.Equals(sessionMetric));
            Assert.IsFalse(sessionMetric.Equals(nodeMetricBase));

            Assert.IsFalse(nodeMetricBase.Equals(sessionMetricBase));
            Assert.IsFalse(sessionMetricBase.Equals(nodeMetricBase));

            Assert.IsFalse(nodeMetricBase.Equals(sessionMetricBase2));
            Assert.IsFalse(sessionMetricBase2.Equals(nodeMetricBase));

            Assert.IsFalse(nodeMetricBase2.Equals(sessionMetric));
            Assert.IsFalse(sessionMetric.Equals(nodeMetricBase2));

            Assert.IsFalse(nodeMetricBase2.Equals(sessionMetricBase));
            Assert.IsFalse(sessionMetricBase.Equals(nodeMetricBase2));

            Assert.IsFalse(nodeMetricBase2.Equals(sessionMetricBase2));
            Assert.IsFalse(sessionMetricBase2.Equals(nodeMetricBase2));
        }

        [Test]
        public void Should_NotBeEqual_WhenCustomMetricEqualsSessionMetric()
        {
            var sessionMetric = new SessionMetric(NodeMetric.Counters.AuthenticationErrors.Name);
            IMetric sessionMetricBase = sessionMetric;
            IMetric sessionMetricBase2 = sessionMetric;

            var testMetric = new TestMetric { Name = NodeMetric.Counters.AuthenticationErrors.Name };
            IMetric testMetricBase = testMetric;
            IMetric testMetricBase2 = testMetric;

            Assert.IsFalse(testMetric.Equals(sessionMetric));
            Assert.IsFalse(sessionMetric.Equals(testMetric));

            Assert.IsFalse(testMetric.Equals(sessionMetricBase));
            Assert.IsFalse(sessionMetricBase.Equals(testMetric));

            Assert.IsFalse(testMetric.Equals(sessionMetricBase2));
            Assert.IsFalse(sessionMetricBase2.Equals(testMetric));

            Assert.IsFalse(testMetricBase.Equals(sessionMetric));
            Assert.IsFalse(sessionMetric.Equals(testMetricBase));

            Assert.IsFalse(testMetricBase.Equals(sessionMetricBase));
            Assert.IsFalse(sessionMetricBase.Equals(testMetricBase));

            Assert.IsFalse(testMetricBase.Equals(sessionMetricBase2));
            Assert.IsFalse(sessionMetricBase2.Equals(testMetricBase));

            Assert.IsFalse(testMetricBase2.Equals(sessionMetric));
            Assert.IsFalse(sessionMetric.Equals(testMetricBase2));

            Assert.IsFalse(testMetricBase2.Equals(sessionMetricBase));
            Assert.IsFalse(sessionMetricBase.Equals(testMetricBase2));

            Assert.IsFalse(testMetricBase2.Equals(sessionMetricBase2));
            Assert.IsFalse(sessionMetricBase2.Equals(testMetricBase2));
        }

        [Test]
        public void Should_NotBeEqual_WhenCustomMetricEqualsNodeMetric()
        {
            var nodeMetric = new NodeMetric(NodeMetric.Counters.AuthenticationErrors.Name);
            IMetric nodeMetricBase = nodeMetric;
            IMetric nodeMetricBase2 = nodeMetric;

            var testMetric = new TestMetric { Name = NodeMetric.Counters.AuthenticationErrors.Name };
            IMetric testMetricBase = testMetric;
            IMetric testMetricBase2 = testMetric;

            Assert.IsFalse(testMetric.Equals(nodeMetric));
            Assert.IsFalse(nodeMetric.Equals(testMetric));

            Assert.IsFalse(testMetric.Equals(nodeMetricBase));
            Assert.IsFalse(nodeMetricBase.Equals(testMetric));

            Assert.IsFalse(testMetric.Equals(nodeMetricBase2));
            Assert.IsFalse(nodeMetricBase2.Equals(testMetric));

            Assert.IsFalse(testMetricBase.Equals(nodeMetric));
            Assert.IsFalse(nodeMetric.Equals(testMetricBase));

            Assert.IsFalse(testMetricBase.Equals(nodeMetricBase));
            Assert.IsFalse(nodeMetricBase.Equals(testMetricBase));

            Assert.IsFalse(testMetricBase.Equals(nodeMetricBase2));
            Assert.IsFalse(nodeMetricBase2.Equals(testMetricBase));

            Assert.IsFalse(testMetricBase2.Equals(nodeMetric));
            Assert.IsFalse(nodeMetric.Equals(testMetricBase2));

            Assert.IsFalse(testMetricBase2.Equals(nodeMetricBase));
            Assert.IsFalse(nodeMetricBase.Equals(testMetricBase2));

            Assert.IsFalse(testMetricBase2.Equals(nodeMetricBase2));
            Assert.IsFalse(nodeMetricBase2.Equals(testMetricBase2));
        }
    }
}