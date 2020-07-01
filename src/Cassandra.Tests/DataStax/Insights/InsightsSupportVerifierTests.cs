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

using System.Collections.Generic;
using System.Net;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Insights;
using Cassandra.SessionManagement;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.DataStax.Insights
{
    [TestFixture]
    public class InsightsSupportVerifierTests
    {
        [Test]
        public void Should_ReturnFalse_When_OneNode_6_0_4_AndOneNode_6_0_5()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.4", "6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode_6_0_5_AndOneNode_6_0_4()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5", "6.0.4"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode_6_1_0_AndOneNode_6_0_5()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.1.0", "6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnTrue_When_TwoNodes_6_0_5()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5", "6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnFalse_When_TwoNodes_6_0_4()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.4", "6.0.4"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode5_1_12()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("5.1.12"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode5_1_13()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("5.1.13"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode5_2_0()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("5.2.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnTrue_When_OneNode6_0_5()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode6_0_5_alpha()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5-alpha"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }
        
        [Test]
        public void Should_ReturnFalse_When_OneNode6_0_4()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.4"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnTrue_When_OneNode6_1_0()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("6.1.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode5_0_99()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("5.0.99"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode5_0_0()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("5.0.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode4_8_0()
        {
            var internalMetadata = Mock.Of<IInternalMetadata>();
            Mock.Get(internalMetadata).Setup(c => c.AllHosts()).Returns(GetHosts("4.8.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(internalMetadata));
        }

        private ICollection<Host> GetHosts(params string[] dseVersions)
        {
            var hosts = new List<Host>();
            var i = 1;
            foreach (var version in dseVersions)
            {
                var host = new Host(new IPEndPoint(IPAddress.Parse($"127.0.0.{i++}"), 9042), contactPoint: null);
                var row = Mock.Of<IRow>();
                Mock.Get(row).Setup(r => r.ContainsColumn("dse_version")).Returns(true);
                Mock.Get(row).Setup(r => r.GetValue<string>("dse_version")).Returns(version);
                host.SetInfo(row);
                hosts.Add(host);
            }

            return hosts;
        }
    }
}