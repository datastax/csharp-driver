//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Net;

using Dse.Insights;
using Dse.SessionManagement;
using Moq;

using NUnit.Framework;

namespace Dse.Test.Unit.Insights
{
    [TestFixture]
    public class InsightsSupportVerifierTests
    {
        [Test]
        public void Should_ReturnFalse_When_OneNode_6_0_4_AndOneNode_6_0_5()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.4", "6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode_6_0_5_AndOneNode_6_0_4()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5", "6.0.4"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode_6_1_0_AndOneNode_6_0_5()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.1.0", "6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnTrue_When_TwoNodes_6_0_5()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5", "6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnFalse_When_TwoNodes_6_0_4()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.4", "6.0.4"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode5_1_12()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("5.1.12"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode5_1_13()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("5.1.13"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode5_2_0()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("5.2.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnTrue_When_OneNode6_0_5()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnTrue_When_OneNode6_0_5_alpha()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.5-alpha"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }
        
        [Test]
        public void Should_ReturnFalse_When_OneNode6_0_4()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.0.4"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnTrue_When_OneNode6_1_0()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("6.1.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsTrue(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode5_0_99()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("5.0.99"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode5_0_0()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("5.0.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }

        [Test]
        public void Should_ReturnFalse_When_OneNode4_8_0()
        {
            var cluster = Mock.Of<IInternalDseCluster>();
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(GetHosts("4.8.0"));

            var target = new InsightsSupportVerifier();

            Assert.IsFalse(target.SupportsInsights(cluster));
        }

        private ICollection<Host> GetHosts(params string[] dseVersions)
        {
            var hosts = new List<Host>();
            var i = 1;
            foreach (var version in dseVersions)
            {
                var host = new Host(new IPEndPoint(IPAddress.Parse($"127.0.0.{i++}"), 9042));
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