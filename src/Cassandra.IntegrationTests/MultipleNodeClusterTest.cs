using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using System.Reflection;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    /// Represents a set of tests that reuse an test cluster of n node
    /// </summary>
    [TestFixture]
    public abstract class MultipleNodesClusterTest
    {
        /// <summary>
        /// Gets or sets the Cluster builder for this test
        /// </summary>
        protected virtual Builder Builder { get; set; }

        protected virtual CcmClusterInfo CcmClusterInfo { get; set; }

        protected virtual Cluster Cluster { get; set; }

        protected virtual int NodeLength { get; set; }

        protected virtual ISession Session { get; set; }

        /// <summary>
        /// Gets the Ip prefix for ccm tests
        /// </summary>
        protected virtual string IpPrefix
        {
            get
            {
                return "127.0.0.";
            }
        }

        private MultipleNodesClusterTest()
        {

        }

        /// <summary>
        /// Creates a new instance of MultipleNodeCluster Test
        /// </summary>
        /// <param name="nodeLength">Determines the amount of nodes in the test cluster</param>
        public MultipleNodesClusterTest(int nodeLength)
        {
            this.NodeLength = nodeLength;
        }

        [TestFixtureSetUp]
        public virtual void TestFixtureSetUp()
        {
            if (this.NodeLength == 0)
            {
                return;
            }
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            this.CcmClusterInfo = TestUtils.CcmSetup(NodeLength, Builder, "tester");
            this.Cluster = this.CcmClusterInfo.Cluster;
            this.Session = this.CcmClusterInfo.Session;
        }

        [TestFixtureTearDown]
        public virtual void TestFixtureTearDown()
        {
            if (this.NodeLength == 0)
            {
                return;
            }
            try
            {
                //Try to close the connections
                Session.Dispose();
            }
            catch
            {

            }
            TestUtils.CcmRemove(this.CcmClusterInfo);
        }

        [SetUp]
        public virtual void SetUp()
        {
            var test = TestContext.CurrentContext.Test;
            var methodFullName = TestContext.CurrentContext.Test.FullName;
            var typeName = methodFullName.Substring(0, methodFullName.Length - test.Name.Length - 1);
            var type = Assembly.GetExecutingAssembly().GetType(typeName);
            if (type == null)
            {
                return;
            }
            var methodAttr = type.GetMethod(test.Name)
                .GetCustomAttributes(true)
                .Select(a => (Attribute) a)
                .FirstOrDefault((a) => a is TestCassandraVersion);
            var attr = Attribute.GetCustomAttributes(type).FirstOrDefault((a) => a is TestCassandraVersion);
            if (attr == null && methodAttr == null)
            {
                //It does not contain the attribute, move on.
                return;
            }
            if (methodAttr != null)
            {
                attr = methodAttr;
            }
            var minimalVersion = (TestCassandraVersion)attr;
            var currentVersion = Options.Default.CassandraVersion;
            //If we are running previous version 
            if ((minimalVersion.Major > currentVersion.Major) ||
                (minimalVersion.Major == currentVersion.Major && minimalVersion.Minor > currentVersion.Minor)
                )
            {
                Assert.Ignore(String.Format("Test Ignored: Test suitable to be run against Cassandra {0}.{1} or above.", minimalVersion.Major, minimalVersion.Minor));
            }
        }
    }
}
