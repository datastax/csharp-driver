using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cassandra.Data.Linq.MSTest
{
    public partial class BasicTests
    {
        [TestMethod]
        public void Test()
        {
            Test1();
        }

        [TestMethod]
        public void TestPagination()
        {
            testPagination();
        }

        [TestMethod]
        public void TestBuffering()
        {
            testBuffering();
        }

        [TestMethod]
        public void TestBug16_JIRA()
        {
            Bug16_JIRA();
        }

    }
}
