using System;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

namespace Cassandra.Data.Linq.MSTest
{
    public partial class BasicLinqTests
    {
        [TestMethod]
        [Ignore]
        public void Test()
        {
            Test1();
        }

        [TestMethod]
        [Ignore]
        public void TestPagination()
        {
            testPagination();
        }

        [TestMethod]
        [Ignore]
        public void TestBuffering()
        {
            testBuffering();
        }

        [TestMethod]
        [Ignore]
        public void TestBug16_JIRA()
        {
            Bug16_JIRA();
        }

    }
}
