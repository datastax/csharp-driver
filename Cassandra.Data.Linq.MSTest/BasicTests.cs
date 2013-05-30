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
        [WorksForMe]
        public void Test()
        {
            Test1();
        }

        [TestMethod]
        [WorksForMe]
        public void TestPagination()
        {
            testPagination();
        }

        [TestMethod]
        [WorksForMe]
        public void TestBuffering()
        {
            testBuffering();
        }

        [TestMethod]
        [WorksForMe]
        public void TestBug16_JIRA()
        {
            Bug16_JIRA();
        }

    }
}
