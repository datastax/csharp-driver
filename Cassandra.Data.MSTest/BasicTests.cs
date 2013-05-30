using System;
#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

namespace Cassandra.Data.MSTest
{
    public partial class BasicTests
    {
        [TestMethod]
		[WorksForMe]
        public void ComplexTest()
        {
            complexTest();
        }
    }
}
