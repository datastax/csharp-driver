using System;
#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

namespace Cassandra.MSTest
{
    public partial class AdvancedTests
    {
        [TestMethod]
		[Ignore]//OK
        public void ParallelInsert()
        {
            parallelInsertTest();
        }

        [TestMethod]
		[Ignore]//OK
        public void ErrorInjectionParallelInsert()
        {
            ErrorInjectionInParallelInsertTest();
        }

    }
}
