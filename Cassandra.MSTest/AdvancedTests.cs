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
        public void ParallelInsert()
        {
            parallelInsertTest();
        }

        [TestMethod]
        public void ErrorInjectionParallelInsert()
        {
            ErrorInjectionInParallelInsertTest();
        }

    }
}
