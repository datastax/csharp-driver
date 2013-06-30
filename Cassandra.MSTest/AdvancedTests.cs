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
		[WorksForMe]
        public void ParallelInsert()
        {
            parallelInsertTest();
        }

        [TestMethod]
        [WorksForMe]
        public void ErrorInjectionParallelInsert()
        {
            ErrorInjectionInParallelInsertTest();
        }

        [TestMethod]
        [WorksForMe]
        public void MassiveAsync()
        {
            MassiveAsyncTest();
        }

    }
}
