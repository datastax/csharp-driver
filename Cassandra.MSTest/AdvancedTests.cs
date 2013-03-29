using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
