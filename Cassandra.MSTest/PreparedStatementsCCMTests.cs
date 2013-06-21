using System;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

#if NET_40_OR_GREATER
using System.Numerics;
#endif

namespace Cassandra.MSTest
{
    public partial class PreparedStatementsCCMTests
    {        
                        
        [TestMethod]        
		[WorksForMe]
        public void reprepareOnNewlyUpNodeTestCCM()
        {
            reprepareOnNewlyUpNodeTest(true);
        }
        
        [TestMethod]
 		[WorksForMe]
       public void reprepareOnNewlyUpNodeNoKeyspaceTestCCM()
        {
            // This is the same test than reprepareOnNewlyUpNodeTest, except that the
            // prepared statement is prepared while no current keyspace is used
            reprepareOnNewlyUpNodeTest(false);
        }

    }
}