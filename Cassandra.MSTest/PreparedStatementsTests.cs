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
    public partial class PreparedStatementsTests
    {        
        [TestMethod]
        [WorksForMe]
        public void testMassivePrepared()
        {
            massivePreparedStatementTest();
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedDecimal()
        {
            insertingSingleValuePrepared(typeof(System.Decimal));
        }

#if NET_40_OR_GREATER     
        [TestMethod] 
		[WorksForMe]
        public void testPreparedVarInt()
        {
            insertingSingleValuePrepared(typeof(BigInteger));            
        }
#endif
        [TestMethod]
		[WorksForMe]
        public void testPreparedBigInt()
        {
            insertingSingleValuePrepared(typeof(System.Int64));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedDouble()
        {
            insertingSingleValuePrepared(typeof(System.Double));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedFloat()
        {
            insertingSingleValuePrepared(typeof(System.Single));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedInt()
        {
            insertingSingleValuePrepared(typeof(System.Int32));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedVarchar()
        {
            insertingSingleValuePrepared(typeof(System.String));
        }

        [TestMethod]
		[WorksForMe]
        public void testPreparedTimestamp()
        {
            insertingSingleValuePrepared(typeof(System.DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testPreparedBoolean()
        {
            insertingSingleValuePrepared(typeof(System.Boolean));
        }

        [TestMethod]
        [WorksForMe]
        public void testPreparedBlob()
        {
            insertingSingleValuePrepared(typeof(System.Byte));
        }
        
        [TestMethod]   
		[WorksForMe]
        public void testPreparedUUID()
        {
            insertingSingleValuePrepared(typeof(System.Guid));
        }                        
    }
}