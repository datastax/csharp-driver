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
    [TestClass]
    public partial class PreparedStatementsTests
    {        
        [TestMethod]
		[Ignore]//OK
        public void testMassivePrepared()
        {
            massivePreparedStatementTest();
        }

        [TestMethod]
		[Ignore]//OK
        public void testPreparedDecimal()
        {
            insertingSingleValuePrepared(typeof(System.Decimal));
        }

#if NET_40_OR_GREATER     
        [TestMethod] 
		[Ignore]//OK
        public void testPreparedVarInt()
        {
            insertingSingleValuePrepared(typeof(BigInteger));            
        }
#endif
        [TestMethod]
		[Ignore]//OK
        public void testPreparedBigInt()
        {
            insertingSingleValuePrepared(typeof(System.Int64));
        }

        [TestMethod]
		[Ignore]//OK
        public void testPreparedDouble()
        {
            insertingSingleValuePrepared(typeof(System.Double));
        }

        [TestMethod]
		[Ignore]//OK
        public void testPreparedFloat()
        {
            insertingSingleValuePrepared(typeof(System.Single));
        }

        [TestMethod]
		[Ignore]//OK
        public void testPreparedInt()
        {
            insertingSingleValuePrepared(typeof(System.Int32));
        }

        [TestMethod]
		[Ignore]//OK
        public void testPreparedVarchar()
        {
            insertingSingleValuePrepared(typeof(System.String));
        }

        [TestMethod]
		[Ignore]//OK
        public void testPreparedTimestamp()
        {
            insertingSingleValuePrepared(typeof(System.DateTimeOffset));
        }

        [TestMethod]
        public void testPreparedBoolean()
        {
            insertingSingleValuePrepared(typeof(System.Boolean));
        }

        [TestMethod]
        public void testPreparedBlob()
        {
            insertingSingleValuePrepared(typeof(System.Byte));
        }
        
        [TestMethod]   
		[Ignore]//OK
        public void testPreparedUUID()
        {
            insertingSingleValuePrepared(typeof(System.Guid));
        }                        
    }
}