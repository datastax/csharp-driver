using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
#if CASSANDRA_NET_40_OR_GREATER
using System.Numerics;
#endif

namespace Cassandra.MSTest
{
    [TestClass]
    public partial class PreparedStatements
    {
        [TestMethod]
        public void testMassivePrepared()
        {
            massivePreparedStatementTest();
        }

        [TestMethod]
        public void testPreparedDecimal()
        {
            insertingSingleValuePrepared(typeof(System.Decimal));
        }

#if CASSANDRA_NET_40_OR_GREATER     
        [TestMethod] 
        public void testPreparedVarInt()
        {
            insertingSingleValuePrepared(typeof(BigInteger));            
        }
#endif
        [TestMethod]
        public void testPreparedBigInt()
        {
            insertingSingleValuePrepared(typeof(System.Int64));
        }

        [TestMethod]
        public void testPreparedDouble()
        {
            insertingSingleValuePrepared(typeof(System.Double));
        }

        [TestMethod]
        public void testPreparedFloat()
        {
            insertingSingleValuePrepared(typeof(System.Single));
        }

        [TestMethod]
        public void testPreparedInt()
        {
            insertingSingleValuePrepared(typeof(System.Int32));
        }

        [TestMethod]
        public void testPreparedVarchar()
        {
            insertingSingleValuePrepared(typeof(System.String));
        }

        [TestMethod]
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
        public void testPreparedUUID()
        {
            insertingSingleValuePrepared(typeof(System.Guid));
        }

    }
}