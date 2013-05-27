using System;
using System.Collections.Generic;

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
    public partial class BasicTests
    {
        [TestMethod]        
		[Ignore]//OK
        public void BigInsert()
        {
            BigInsertTest(3000);
        }

        [TestMethod]        
		[Ignore]//OK
        public void creatingSecondaryIndex()
        {
            createSecondaryIndexTest();
        }

        [TestMethod]
        [Ignore]//OK
        public void testCounter()
        {
            testCounters();
        }

        [TestMethod]
        [Ignore]//OK
        public void testBlob()
        {
            insertingSingleValue(typeof(byte));
        }

        [TestMethod]
        [Ignore]//OK
        public void testASCII()
        {
            insertingSingleValue(typeof(Char));
        }

        [TestMethod]
        public void testDecimal()
        {
            insertingSingleValue(typeof(Decimal));
        }
#if NET_40_OR_GREATER
        [TestMethod]
        public void testVarInt()
        {
            insertingSingleValue(typeof(BigInteger));
        }
#endif

        [TestMethod]
        [Ignore]//OK
        public void testBigInt()
        {
            insertingSingleValue(typeof(System.Int64));
        }
        [TestMethod]
        [Ignore]//OK
        public void testDouble()
        {
            insertingSingleValue(typeof(System.Double));
        }
        [TestMethod]
        [Ignore]//OK
        public void testFloat()
        {
            insertingSingleValue(typeof(System.Single));
        }
        [TestMethod]
        [Ignore]//OK
        public void testInt()
        {
            insertingSingleValue(typeof(System.Int32));
        }
        [TestMethod]
        [Ignore]//OK
        public void testBoolean()
        {
            insertingSingleValue(typeof(System.Boolean));
        }

        [TestMethod]
        [Ignore]//OK
        public void testUUID()
        {
            insertingSingleValue(typeof(System.Guid));
        }

        [TestMethod]
        [Ignore]//OK
        public void testTimestamp()
        {
            TimestampTest();
        }

        [TestMethod]
        [Ignore]//OK
        public void MaxingBoundsOf_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int32));
        }
        [TestMethod]
		[Ignore]//OK
        public void MaxingBoundsOf_BIGINT()
        {
            ExceedingCassandraType(typeof(System.Int64), typeof(System.Int64));
        }
        [TestMethod]
		[Ignore]//OK
        public void MaxingBoundsOf_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Single));
        }
        [TestMethod]
		[Ignore]//OK
        public void MaxingBoundsOf_DOUBLE()
        {
            ExceedingCassandraType(typeof(System.Double), typeof(System.Double));
        }

        [TestMethod]
		[Ignore]//OK
        public void ExceedingCassandra_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int64), false);
        }

        [TestMethod]
		[Ignore]//OK
        public void ExceedingCassandra_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Double), false);
        }

    }
}
