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
		[WorksForMe]
        public void BigInsert()
        {
            BigInsertTest(3000);
        }

        [TestMethod]
        [WorksForMe]
        public void creatingSecondaryIndex()
        {
            createSecondaryIndexTest();
        }

        [TestMethod]
        [WorksForMe]
        public void testCounter()
        {
            testCounters();
        }

        [TestMethod]
        [WorksForMe]
        public void testBlob()
        {
            insertingSingleValue(typeof(byte));
        }

        [TestMethod]
        [WorksForMe]
        public void testASCII()
        {
            insertingSingleValue(typeof(Char));
        }

        [TestMethod]
        [WorksForMe]
        public void testDecimal()
        {
            insertingSingleValue(typeof(Decimal));
        }

#if NET_40_OR_GREATER
        [TestMethod]
        [WorksForMe]
        public void testVarInt()
        {
            insertingSingleValue(typeof(BigInteger));
        }
#endif

        [TestMethod]
        [WorksForMe]
        public void testBigInt()
        {
            insertingSingleValue(typeof(System.Int64));
        }
        [TestMethod]
        [WorksForMe]
        public void testDouble()
        {
            insertingSingleValue(typeof(System.Double));
        }
        [TestMethod]
        [WorksForMe]
        public void testFloat()
        {
            insertingSingleValue(typeof(System.Single));
        }
        [TestMethod]
        [WorksForMe]
        public void testInt()
        {
            insertingSingleValue(typeof(System.Int32));
        }
        [TestMethod]
        [WorksForMe]
        public void testBoolean()
        {
            insertingSingleValue(typeof(System.Boolean));
        }

        [TestMethod]
        [WorksForMe]
        public void testUUID()
        {
            insertingSingleValue(typeof(System.Guid));
        }

        [TestMethod]
        [WorksForMe]
        public void testTimestamp()
        {
            TimestampTest();
        }

        [TestMethod]
        [WorksForMe]
        public void MaxingBoundsOf_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int32));
        }
        [TestMethod]
		[WorksForMe]
        public void MaxingBoundsOf_BIGINT()
        {
            ExceedingCassandraType(typeof(System.Int64), typeof(System.Int64));
        }
        [TestMethod]
		[WorksForMe]
        public void MaxingBoundsOf_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Single));
        }
        [TestMethod]
		[WorksForMe]
        public void MaxingBoundsOf_DOUBLE()
        {
            ExceedingCassandraType(typeof(System.Double), typeof(System.Double));
        }

        [TestMethod]
		[WorksForMe]
        public void ExceedingCassandra_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int64), false);
        }

        [TestMethod]
		[WorksForMe]
        public void ExceedingCassandra_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Double), false);
        }

    }
}
