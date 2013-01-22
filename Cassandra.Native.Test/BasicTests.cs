using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Threading;
using System.Net;
using MyUTExt;
#if CASSANDRA_NET_40_OR_GREATER
using System.Numerics;
#endif
using System.Globalization;

namespace Cassandra.Test
{
    public partial class CommonBasicTests : MyUTExt.CommonBasicTests
    {
        public CommonBasicTests()
            : base(false)
        {
            System.Globalization.CultureInfo ci = new System.Globalization.CultureInfo("en-GB");
            System.Threading.Thread.CurrentThread.CurrentCulture = ci;
        }

        [Fact]
        [Dev.Priorioty]
        public void checkTableMetadata()
        {
            base.checkMetadata();
        }

        [Fact]
        [Dev.Priorioty]
        public void checkKeyspaceMetadata()
        {
            base.checkKSMetadata();
        }

        [Fact]
        public void testCounter()
        {
            base.testCounters();
        }

        [Fact]
        public void testBlob()
        {
            insertingSingleValue(typeof(byte));
        }

//        [Fact]
        public void creatingSecondaryIndex()
        {
            createSecondaryIndexTest();
        }

        [Fact]
        public void testASCII()
        {
            insertingSingleValue(typeof(Char));
        }
        [Fact]
        public void Test()
        {
            base.Test();
        }
        [Fact]
        public void testDecimal()
        {
            insertingSingleValue(typeof(System.Decimal));
        }
#if CASSANDRA_NET_40_OR_GREATER
        [Fact]
        public void testVarInt()
        {
            insertingSingleValue(typeof(BigInteger));
        }
#endif
        [Fact]
        public void testBigInt()
        {
            base.insertingSingleValue(typeof(System.Int64));
        }
        [Fact]
        public void testDouble()
        {
            base.insertingSingleValue(typeof(System.Double));
        }
        [Fact]
        public void testFloat()
        {
            base.insertingSingleValue(typeof(System.Single));
        }
        [Fact]
        public void testInt()
        {
            base.insertingSingleValue(typeof(System.Int32));
        }
        [Fact]
        public void testBoolean()
        {
            base.insertingSingleValue(typeof(System.Boolean));
        }

        [Fact]
        public void testUUID()
        {
            base.insertingSingleValue(typeof(System.Guid));
        }

        [Fact]
        public void MaxingBoundsOf_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int32));
        }
        [Fact]
        public void MaxingBoundsOf_BIGINT()
        {
            ExceedingCassandraType(typeof(System.Int64), typeof(System.Int64));
        }
        [Fact]
        public void MaxingBoundsOf_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Single));
        }
        [Fact]
        public void MaxingBoundsOf_DOUBLE()
        {
            ExceedingCassandraType(typeof(System.Double), typeof(System.Double));
        }

        [Fact]
        public void TimestampTest()
        {
            base.TimestampTest();
        }

        [Fact]
        public void ExceedingCassandra_INT()
        {
            ExceedingCassandraType(typeof(System.Int32), typeof(System.Int64), false);
        }
        [Fact]
        public void ExceedingCassandra_FLOAT()
        {
            ExceedingCassandraType(typeof(System.Single), typeof(System.Double), false);
        }
    }
}
