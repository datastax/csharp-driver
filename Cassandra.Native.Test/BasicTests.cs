using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Threading;
using System.Net;
using MyUTExt;
using System.Numerics;
using System.Globalization;

namespace Cassandra.Native.Test
{        
    public class NativeBasicTests : MyUTExt.CommonBasicTests
    {
        public NativeBasicTests() : base(false,true)
        {            
        }

        [Fact]
        public void testDecimal()
        {
            inputingSingleValue(typeof(System.Decimal));
        }

        [Fact]
        public void testASCII()
        {
            inputingSingleValue(typeof(Char));
        }

        [Fact]
        public void testVarInt()
        {
            inputingSingleValue(typeof(BigInteger));
        }

        [Fact]
        public void Test()
        {
            base.Test();
        }

        [Fact]
        public void testBigInt()
        {
            base.inputingSingleValue(typeof(System.Int64));
        }

        [Fact]
        public void testDouble()
        {
            base.inputingSingleValue(typeof(System.Double));
        }
        [Fact]
        public void testFloat()
        {
            base.inputingSingleValue(typeof(System.Single));
        }
        [Fact]
        public void testInt()
        {
            base.inputingSingleValue(typeof(System.Int32));
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
