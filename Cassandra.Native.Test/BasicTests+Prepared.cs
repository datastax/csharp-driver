using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using System.Numerics;

namespace Cassandra.Native.Test
{
    public partial class CommonBasicTests : MyUTExt.CommonBasicTests
    {
        [Fact]
        public void testMassivePrepared()
        {
            base.massivePreparedStatementTest();
        }
        //[Fact]
        public void testPreparedDecimal()
        {
            insertingSingleValue(typeof(System.Decimal), true);
        }
        //[Fact]
        public void testPreparedVarInt()
        {
            insertingSingleValue(typeof(BigInteger), true);            
        }
        [Fact]
        public void testPreparedBigInt()
        {
            base.insertingSingleValue(typeof(System.Int64), true);
        }
        [Fact]
        public void testPreparedDouble()
        {
            base.insertingSingleValue(typeof(System.Double), true);
        }
        [Fact]
        public void testPreparedFloat()
        {
            base.insertingSingleValue(typeof(System.Single), true);
        }
        [Fact]
        public void testPreparedInt()
        {
            base.insertingSingleValue(typeof(System.Int32), true);
        }

    }
}
