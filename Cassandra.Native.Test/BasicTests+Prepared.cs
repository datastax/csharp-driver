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
        //[Fact]  // generates OutOfMemory exception in cassandra
        public void testPreparedDecimal()
        {
            insertingSingleValuePrepared(typeof(System.Decimal));
        }
        //[Fact] // generates OutOfMemory exception in cassandra
        public void testPreparedVarInt()
        {
            insertingSingleValuePrepared(typeof(BigInteger));            
        }

        [Fact]
        public void testPreparedBigInt()
        {
            base.insertingSingleValuePrepared(typeof(System.Int64));
        }

        [Fact]
        public void testPreparedDouble()
        {
            base.insertingSingleValuePrepared(typeof(System.Double));
        }

        [Fact]
        public void testPreparedFloat()
        {
            base.insertingSingleValuePrepared(typeof(System.Single));
        }

        [Fact]
        public void testPreparedInt()
        {
            base.insertingSingleValuePrepared(typeof(System.Int32));
        }

        [Fact]
        public void testPreparedVarchar()
        {
            base.insertingSingleValuePrepared(typeof(System.String));
        }
        
        //[Fact] //it works fine, but server returns date in another format
        public void testPreparedTimestamp()
        {
            base.insertingSingleValuePrepared(typeof(System.DateTimeOffset));
        }

        [Fact]
        public void testPreparedBoolean()
        {
            base.insertingSingleValuePrepared(typeof(System.Boolean));
        }

        [Fact]
        public void testPreparedBlob()
        {
            base.insertingSingleValuePrepared(typeof(System.Byte));
        }

        [Fact]
        public void testPreparedUUID()
        {
            base.insertingSingleValuePrepared(typeof(System.Guid));
        }
    }
}
