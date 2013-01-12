using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Cassandra.Test
{
    public partial class CommonBasicTests : MyUTExt.CommonBasicTests
    {
        [Fact]
        public void testListOrderPrepending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "prepending");
        }

        [Fact]
        public void testListOrderAppending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "appending");
        }

        [Fact]
        public void testSetOrder()
        {
            checkingOrderOfCollection("set", typeof(System.Int32));
        }

        [Fact]
        public void testMap()
        {
            insertingSingleCollection("map", typeof(string), typeof(DateTimeOffset));
        }

        [Fact]
        public void testMapDouble()
        {
            insertingSingleCollection("map", typeof(Double), typeof(DateTimeOffset));
        }

        [Fact]
        public void testMapInt32()
        {
            insertingSingleCollection("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [Fact]
        public void testMapInt64()
        {
            insertingSingleCollection("map", typeof(Int64), typeof(DateTimeOffset));
        }
        [Fact]
        public void testListDouble()
        {
            insertingSingleCollection("list", typeof(Double));
        }

        [Fact]
        public void testListInt64()
        {
            insertingSingleCollection("list", typeof(Int64));
        }

        [Fact]
        public void testListInt32()
        {
            insertingSingleCollection("list", typeof(Int32));
        }

        [Fact]
        public void testListString()
        {
            insertingSingleCollection("list", typeof(string));
        }

        [Fact]
        public void testSetString()
        {
            insertingSingleCollection("set", typeof(string));
        }

        [Fact]
        public void testSetDouble()
        {
            insertingSingleCollection("set", typeof(Double));
        }

        [Fact]
        public void testSetInt32()
        {
            insertingSingleCollection("set", typeof(Int32));
        }
        [Fact]
        public void testSetInt64()
        {
            insertingSingleCollection("set", typeof(Int64));
        }
    }
}