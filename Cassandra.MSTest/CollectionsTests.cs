using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cassandra.MSTest
{
    [TestClass]
    public partial class Collections
    {
        [TestMethod]
        public void testListOrderPrepending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "prepending");
        }

        [TestMethod]
        public void testListOrderAppending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "appending");
        }

        [TestMethod]
        public void testSetOrder()
        {
            checkingOrderOfCollection("set", typeof(System.Int32));
        }

        [TestMethod]
        public void testMap()
        {
            insertingSingleCollection("map", typeof(string), typeof(DateTimeOffset));
        }

        [TestMethod]
        public void testMapDouble()
        {
            insertingSingleCollection("map", typeof(Double), typeof(DateTimeOffset));
        }

        [TestMethod]
        public void testMapInt32()
        {
            insertingSingleCollection("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [TestMethod]
        public void testMapInt64()
        {
            insertingSingleCollection("map", typeof(Int64), typeof(DateTimeOffset));
        }

        [TestMethod]
        public void testListDouble()
        {
            insertingSingleCollection("list", typeof(Double));
        }

        [TestMethod]
        public void testListInt64()
        {
            insertingSingleCollection("list", typeof(Int64));
        }

        [TestMethod]
        public void testListInt32()
        {
            insertingSingleCollection("list", typeof(Int32));
        }

        [TestMethod]
        public void testListString()
        {
            insertingSingleCollection("list", typeof(string));
        }

        [TestMethod]
        public void testSetString()
        {
            insertingSingleCollection("set", typeof(string));
        }

        [TestMethod]
        public void testSetDouble()
        {
            insertingSingleCollection("set", typeof(Double));
        }

        [TestMethod]
        public void testSetInt32()
        {
            insertingSingleCollection("set", typeof(Int32));
        }

        [TestMethod]
        public void testSetInt64()
        {
            insertingSingleCollection("set", typeof(Int64));
        }
 
    }
}
