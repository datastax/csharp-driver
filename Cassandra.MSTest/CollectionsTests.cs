using System;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

namespace Cassandra.MSTest
{
    [TestClass]
    public partial class CollectionsTests
    {
        [TestMethod]
		[Ignore]//OK
        public void testListOrderPrepending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "prepending");
        }

        [TestMethod]
		[Ignore]//OK
        public void testListOrderAppending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "appending");
        }

        [TestMethod]
		[Ignore]//OK
        public void testSetOrder()
        {
            checkingOrderOfCollection("set", typeof(System.Int32));
        }

        [TestMethod]
		[Ignore]//OK
        public void testMap()
        {
            insertingSingleCollection("map", typeof(string), typeof(DateTimeOffset));
        }

        [TestMethod]
		[Ignore]//OK
        public void testMapDouble()
        {
            insertingSingleCollection("map", typeof(Double), typeof(DateTimeOffset));
        }

        [TestMethod]
		[Ignore]//OK
        public void testMapInt32()
        {
            insertingSingleCollection("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [TestMethod]
		[Ignore]//OK
        public void testMapInt64()
        {
            insertingSingleCollection("map", typeof(Int64), typeof(DateTimeOffset));
        }

        [TestMethod]
		[Ignore]//OK
        public void testListDouble()
        {
            insertingSingleCollection("list", typeof(Double));
        }

        [TestMethod]
		[Ignore]//OK
        public void testListInt64()
        {
            insertingSingleCollection("list", typeof(Int64));
        }

        [TestMethod]
		[Ignore]//OK
        public void testListInt32()
        {
            insertingSingleCollection("list", typeof(Int32));
        }

        [TestMethod]
		[Ignore]//OK
        public void testListString()
        {
            insertingSingleCollection("list", typeof(string));
        }

        [TestMethod]
		[Ignore]//OK
        public void testSetString()
        {
            insertingSingleCollection("set", typeof(string));
        }

        [TestMethod]
		[Ignore]//OK
        public void testSetDouble()
        {
            insertingSingleCollection("set", typeof(Double));
        }

        [TestMethod]
		[Ignore]//OK
        public void testSetInt32()
        {
            insertingSingleCollection("set", typeof(Int32));
        }

        [TestMethod]
		[Ignore]//OK
        public void testSetInt64()
        {
            insertingSingleCollection("set", typeof(Int64));
        }
 
    }
}
