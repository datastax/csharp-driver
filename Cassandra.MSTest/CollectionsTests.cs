//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
﻿using System;

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
		[WorksForMe]
        public void testListOrderPrepending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "prepending");
        }

        [TestMethod]
		[WorksForMe]
        public void testListOrderAppending()
        {
            checkingOrderOfCollection("list", typeof(System.Int32), null, "appending");
        }

        [TestMethod]
		[WorksForMe]
        public void testSetOrder()
        {
            checkingOrderOfCollection("set", typeof(System.Int32));
        }

        [TestMethod]
		[WorksForMe]
        public void testMap()
        {
            insertingSingleCollection("map", typeof(string), typeof(DateTimeOffset));
        }

        [TestMethod]
		[WorksForMe]
        public void testMapDouble()
        {
            insertingSingleCollection("map", typeof(Double), typeof(DateTimeOffset));
        }

        [TestMethod]
		[WorksForMe]
        public void testMapInt32()
        {
            insertingSingleCollection("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [TestMethod]
		[WorksForMe]
        public void testMapInt64()
        {
            insertingSingleCollection("map", typeof(Int64), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testListDouble()
        {
            insertingSingleCollection("list", typeof(Double));
        }

        [TestMethod]
		[WorksForMe]
        public void testListInt64()
        {
            insertingSingleCollection("list", typeof(Int64));
        }

        [TestMethod]
		[WorksForMe]
        public void testListInt32()
        {
            insertingSingleCollection("list", typeof(Int32));
        }

        [TestMethod]
		[WorksForMe]
        public void testListString()
        {
            insertingSingleCollection("list", typeof(string));
        }

        [TestMethod]
		[WorksForMe]
        public void testSetString()
        {
            insertingSingleCollection("set", typeof(string));
        }

        [TestMethod]
		[WorksForMe]
        public void testSetDouble()
        {
            insertingSingleCollection("set", typeof(Double));
        }

        [TestMethod]
		[WorksForMe]
        public void testSetInt32()
        {
            insertingSingleCollection("set", typeof(Int32));
        }

        [TestMethod]
		[WorksForMe]
        public void testSetInt64()
        {
            insertingSingleCollection("set", typeof(Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapPrepared()
        {
            insertingSingleCollectionPrepared("map", typeof(string), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapDoublePrepared()
        {
            insertingSingleCollectionPrepared("map", typeof(Double), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapInt32Prepared()
        {
            insertingSingleCollectionPrepared("map", typeof(Int32), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testMapInt64Prepared()
        {
            insertingSingleCollectionPrepared("map", typeof(Int64), typeof(DateTimeOffset));
        }

        [TestMethod]
        [WorksForMe]
        public void testListDoublePrepared()
        {
            insertingSingleCollectionPrepared("list", typeof(Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testListInt64Prepared()
        {
            insertingSingleCollectionPrepared("list", typeof(Int64));
        }

        [TestMethod]
        [WorksForMe]
        public void testListInt32Prepared()
        {
            insertingSingleCollectionPrepared("list", typeof(Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testListStringPrepared()
        {
            insertingSingleCollectionPrepared("list", typeof(string));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetStringPrepared()
        {
            insertingSingleCollectionPrepared("set", typeof(string));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetDoublePrepared()
        {
            insertingSingleCollectionPrepared("set", typeof(Double));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetInt32Prepared()
        {
            insertingSingleCollectionPrepared("set", typeof(Int32));
        }

        [TestMethod]
        [WorksForMe]
        public void testSetInt64Prepared()
        {
            insertingSingleCollectionPrepared("set", typeof(Int64));
        }
 
    }
}
