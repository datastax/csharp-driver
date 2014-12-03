﻿//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.FoundBugs
{
    [Category("short")]
    public class CSHARP_42 : TestGlobals
    {
        ISession _session = null;

        [SetUp]
        public void SetupFixture()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
        }

        [Test]
        //https://datastax-oss.atlassian.net/browse/CSHARP-42
        //Create table causes InvalidOperationException
        public void Bug_CSHARP_42()
        {
            Table<SalesOrder> table = _session.GetTable<SalesOrder>();
            table.CreateIfNotExists();

            Batch batch = _session.CreateBatch();

            var order = new SalesOrder
            {
                OrderNumber = "OR00012345",
                Customer = "Jeremiah Peschka",
                SalesPerson = "The Internet",
                OrderDate = DateTime.Now,
                ShipDate = DateTime.Now.AddDays(2)
            };

            batch.Append(table.Insert(order));
            batch.Execute();

            List<SalesOrder> lst = (from x in table select x).Execute().ToList();

            Trace.TraceInformation("done!");
        }
    }
}
