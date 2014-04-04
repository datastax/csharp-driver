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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq
{
    [TestClass]
    public class FoundBug3Tests
    {
        private string KeyspaceName = "test";

        private Session Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
            Session.CreateKeyspaceIfNotExists(KeyspaceName);
            Session.ChangeKeyspace(KeyspaceName);
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        [TestMethod]
        [WorksForMe]
        //https://datastax-oss.atlassian.net/browse/CSHARP-42
        //Create table causes InvalidOperationException
        public void Bug_CSHARP_42()
        {
            Console.WriteLine("Hello World!");
            Table<SalesOrder> table = Session.GetTable<SalesOrder>();
            table.CreateIfNotExists();

            Batch batch = Session.CreateBatch();

            var order = new SalesOrder
            {
                OrderNumber = "OR00012345",
                Customer = "Jeremiah Peschka",
                SalesPerson = "The Internet",
                OrderDate = DateTime.Now,
                ShipDate = DateTime.Now.AddDays(2),
                //ShippingAddress = new Address()
                //{
                //    Line1 = "742 Evergreen Terrace",
                //    City = "Springfield",
                //    State = "Yup"
                //},
                //BillingAddress = new Address()
                //{
                //    Line1 = "1234 Fake Street",
                //    City = "Springfield",
                //    State = "Yup"
                //},
                //LineItems = new List<LineItem>()
                //        {
                //            new LineItem() { Quantity = 1, ProductNumber = "PN54321", UnitPrice = 43.43 },
                //            new LineItem() { Quantity = 12, ProductNumber = "PN12345", UnitPrice = 5.00 }
                //        }
            };

            batch.Append(table.Insert(order));
            batch.Execute();

            List<SalesOrder> lst = (from x in table select x).Execute().ToList();

            Console.WriteLine("done!");
        }
    }
}