using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Globalization;
using System.Diagnostics;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cassandra.MSTest;
#endif

namespace Cassandra.Data.Linq.MSTest
{
    [AllowFiltering]
    [Table("sales")]
    public class SalesOrder
    {
        [PartitionKey]
        [Column("order_number")]
        public string OrderNumber { get; set; }

        [ClusteringKey(1)]
        [Column("customer")]
        public string Customer { get; set; }

        [Column("sales_person")]
        public string SalesPerson { get; set; }
        [Column("order_date")]
        public DateTimeOffset OrderDate { get; set; }
        [Column("ship_date")]
        public DateTimeOffset ShipDate { get; set; }
        //[Column("line_items")]
        //public List<LineItem> LineItems = new List<LineItem>();
        //[Column("shipping_address")]
        //public Address ShippingAddress { get; set; }
        //        [Column("billing_address")]
        //        public Address BillingAddress { get; set; }
    }

    //public class LineItem
    //{
    //    public int Quantity { get; set; }
    //    public string ProductNumber { get; set; }
    //    public string ProductDescription { get; set; }
    //    public double UnitPrice { get; set; }
    //}

    //public class Address
    //{
    //    public string Line1 { get; set; }
    //    public string Line2 { get; set; }
    //    public string City { get; set; }
    //    public string State { get; set; }
    //    public string PostalCode { get; set; }
    //}


    [TestClass]
    public class FoundBug3Tests
    {
        private string KeyspaceName = "test";

        Session Session;

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
            var table = Session.GetTable<SalesOrder>();
            table.CreateIfNotExists();

            var batch = Session.CreateBatch();

            var order = new SalesOrder()
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

            var lst = (from x in table select x).Execute().ToList();

            Console.WriteLine("done!");
        }
    }
}
