//
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
using Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Linq
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
        public DateTime OrderDate { get; set; }

        [Column("ship_date")]
        public DateTime ShipDate { get; set; }

        //[Column("line_items")]
        //public List<LineItem> LineItems = new List<LineItem>();
        //[Column("shipping_address")]
        //public Address ShippingAddress { get; set; }
        //        [Column("billing_address")]
        //        public Address BillingAddress { get; set; }
    }
}