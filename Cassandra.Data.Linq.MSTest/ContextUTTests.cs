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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Globalization;
using System.Diagnostics;

#if MYTEST
using MyTest;
using System.IO;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cassandra.MSTest;
#endif

namespace Cassandra.Data.Linq.MSTest
{
    [TestClass]
    public class ContextUTTests
    {

        [AllowFiltering]
        [Table("x_t")]
        public class TestTable
        {
            [PartitionKey]
            [Column("x_pk")]
            public string pk { get; set; }

            [ClusteringKey(1)]
            [Column("x_ck1")]
            public int? ck1 { get; set; }

            [ClusteringKey(2)]
            [Column("x_ck2")]
            public int ck2 { get; set; }

            [Column("x_f1")]
            public int? f1 { get; set; }
        }

        private string ContextLine(Context context, int line)
        {
            var sr = new StringReader(context.ToString());
            for (int i = 0; i < line; i++)
                sr.ReadLine();
            return sr.ReadLine().Split(';').First();
        }

        [TestMethod]
        [WorksForMe]
        public void TestCqlFromContext()
        {
            var context = new Context(null);
            context.AddTable<TestTable>();
            var table = context.GetTable<TestTable>(null);

            table.AddNew(new TestTable() { ck1 = 1, ck2 = 2, f1 = 3, pk = "x" });
            Assert.Equal(ContextLine(context, 1), @"INSERT INTO ""x_t""(""x_pk"", ""x_ck1"", ""x_ck2"", ""x_f1"") VALUES ('x', 1, 2, 3)");

            var e = new TestTable() { ck1 = 3, ck2 = 4, f1 = 5, pk = "y" };
            table.Attach(e);

            e.f1 = null;
            Assert.Equal(ContextLine(context, 2), @"UPDATE ""x_t"" SET ""x_f1"" = NULL  WHERE ""x_pk"" = 'y'  AND ""x_ck1"" = 3  AND ""x_ck2"" = 4 ");

            e.f1 = 10;
            Assert.Equal(ContextLine(context, 2), @"UPDATE ""x_t"" SET ""x_f1"" = 10  WHERE ""x_pk"" = 'y'  AND ""x_ck1"" = 3  AND ""x_ck2"" = 4 ");
        }

    }
}
