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
using System.Threading.Tasks;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif

namespace Cassandra.MSTest
{
    [TestClass]
    public class SessionExecuteAsyncTests
    {
        Session Session;

        [TestInitialize]
        public void SetFixture()
        {
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
        }

        [TestMethod]
        [TestCategory("short")]
        public void SessionExecuteAsyncCQLQueryToSync()
        {
            var task = Session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.schema_keyspaces"));
            //forcing it to execute sync for testing purposes
            var rowset = task.Result;
            Assert.True(rowset.GetRows().Count() > 0, "Returned result set of keyspaces should be greater than zero.");
        }

        [TestMethod]
        [TestCategory("short")]
        public void SessionExecuteAsyncPreparedToSync()
        {
            var statement = Session.Prepare("SELECT * FROM system.schema_keyspaces");
            var task = Session.ExecuteAsync(statement.Bind());
            //forcing it to execute sync for testing purposes
            var rowset = task.Result;
            Assert.True(rowset.GetRows().Count() > 0, "Returned result set of keyspaces should be greater than zero.");
        }

        [TestMethod]
        [TestCategory("short")]
        public void SessionExecuteAsyncCQLQueriesParallel()
        {
            var task1 = Session.ExecuteAsync(new SimpleStatement("select keyspace_name FROM system.schema_keyspaces"));
            var task2 = Session.ExecuteAsync(new SimpleStatement("select cluster_name from system.local"));
            var task3 = Session.ExecuteAsync(new SimpleStatement("select column_name from system.schema_columns"));
            //forcing the calling thread to wait for all the parallel task to finish
            Task.WaitAll(new[] { task1, task2, task3 });
            Assert.True(task1.Result.GetRows().First().GetValue<string>("keyspace_name") != null, "Returned result set of keyspaces should be greater than zero.");
            Assert.True(task2.Result.GetRows().First().GetValue<string>("cluster_name") != null, "Returned result set of local cluster table should be greater than zero.");
            Assert.True(task3.Result.GetRows().First().GetValue<string>("column_name") != null, "Returned result set of columns should be greater than zero.");
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }
    }
}
