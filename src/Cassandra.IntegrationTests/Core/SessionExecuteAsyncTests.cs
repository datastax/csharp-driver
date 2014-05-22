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
using System.Globalization;
using System.Threading;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class SessionExecuteAsyncTests : SingleNodeClusterTest
    {
        [Test]
        public void SessionExecuteAsyncCQLQueryToSync()
        {
            var task = Session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.schema_keyspaces"));
            //forcing it to execute sync for testing purposes
            var rowset = task.Result;
            Assert.True(rowset.Count() > 0, "Returned result set of keyspaces should be greater than zero.");
        }

        [Test]
        public void SessionExecuteAsyncPreparedToSync()
        {
            var statement = Session.Prepare("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = ?");
            var task = Session.ExecuteAsync(statement.Bind("system"));
            //forcing it to execute sync for testing purposes
            var rowset = task.Result;
            Assert.True(rowset.Count() > 0, "Returned result set of keyspaces should be greater than zero.");
        }

        [Test]
        public void SessionExecuteAsyncSyntaxErrorQuery()
        {
            //Execute an invalid query 
            var task = Session.ExecuteAsync(new SimpleStatement("SELECT WILL FAIL"));
            task.ContinueWith(t =>
            {
                Assert.NotNull(t.Exception);
            }, TaskContinuationOptions.OnlyOnFaulted);

            task.ContinueWith(t =>
            {
                Assert.Fail("It should not continued");
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            Exception exThrown = null;
            try
            {
                task.Wait();
            }
            catch (Exception ex)
            {
                exThrown = ex;
            }
            Assert.NotNull(exThrown);
            Assert.IsInstanceOf<AggregateException>(exThrown);
            Assert.IsInstanceOf<SyntaxError>(((AggregateException)exThrown).InnerExceptions.First());
        }

        [Test]
        public void SessionExecuteAsyncCQLQueriesParallel()
        {
            var task1 = Session.ExecuteAsync(new SimpleStatement("select keyspace_name FROM system.schema_keyspaces"));
            var task2 = Session.ExecuteAsync(new SimpleStatement("select cluster_name from system.local"));
            var task3 = Session.ExecuteAsync(new SimpleStatement("select column_name from system.schema_columns"));
            //forcing the calling thread to wait for all the parallel task to finish
            Task.WaitAll(new[] { task1, task2, task3 });
            Assert.True(task1.Result.First().GetValue<string>("keyspace_name") != null, "Returned result set of keyspaces should be greater than zero.");
            Assert.True(task2.Result.First().GetValue<string>("cluster_name") != null, "Returned result set of local cluster table should be greater than zero.");
            Assert.True(task3.Result.First().GetValue<string>("column_name") != null, "Returned result set of columns should be greater than zero.");
        }
    }
}
