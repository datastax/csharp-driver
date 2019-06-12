//
//      Copyright (C) DataStax Inc.
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
﻿using Cassandra.IntegrationTests.TestBase;
﻿using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class SessionExecuteAsyncTests : SharedClusterTest
    {
        [Test]
        public void SessionExecuteAsyncCQLQueryToSync()
        {
            var task = Session.ExecuteAsync(new SimpleStatement("SELECT * FROM system.local"));
            //forcing it to execute sync for testing purposes
            var rowset = task.Result;
            Assert.True(rowset.Any(), "Returned result should contain rows.");
        }

        [Test]
        public void SessionExecuteAsyncPreparedToSync()
        {
            var statement = Session.Prepare("SELECT * FROM system.local WHERE key= ?");
            var task = Session.ExecuteAsync(statement.Bind("local"));
            //forcing it to execute sync for testing purposes
            var rowset = task.Result;
            Assert.True(rowset.Any(), "Returned result should contain rows.");
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
                Assert.Fail("Task should not have continued");
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
            var task1 = Session.ExecuteAsync(new SimpleStatement("select * FROM system.local"));
            var task2 = Session.ExecuteAsync(new SimpleStatement("select key from system.local"));
            var task3 = Session.ExecuteAsync(new SimpleStatement("select tokens from system.local"));
            //forcing the calling thread to wait for all the parallel task to finish
            Task.WaitAll(task1, task2, task3);
            Assert.NotNull(task1.Result.First().GetValue<string>("key"));
            Assert.NotNull(task2.Result.First().GetValue<string>("key"));
            Assert.NotNull(task3.Result.First().GetValue<string[]>("tokens"));
        }
    }
}
