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

﻿using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    public class SessionTests : TwoNodesClusterTest
    {
        [Test]
        public void SessionCancelsPendingWhenDisposed()
        {
            Logger.Info("SessionCancelsPendingWhenDisposed");
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            try
            {
                var localSession = localCluster.Connect();
                var taskList = new List<Task>();
                for (var i = 0; i < 500; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(new SimpleStatement("SELECT * FROM system.schema_columns")));
                }
                //Most task should be pending
                Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
                //Force it to close connections
                Logger.Info("Start Disposing localSession");
                localSession.Dispose();
                //Wait for the worker threads to cancel the rest of the operations.
                Thread.Sleep(10000);
                Assert.False(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "No more task should be pending");
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion || t.Status == TaskStatus.Faulted), "All task should be completed or faulted");
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionGracefullyWaitsPendingOperations()
        {
            Logger.Info("Starting SessionGracefullyWaitsPendingOperations");
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            try
            {
                var localSession = (Session)localCluster.Connect();

                //Create more async operations that can be finished
                var taskList = new List<Task>();
                for (var i = 0; i < 1000; i++)
                {
                    taskList.Add(localSession.ExecuteAsync(new SimpleStatement("SELECT * FROM system.schema_columns")));
                }
                //Most task should be pending
                Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
                //Wait for finish
                Assert.True(localSession.WaitForAllPendingActions(60000), "All handles have received signal");

                Assert.False(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "All task should be completed (not pending)");

                if (taskList.Any(t => t.Status == TaskStatus.Faulted))
                {
                    throw taskList.First(t => t.Status == TaskStatus.Faulted).Exception;
                }
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "All task should be completed");

                localSession.Dispose();
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceDoesNotExistOnConnectThrows()
        {
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            try
            {
                var ex = Assert.Throws<InvalidQueryException>(() => localCluster.Connect("THIS_KEYSPACE_DOES_NOT_EXIST"));
                Assert.True(ex.Message.ToLower().Contains("keyspace"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceEmptyOnConnect()
        {
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            try
            {
                Assert.DoesNotThrow(() =>
                {
                    var localSession = localCluster.Connect("");
                    localSession.Execute("SELECT * FROM system.schema_keyspaces");
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceDoesNotExistOnChangeThrows()
        {
            var localCluster = Cluster.Builder().AddContactPoint(IpPrefix + "1").Build();
            try
            {
                var localSession = localCluster.Connect();
                var ex = Assert.Throws<InvalidQueryException>(() => localSession.ChangeKeyspace("THIS_KEYSPACE_DOES_NOT_EXIST_EITHER"));
                Assert.True(ex.Message.ToLower().Contains("keyspace"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceConnectCaseSensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(IpPrefix + "1")
                .Build();
            try
            {
                Assert.Throws<InvalidQueryException>(() => localCluster.Connect("SYSTEM"));
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionUseStatementChangesKeyspace()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(IpPrefix + "1")
                .Build();
            try
            {
                var localSession = localCluster.Connect();
                localSession.Execute("USE system");
                //The session should be using the system keyspace now
                Assert.DoesNotThrow(() =>
                {
                    for (var i = 0; i < 5; i++)
                    {
                        localSession.Execute("select * from schema_keyspaces");
                    }
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionUseStatementChangesKeyspaceCaseInsensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(IpPrefix + "1")
                .Build();
            try
            {
                var localSession = localCluster.Connect();
                //The statement is case insensitive by default, as no quotes were specified
                localSession.Execute("USE SyStEm");
                //The session should be using the system keyspace now
                Assert.DoesNotThrow(() =>
                {
                    for (var i = 0; i < 5; i++)
                    {
                        localSession.Execute("select * from schema_keyspaces");
                    }
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        public void SessionKeyspaceCreateCaseSensitive()
        {
            var localCluster = Cluster.Builder()
                .AddContactPoint(IpPrefix + "1")
                .Build();
            try
            {
                var localSession = localCluster.Connect();
                const string ks1 = "UPPER_ks";
                localSession.CreateKeyspace(ks1);
                localSession.ChangeKeyspace(ks1);
                localSession.Execute("CREATE TABLE test1 (k uuid PRIMARY KEY, v text)");

                //Execute multiple times a query on the newly created keyspace
                Assert.DoesNotThrow(() =>
                {
                    for (var i = 0; i < 5; i++)
                    {
                        localSession.Execute("SELECT * FROM test1");
                    }
                });
            }
            finally
            {
                localCluster.Shutdown(1000);
            }
        }

        [Test]
        [Explicit("Not implemented")]
        public void SessionFaultsTasksAfterDisposed()
        {
            throw new NotImplementedException();
        }

        [Test]
        [Explicit("Not implemented")]
        public void SessionDisposedOnCluster()
        {
            throw new NotImplementedException();
        }
    }
}
