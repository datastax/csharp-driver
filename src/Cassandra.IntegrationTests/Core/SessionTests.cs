using NUnit.Framework;
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
