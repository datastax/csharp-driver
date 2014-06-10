using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class SessionTests : TwoNodesClusterTest
    {
        [Test]
        public void SessionGracefullyWaitsPendingOperations()
        {
            var localSession = (Session)Cluster.Connect();

            //Create more async operations that can be finished
            var taskList = new List<Task>();
            for (var i = 0; i < 500; i++)
            {
                taskList.Add(localSession.ExecuteAsync(new SimpleStatement("SELECT * FROM system.schema_columns")));
            }
            //Most task should be pending
            Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
            //Wait for finish
            Assert.True(localSession.WaitForAllPendingActions(60000), "All handles have received signal");
            Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "All task should be completed (not pending)");
            Assert.True(taskList.Any(t => t.Status == TaskStatus.Faulted), "All task should be completed (not faulted)");
            Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "All task should be completed");
            localSession.Dispose();
        }

        [Test]
        public void SessionCancelsPendingWhenDisposed()
        {
            var localSession = (Session)Cluster.Connect();
            var taskList = new List<Task>();
            for (var i = 0; i < 500; i++)
            {
                taskList.Add(localSession.ExecuteAsync(new SimpleStatement("SELECT * FROM system.schema_columns")));
            }
            //Most task should be pending
            Assert.True(taskList.Any(t => t.Status == TaskStatus.WaitingForActivation), "Most task should be pending");
            //Force it to close connections
            localSession.Dispose();
            Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion || t.Status == TaskStatus.Faulted), "All task should be completed or faulted");
        }

        [Test]
        public void SessionFaultsTasksAfterDisposed()
        {

        }
    }
}
