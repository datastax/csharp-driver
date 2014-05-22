using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests
{
    public class ThreadPerTaskScheduler : TaskScheduler
    {
        protected override IEnumerable<Task> GetScheduledTasks() { return Enumerable.Empty<Task>(); }

        protected override void QueueTask(Task task)
        {
            new Thread(() => TryExecuteTask(task)) { IsBackground = true }.Start();
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return TryExecuteTask(task);
        }
    }
}
