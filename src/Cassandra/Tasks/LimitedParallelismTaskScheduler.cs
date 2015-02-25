using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.Tasks
{
    /// <summary>
    /// A task scheduler that runs on top of the ThreadPool but schedules limited amount of task in parallel.
    /// </summary>
    internal class LimitedParallelismTaskScheduler : TaskScheduler
    {
        [ThreadStatic]
        private static bool _isCurrentThreadProcessing;
        private readonly ConcurrentQueue<Task> _tasks = new ConcurrentQueue<Task>();
        private readonly int _maxParallelismLevel;
        /// <summary>
        /// The number of task queued or running
        /// </summary>
        private int _taskProcessing;

        /// <summary>
        /// Gets the maximum concurrency level supported by this scheduler.
        /// </summary>
        public sealed override int MaximumConcurrencyLevel
        {
            get { return _maxParallelismLevel; }
        }

        /// <summary>
        /// Initializes an instance of the LimitedParallelismTaskScheduler class with the
        /// specified degree of parallelism.
        /// </summary>
        /// <param name="maxParallelismLevel">The maximum degree of parallelism provided by this scheduler.</param>
        public LimitedParallelismTaskScheduler(int maxParallelismLevel)
        {
            if (maxParallelismLevel < 1)
            {
                throw new ArgumentOutOfRangeException("maxParallelismLevel");
            }
            _maxParallelismLevel = maxParallelismLevel;
        }

        /// <summary>
        /// Queues a task to the scheduler.
        /// </summary>
        protected sealed override void QueueTask(Task task)
        {
            // Add the task to the list of tasks to be processed.
            _tasks.Enqueue(task);
            var processing = Interlocked.Increment(ref _taskProcessing);
            if (processing > _maxParallelismLevel)
            {
                //There are too many tasks scheduled
                //Do not notify of pending work
                return;
            }
            //There is pending items to processes
            NotifyThreadPoolOfPendingWork();
        }

        /// <summary>
        /// Informs the ThreadPool that there's work to be executed for this scheduler.
        /// </summary>
        private void NotifyThreadPoolOfPendingWork()
        {
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                // Note that the current thread is now processing work items.
                // This is necessary to enable inlining of tasks into this thread.
                _isCurrentThreadProcessing = true;
                try
                {
                    // Process all available items in the queue.
                    while (true)
                    {
                        Task item;
                        var dequeued = _tasks.TryDequeue(out item);
                        if (!dequeued)
                        {
                            break;
                        }
                        Interlocked.Decrement(ref _taskProcessing);
                        TryExecuteTask(item);
                    }
                }
                finally
                {
                    _isCurrentThreadProcessing = false;
                }
            }, null);
        }

        /// <summary>Attempts to execute the specified task on the current thread.</summary>
        /// <param name="task">The task to be executed.</param>
        /// <param name="taskWasPreviouslyQueued"></param>
        /// <returns>Whether the task could be executed on the current thread.</returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            if (taskWasPreviouslyQueued)
            {
                return false;
            }
            if (!_isCurrentThreadProcessing)
            {
                // If this thread isn't already processing a task, we don't support inlining
                return false;
            }

            // Try to run the task.
            return base.TryExecuteTask(task);
        }

        /// <summary>
        /// Attempts to remove a previously scheduled task from the scheduler.
        /// </summary>
        /// <param name="task">The task to be removed.</param>
        /// <returns>Whether the task could be found and removed.</returns>
        protected sealed override bool TryDequeue(Task task)
        {
            return false;
        }

        /// <summary>
        /// Generates an enumerable of <see cref="T:System.Threading.Tasks.Task">Task</see> instances
        /// currently queued to the scheduler waiting to be executed.
        /// </summary>
        protected sealed override IEnumerable<Task> GetScheduledTasks()
        {
            return _tasks.ToArray();
        }
    }
}
