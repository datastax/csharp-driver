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
        private readonly LinkedList<Task> _tasks = new LinkedList<Task>();
        private readonly int _maxParallelismLevel;
        private int _delegatesRunning;


        /// <summary>
        /// Gets the maximum concurrency level supported by this scheduler.
        /// </summary>
        public override int MaximumConcurrencyLevel
        {
            get {  return _maxParallelismLevel; }
        }

        /// <summary>
        /// Initializes an instance of the scheduler with the specified degree of parallelism.
        /// </summary>
        /// <param name="maxParallelismLevel">The maximum degree of parallelism allowed by this scheduler.</param>
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
        /// <param name="task">The task to be queued.</param>
        protected sealed override void QueueTask(Task task)
        {
            lock (_tasks)
            {
                _tasks.AddLast(task);
                if (_delegatesRunning >= _maxParallelismLevel)
                {
                    //We are already processing items
                    return;
                }
                _delegatesRunning++;
                NotifyThreadPoolOfPendingWork();
            }
        }

        /// <summary>
        /// Informs the ThreadPool that there's work to be executed for this scheduler.
        /// </summary>
        private void NotifyThreadPoolOfPendingWork()
        {
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                _isCurrentThreadProcessing = true;
                try
                {
                    // Process all available items in the queue.
                    while (true)
                    {
                        Task item;
                        lock (_tasks)
                        {
                            if (_tasks.Count == 0)
                            {
                                //We are done processing, and get out.
                                _delegatesRunning--;
                                break;
                            }

                            // Get the next item from the queue
                            item = _tasks.First.Value;
                            _tasks.RemoveFirst();
                        }

                        // Execute the task we pulled out of the queue
                        base.TryExecuteTask(item);
                    }
                }
                finally
                {
                    _isCurrentThreadProcessing = false;
                }
            }, null);
        }

        /// <summary>
        /// Attempts to execute the specified task on the current thread.
        /// </summary>
        /// <param name="task">The task to be executed.</param>
        /// <param name="taskWasPreviouslyQueued"></param>
        /// <returns>Whether the task could be executed on the current thread.</returns>
        protected sealed override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            // If this thread isn't already processing a task, we don't support inlining
            if (!_isCurrentThreadProcessing)
            {
                return false;
            }
            // If the task was previously queued, remove it from the queue
            if (taskWasPreviouslyQueued)
            {
                TryDequeue(task);
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
            lock (_tasks)
            {
                return _tasks.Remove(task);
            }
        }


        /// <summary>
        /// Gets an enumerable of the tasks currently scheduled on this scheduler.
        /// </summary>
        /// <returns>An enumerable of the tasks currently scheduled.</returns>
        protected sealed override IEnumerable<Task> GetScheduledTasks()
        {
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(_tasks, ref lockTaken);
                if (lockTaken)
                {
                    return _tasks.ToArray();
                }
                else
                {
                    //Parallel calls to scheduled tasks from different threads is not supported
                    throw new NotSupportedException();
                }
            }
            finally
            {
                if (lockTaken)
                {
                    Monitor.Exit(_tasks);
                }
            }
        }
    }
}
