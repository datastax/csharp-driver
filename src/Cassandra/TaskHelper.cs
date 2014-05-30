using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    internal static class TaskHelper
    {
        /// <summary>
        /// Returns a faulted task with the provided exception
        /// </summary>
        public static Task<TResult> FromException<TResult>(Exception exception)
        {
            var tcs = new TaskCompletionSource<TResult>();
            tcs.SetException(exception);
            return tcs.Task;
        }

        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the Aggregate exception thrown by the task or a TimeoutException
        /// </summary>
        /// <param name="timeout">timeout in milliseconds</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static void WaitToComplete(Task task, int timeout = System.Threading.Timeout.Infinite)
        {
            //It should wait and throw any exception
            task.Wait(timeout);
            if (task.Status != TaskStatus.RanToCompletion)
            {
                throw new TimeoutException("The task didn't complete before timeout.");
            }
        }
    }
}
