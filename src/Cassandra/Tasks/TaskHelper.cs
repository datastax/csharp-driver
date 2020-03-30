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

using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Metrics.Internal;

namespace Cassandra.Tasks
{
    internal static class TaskHelper
    {
        static TaskHelper()
        {
            var tcs = new TaskCompletionSource<bool>();
            tcs.SetResult(false);
            Completed = tcs.Task;
        }

        /// <summary>
        /// Gets a single completed task
        /// </summary>
        public static Task<bool> Completed { get; }

        /// <summary>
        /// Returns an AsyncResult according to the .net async programming model (Begin)
        /// </summary>
        public static Task<TResult> ToApm<TResult>(this Task<TResult> task, AsyncCallback callback, object state)
        {
            if (task.AsyncState == state)
            {
                if (callback != null)
                {
                    task.ContinueWith(t => callback(t), TaskContinuationOptions.ExecuteSynchronously);
                }
                return task;
            }

            var tcs = new TaskCompletionSource<TResult>(state);
            task.ContinueWith(delegate
            {
                if (task.IsFaulted)
                {
                    // ReSharper disable once PossibleNullReferenceException
                    tcs.TrySetException(task.Exception.InnerExceptions);
                }
                else if (task.IsCanceled)
                {
                    tcs.TrySetCanceled();
                }
                else
                {
                    tcs.TrySetResult(task.Result);
                }

                callback?.Invoke(tcs.Task);

            }, TaskContinuationOptions.ExecuteSynchronously);
            return tcs.Task;
        }

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
        /// Waits the task to transition to RanToComplete and returns the Task.Result.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout in milliseconds</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static T WaitToComplete<T>(Task<T> task, int timeout = Timeout.Infinite)
        {
            TaskHelper.WaitToComplete((Task) task, timeout);
            return task.Result;
        }
        
        /// <summary>
        /// Increments session client timeout counter in case of timeout.
        /// </summary>
        public static void WaitToCompleteWithMetrics(IMetricsManager manager, Task task, int timeout = Timeout.Infinite)
        {
            if (!(manager?.AreMetricsEnabled ?? false))
            {
                TaskHelper.WaitToComplete(task, timeout);
                return;
            }

            try
            {
                TaskHelper.WaitToComplete(task, timeout);
            }
            catch (TimeoutException)
            {
                manager.GetSessionMetrics().CqlClientTimeouts.Increment();
                throw;
            }
        }

        /// <summary>
        /// Increments session client timeout counter in case of timeout.
        /// </summary>
        public static T WaitToCompleteWithMetrics<T>(IMetricsManager manager, Task<T> task, int timeout = Timeout.Infinite)
        {
            TaskHelper.WaitToCompleteWithMetrics(manager, (Task) task, timeout);
            return task.Result;
        }

        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout in milliseconds</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static void WaitToComplete(Task task, int timeout = Timeout.Infinite)
        {
            //It should wait and throw any exception
            try
            {
                task.Wait(timeout);
            }
            catch (AggregateException ex)
            {
                ex = ex.Flatten();
                //throw the actual exception when there was a single exception
                if (ex.InnerExceptions.Count == 1)
                {
                    ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                }
                throw;
            }
            if (task.Status != TaskStatus.RanToCompletion)
            {
                throw new TimeoutException("The task didn't complete before timeout.");
            }
        }

        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout in milliseconds</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static async Task WaitToCompleteAsync(this Task task, int timeout = Timeout.Infinite)
        {
            //It should wait and throw any exception
            if (timeout == Timeout.Infinite)
            {
                await task.ConfigureAwait(false);
                return;
            }

            try
            {
                var timeoutTask = Task.Delay(TimeSpan.FromMilliseconds(timeout));
                var finishedTask = await Task.WhenAny(task, timeoutTask).ConfigureAwait(false);
                if (finishedTask == timeoutTask)
                {
                    throw new TimeoutException("The task didn't complete before timeout.");
                }
                await task.ConfigureAwait(false);
            }
            catch (AggregateException ex)
            {
                ex = ex.Flatten();
                //throw the actual exception when there was a single exception
                if (ex.InnerExceptions.Count == 1)
                {
                    ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                }
                throw;
            }
        }

        /// <summary>
        /// Attempts to transition the underlying Task to RanToCompletion or Faulted state.
        /// </summary>
        public static void TrySet<T>(this TaskCompletionSource<T> tcs, Exception ex, T result)
        {
            if (ex != null)
            {
                tcs.TrySetException(ex);
            }
            else
            {
                tcs.TrySetResult(result);
            }
        }
        
        /// <summary>
        /// Attempts to transition the underlying Task to RanToCompletion or Faulted state.
        /// </summary>
        public static void TrySetRequestError<T>(this TaskCompletionSource<T> tcs, IRequestError error, T result)
        {
            if (error?.Exception != null)
            {
                tcs.TrySetException(error.Exception);
            }
            else
            {
                tcs.TrySetResult(result);
            }
        }

        /// <summary>
        /// Smart ContinueWith that executes the sync delegate once the initial task is completed and returns 
        /// a Task of the result of sync delegate while propagating exceptions
        /// </summary>
        public static Task<TOut> ContinueSync<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> next)
        {
            const TaskContinuationOptions options = TaskContinuationOptions.ExecuteSynchronously;
            var tcs = new TaskCompletionSource<TOut>();
            if (task.IsCompleted)
            {
                DoNextAndHandle(tcs, task, next);
                return tcs.Task;
            }
            task.ContinueWith(previousTask =>
            {
                DoNextAndHandle(tcs, previousTask, next);
            }, options);
            return tcs.Task;
        }

        /// <summary>
        /// Invokes the next function immediately and assigns the result to a Task, propagating exceptions to the new Task
        /// </summary>
        private static void DoNextAndHandle<TIn, TOut>(TaskCompletionSource<TOut> tcs, Task<TIn> previousTask, Func<TIn, TOut> next)
        {
            try
            {
                if (previousTask.IsFaulted && previousTask.Exception != null)
                {
                    SetInnerException(tcs, previousTask.Exception);
                    return;
                }
                if (previousTask.IsCanceled)
                {
                    tcs.TrySetCanceled();
                    return;
                }
                tcs.TrySetResult(next(previousTask.Result));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        }

        /// <summary>
        /// Once Task is completed with another Task, returning the second task, propagating exceptions to the second Task.
        /// </summary>
        public static Task<TOut> Then<TIn, TOut>(this Task<TIn> task, Func<TIn, Task<TOut>> next)
        {
            const TaskContinuationOptions options = TaskContinuationOptions.ExecuteSynchronously;
            var tcs = new TaskCompletionSource<TOut>();
            if (task.IsCompleted)
            {
                //RanToCompletion, Faulted, or Canceled.
                DoNextThen(tcs, task, next, options);
                return tcs.Task;
            }
            task.ContinueWith(previousTask =>
            {
                DoNextThen(tcs, task, next, options);
            }, options);

            return tcs.Task;
        }

        /// <summary>
        /// Checks whether the task has finished.
        /// </summary>
        public static bool HasFinished(this Task task)
        {
            return task.IsCompleted || task.IsCanceled || task.IsFaulted;
        }

        private static void DoNextThen<TIn, TOut>(TaskCompletionSource<TOut> tcs, Task<TIn> previousTask, Func<TIn, Task<TOut>> next, TaskContinuationOptions options)
        {
            if (previousTask.IsFaulted && previousTask.Exception != null)
            {
                SetInnerException(tcs, previousTask.Exception);
                return;
            }
            if (previousTask.IsCanceled)
            {
                tcs.TrySetCanceled();
                return;
            }
            try
            {
                next(previousTask.Result).ContinueWith(nextTask =>
                {
                    if (nextTask.IsFaulted && nextTask.Exception != null)
                    {
                        SetInnerException(tcs, nextTask.Exception);
                        return;
                    }
                    if (nextTask.IsCanceled)
                    {
                        tcs.TrySetCanceled();
                        return;
                    }
                    try
                    {
                        tcs.TrySetResult(nextTask.Result);
                    }
                    catch (Exception ex)
                    {
                        tcs.TrySetException(ex);
                    }
                }, options);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        }

        private static void SetInnerException<T>(TaskCompletionSource<T> tcs, AggregateException ex)
        {
            // We are setting the Task from the tcs as faulted
            // The previous AggregateException is handled
            ex.Handle(_ => true);
            tcs.TrySetException(ex.InnerException);
        }

        public static Task<T> ToTask<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
            return tcs.Task;
        }

        /// <summary>
        /// It creates a <see cref="TaskCompletionSource{T}"/> that transitions to Faulted once 
        /// </summary>
        /// <typeparam name="T">The type of the result value associated with this <see cref="TaskCompletionSource{T}"/></typeparam>
        /// <param name="milliseconds">The timer due time in milliseconds</param>
        /// <param name="newTimeoutException">The method to call in case timeout expired</param>
        public static TaskCompletionSource<T> TaskCompletionSourceWithTimeout<T>(int milliseconds, Func<Exception> newTimeoutException)
        {
            var tcs = new TaskCompletionSource<T>();
            Timer timer = null;
            TimerCallback timerCallback = _ =>
            {
                // ReSharper disable once AccessToModifiedClosure
                // timer is a modified closure and can not be null
                var t = timer;
                if (t == null)
                {
                    throw new NullReferenceException("timer instance from closure is null");
                }
                t.Dispose();
                // Transition the underlying Task outside the IO thread
                Task.Factory.StartNew(() =>
                {
                    try
                    {
                        tcs.TrySetException(newTimeoutException());
                    }
                    catch (ObjectDisposedException)
                    {
                        //The task was already disposed: move on
                    }
                    catch (Exception ex)
                    {
                        tcs.TrySetException(ex);
                    }
                });
            };
            // We can not use constructor that sets the timer as the state object
            // as it is not available in .NET Standard 1.5 
            timer = new Timer(timerCallback, null, Timeout.Infinite, Timeout.Infinite);
            Interlocked.MemoryBarrier();
            tcs.Task.ContinueWith(t => timer.Dispose());
            timer.Change(milliseconds, Timeout.Infinite);
            return tcs;
        }

        /// <summary>
        /// Executes method after the provided delay
        /// </summary>
        public static Task<TOut> ScheduleExecution<TOut>(Func<TOut> method, HashedWheelTimer timer, int delay)
        {
            var tcs = new TaskCompletionSource<TOut>();
            timer.NewTimeout(state =>
            {
                var tcsState = (TaskCompletionSource<TOut>) state;
                try
                {
                    tcsState.SetResult(method());
                }
                catch (Exception ex)
                {
                    tcsState.SetException(ex);
                }
            }, tcs, delay);
            return tcs.Task;
        }

        /// <summary>
        /// Designed for Tasks that were started but the result should not be awaited upon (fire and forget).
        /// </summary>
        public static void Forget(this Task task)
        {
            // Avoid compiler warning CS4014 and Unobserved exceptions
            task?.ContinueWith(t =>
            {
                t.Exception?.Handle(_ => true);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Designed to Await tasks with a cancellation token when the method that returns the task doesn't
        /// accept a token.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<T> WithCancellation<T>(
            this Task<T> task, CancellationToken cancellationToken)
        {
            var cancellationCompletionSource = new TaskCompletionSource<bool>();

            using (cancellationToken.Register(() => cancellationCompletionSource.TrySetResult(true)))
            {
                if (task != await Task.WhenAny(task, cancellationCompletionSource.Task).ConfigureAwait(false))
                {
                    throw new OperationCanceledException(cancellationToken);
                }
            }

            return await task.ConfigureAwait(false);
        }

        /// <summary>
        /// Simple helper to create a CancellationToken that is cancelled after
        /// the provided <paramref name="timespan"/>.
        /// </summary>
        /// <param name="timespan">Timespan after which the returned <see cref="CancellationToken"/>
        /// is canceled.</param>
        /// <returns>A newly created <see cref="CancellationToken"/> that will be canceled
        /// after the specified <paramref name="timespan"/>.</returns>
        public static CancellationToken CancelTokenAfterDelay(TimeSpan timespan)
        {
            return new CancellationTokenSource(timespan).Token;
        }

        /// <summary>
        /// Calls <code>Task.Delay</code> with a cancellation token.
        /// </summary>
        /// <returns><code>true</code> if delay ran to completion; <code>false</code> if delay was canceled.</returns>
        public static async Task<bool> DelayWithCancellation(TimeSpan delayTimeSpan, CancellationToken token)
        {
            try
            {
                await Task.Delay(delayTimeSpan, token).ConfigureAwait(false);
                return true;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
        }

        public static Func<Task> ActionToAsync(Action act)
        {
            return () =>
            {
                act();
                return TaskHelper.Completed;
            };
        }
    }
}
