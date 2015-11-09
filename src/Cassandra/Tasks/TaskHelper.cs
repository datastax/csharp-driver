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

﻿using System;
using System.Reflection;
﻿using System.Threading;
﻿using System.Threading.Tasks;

namespace Cassandra.Tasks
{
    internal static class TaskHelper
    {
        private static readonly MethodInfo PreserveStackMethod;
        private static readonly Action<Exception> PreserveStackHandler = ex => { };
        private static readonly Task<bool> CompletedTask;

        static TaskHelper()
        {
            try
            {
                TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
                tcs.SetResult(false);
                CompletedTask = tcs.Task;
                PreserveStackMethod = typeof(Exception).GetMethod("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic);
                if (PreserveStackMethod == null)
                {
                    return;
                }
                //Only under .NET Framework
                PreserveStackHandler = ex =>
                {
                    try
                    {
                        //This could result in a MemberAccessException
                        PreserveStackMethod.Invoke(ex, null);
                    }
                    catch
                    {
                        //Tried to preserve the stack trace, failed.
                        //Move on on.
                    }
                };
            }
            catch
            {
                //Do nothing
                //Do not throw exceptions on static constructors
            }
        }

        /// <summary>
        /// Gets a single completed task
        /// </summary>
        public static Task<bool> Completed
        {
            get { return CompletedTask; }
        }

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

                if (callback != null)
                {
                    callback(tcs.Task);
                }

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
            WaitToComplete((Task) task, timeout);
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
                    throw PreserveStackTrace(ex.InnerExceptions[0]);
                }
                throw;
            }
            if (task.Status != TaskStatus.RanToCompletion)
            {
                throw new TimeoutException("The task didn't complete before timeout.");
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
        /// Required when retrowing exceptions to maintain the stack trace of the original exception
        /// </summary>
        private static Exception PreserveStackTrace(Exception ex)
        {
            PreserveStackHandler(ex);
            return ex;
        }

        /// <summary>
        /// Smart ContinueWith
        /// </summary>
        public static Task Continue<TIn>(this Task<TIn> task, Action<Task<TIn>> next)
        {
            return task.Continue(t =>
            {
                next(t);
                return 0;
            });
        }

        /// <summary>
        /// Smart ContinueWith
        /// </summary>
        public static Task<TOut> Continue<TIn, TOut>(this Task<TIn> task, Func<Task<TIn>, TOut> next)
        {
            if (!task.IsCompleted)
            {
                //Do an actual continuation
                return task.ContinueWith(innerTask => DoNext(innerTask, next), TaskContinuationOptions.ExecuteSynchronously).Unwrap();
            }
            //Use the task result to build the task
            return DoNext(task, next);
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
        /// Invokes the next function immediately and assigns the result to a Task
        /// </summary>
        private static Task<TOut> DoNext<TIn, TOut>(Task<TIn> task, Func<Task<TIn>, TOut> next)
        {
            var tcs = new TaskCompletionSource<TOut>();
            try
            {
                var res = next(task);
                tcs.TrySetResult(res);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
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
                // ReSharper disable once PossibleNullReferenceException, AccessToModifiedClosure
                timer.Dispose();
                //Transition the underlying Task outside the IO thread
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
            timer = new Timer(timerCallback, null, milliseconds, Timeout.Infinite);
            tcs.Task.ContinueWith(t =>
            {
                //Timer can be disposed multiple times
                timer.Dispose();
            });
            return tcs;
        }
    }
}
