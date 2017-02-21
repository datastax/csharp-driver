//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Dse.Tasks
{
    internal static class TaskHelper
    {
        private static readonly MethodInfo PreserveStackMethod;
        private static readonly Action<Exception> PreserveStackHandler = ex => { };

        static TaskHelper()
        {
            try
            {
                PreserveStackMethod = typeof(Exception).GetTypeInfo()
                    .GetMethod("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic);
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
        private static void DoNextAndHandle<TIn, TOut>(TaskCompletionSource<TOut> tcs, Task<TIn> previousTask,
            Func<TIn, TOut> next)
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
        /// Returns a faulted task with the provided exception
        /// </summary>
        public static Task<TResult> FromException<TResult>(Exception exception)
        {
            var tcs = new TaskCompletionSource<TResult>();
            tcs.SetException(exception);
            return tcs.Task;
        }

        /// <summary>
        /// Required when re-throwing exceptions to maintain the stack trace of the original exception
        /// </summary>
        private static Exception PreserveStackTrace(Exception ex)
        {
            PreserveStackHandler(ex);
            return ex;
        }

        private static void SetInnerException<T>(TaskCompletionSource<T> tcs, AggregateException ex)
        {
            tcs.TrySetException(ex.InnerException);
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


        /// <summary>
        /// Returns a completed task with the result.
        /// </summary>
        public static Task<T> ToTask<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
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
            WaitToComplete((Task)task, timeout);
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
    }
}
