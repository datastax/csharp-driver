using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqlPoco
{
    internal static class TaskHelper
    {
        public static Task<TOut> Continue<TIn, TOut>(this Task<TIn> task, Func<Task<TIn>, TOut> next)
        {
            if (!task.IsCompleted)
            {
                //Do an actual continuation
                return ContinueContext(task, next);
            }
            //Use the task result to build the task
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

        public static Task<TOut> ContinueContext<TIn, TOut>(
                Task<TIn> task,
                Func<Task<TIn>, TOut> next)
        {
            var ctxt = SynchronizationContext.Current;
            return task.ContinueWith(innerTask =>
            {
                var tcs = new TaskCompletionSource<TOut>();

                try
                {
                    if (ctxt != null)
                    {
                        ctxt.Post(state =>
                        {
                            try
                            {
                                var res = next(innerTask);
                                tcs.TrySetResult(res);
                            }
                            catch (Exception ex)
                            {
                                tcs.TrySetException(ex);
                            }
                        }, null);
                    }   
                    else
                    {
                        var res = next(innerTask);
                        tcs.TrySetResult(res);
                    }
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }

                return tcs.Task;
            }, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
        }

        public static Task<T> ToTask<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
            return tcs.Task;
        }
    }
}
