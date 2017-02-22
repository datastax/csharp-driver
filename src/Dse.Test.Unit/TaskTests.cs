//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TaskTests
    {
        [Test]
        public void TaskHelper_Then_Continues_Completed_Tasks()
        {
            var completedTask = Completed<bool>();
            var t2 = completedTask.Then(_ => TestHelper.DelayedTask(true));
            t2.Wait();
            Assert.True(t2.Result);
        }

        [Test]
        public void TaskHelper_Then_Continues_Delayed_Tasks()
        {
            var t1 = TestHelper.DelayedTask(1);
            var t2 = t1.Then(inValue => TestHelper.DelayedTask(2 + inValue));
            t2.Wait();
            Assert.AreEqual(3, t2.Result);
        }

        [Test]
        public void TaskHelper_Then_Propagates_Exceptions()
        {
            var tcs = new TaskCompletionSource<bool>();
            var t2 = tcs.Task.Then(_ => TestHelper.DelayedTask(true));
            tcs.SetException(new InvalidQueryException("Dummy exception"));
            var ex = Assert.Throws<AggregateException>(() => t2.Wait());
            Assert.IsInstanceOf<InvalidQueryException>(ex.InnerException);
            Assert.AreEqual("Dummy exception", ex.InnerException.Message);
        }

        [Test]
        public void TaskHelper_ContinueSync_Continues_Completed_Tasks()
        {
            var completedTask = Completed<bool>();
            var t2 = completedTask.ContinueSync(_ => true);
            t2.Wait();
            Assert.True(t2.Result);
        }

        [Test]
        public void TaskHelper_ContinueSync_Continues_Faulted_Tasks()
        {
            var tcs = new TaskCompletionSource<bool>();
            tcs.SetException(new InvalidQueryException("Dummy exception"));
            var t2 = tcs.Task.ContinueSync(_ => true);
            var ex = Assert.Throws<AggregateException>(() => t2.Wait());
            Assert.AreEqual("Dummy exception", ex.InnerExceptions[0].Message);
        }

        [Test]
        public void TaskHelper_ContinueSync_Continues_Delayed_Tasks()
        {
            var t1 = TestHelper.DelayedTask(1);
            var t2 = t1.ContinueSync(inValue => 2 + inValue);
            t2.Wait();
            Assert.AreEqual(3, t2.Result);
        }

        [Test]
        public void TaskHelper_ContinueSync_Propagates_Exceptions()
        {
            var tcs = new TaskCompletionSource<bool>();
            var t2 = tcs.Task.ContinueSync(_ => true);
            tcs.SetException(new InvalidQueryException("Dummy exception"));
            var ex = Assert.Throws<AggregateException>(t2.Wait);
            Assert.AreEqual(1, ex.InnerExceptions.Count);
            Assert.AreEqual("Dummy exception", ex.InnerException.Message);
        }

        [Test]
        public void Task_Continue_Delayed_Exception()
        {
            var t1 = TestHelper.DelayedTask(true);
            var t2 = t1.ContinueWith(t =>
            {
                if (t1.Result)
                {
                    throw new InvalidQueryException("Dummy exception from continuation");
                }
                return 2;
            }, TaskContinuationOptions.ExecuteSynchronously);

            var ex = Assert.Throws<AggregateException>(t2.Wait);
            Assert.AreEqual(1, ex.InnerExceptions.Count);
            Assert.AreEqual("Dummy exception from continuation", ex.InnerException.Message);
        }

        [Test]
        public void TaskHelper_Completed_Continues_On_The_Same_Thread()
        {
            var threadIdInit = Thread.CurrentThread.ManagedThreadId;
            var threadIdContinue = 0;
            TaskHelper.Completed.ContinueWith(t =>
            {
                threadIdContinue = Thread.CurrentThread.ManagedThreadId;
            }, TaskContinuationOptions.ExecuteSynchronously).Wait();

            Trace.TraceInformation("{0} - {1}", threadIdInit, threadIdContinue);
            Assert.AreEqual(threadIdInit, threadIdContinue);
        }

        [Test]
        public void TaskHelper_TaskCompletionSourceWithTimeout_Sets_Exception_When_Expired()
        {
            var ex = new TimeoutException("Test message");
            var tcs = TaskHelper.TaskCompletionSourceWithTimeout<int>(100, () => ex);
            var task = tcs.Task;
            Thread.Sleep(200);
            Assert.AreEqual(TaskStatus.Faulted, task.Status);
            Assert.NotNull(task.Exception);
            Assert.AreEqual(ex, task.Exception.InnerException);
        }

        /// <summary>
        /// Gets a completed task
        /// </summary>
        private static Task<T> Completed<T>()
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(default(T));
            return tcs.Task;
        }
    }
}
