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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
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
            var tcs = TaskHelper.TaskCompletionSourceWithTimeout<int>(20, () => ex);
            var task = tcs.Task;
            Thread.Sleep(200);
            Assert.AreEqual(TaskStatus.Faulted, task.Status);
            Assert.NotNull(task.Exception);
            Assert.AreEqual(ex, task.Exception.InnerException);
        }

        [Test]
        public void TaskHelper_TaskCompletionSourceWithTimeout_Does_Not_Invoke_Delegate_When_Transitioned()
        {
            bool called = false;
            var tcs = TaskHelper.TaskCompletionSourceWithTimeout<int>(200, () =>
            {
                called = true;
                return new Exception();
            });
            var task = tcs.Task;
            tcs.TrySetResult(1);
            Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
            Thread.Sleep(200);
            Assert.False(called);
        }

        [Test]
        public void ConfigureAwait_Used_For_Every_Awaited_Task()
        {
            var assemblyFile = new FileInfo(new Uri(GetType().GetTypeInfo().Assembly.Location).LocalPath);
            var directory = assemblyFile.Directory;
            while (directory != null && directory.Name != "src")
            {
                directory = directory.Parent;
            }
            if (directory == null)
            {
                Assert.Fail("src folder could not be determined");
            }
            directory = directory.GetDirectories("Cassandra").FirstOrDefault() ??
                        directory.GetDirectories("Dse").FirstOrDefault();
            if (directory == null)
            {
                Assert.Fail("Library source folder could not be determined");
            }
            var regex = new Regex("\\bawait\\b(?![^;]*ConfigureAwait\\(false\\))[^;]*;", 
                                  RegexOptions.Multiline | RegexOptions.Compiled);
            foreach (var fileInfo in directory.GetFiles("*.cs", SearchOption.AllDirectories))
            {
                var source = File.ReadAllText(fileInfo.FullName);
                var match = regex.Match(source);
                if (match.Success)
                {
                    Assert.Fail("Awaited Task without ConfigureAwait() call in file {0}: {1}", 
                                fileInfo.FullName, match.Value);
                }
            }
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
