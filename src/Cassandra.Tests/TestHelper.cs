using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Serialization;
using IgnoreAttribute = Cassandra.Mapping.Attributes.IgnoreAttribute;

namespace Cassandra.Tests
{
    internal static class TestHelper
    {
        public static Row CreateRow(IDictionary<string, object> valueMap)
        {
            var columns = new List<CqlColumn>();
            var rowValues = new List<object>();
            var serializer = new Serializer(4);
            foreach (var kv in valueMap)
            {
                if (kv.Value != null)
                {
                    IColumnInfo typeInfo;
                    var typeCode = serializer.GetCqlType(kv.Value.GetType(), out typeInfo);
                    columns.Add(new CqlColumn { Name = kv.Key, TypeCode = typeCode, TypeInfo = typeInfo });
                }
                else
                {
                    columns.Add(new CqlColumn() { Name = kv.Key, TypeCode = ColumnTypeCode.Text });
                }
                rowValues.Add(kv.Value);
            }
            var i = 0;
            return new Row(rowValues.ToArray(), columns.ToArray(), valueMap.ToDictionary(kv => kv.Key, kv => i++));
        }

        public static IEnumerable<Row> CreateRows(IEnumerable<Dictionary<string, object>> valueMapList)
        {
            return valueMapList.Select(CreateRow);
        }

        public static Host CreateHost(string address, string dc = "dc1", string rack = "rack1", IEnumerable<string> tokens = null)
        {
            var h = new Host(new IPEndPoint(IPAddress.Parse(address), ProtocolOptions.DefaultPort), new ConstantReconnectionPolicy(1));
            h.SetLocationInfo(dc, rack);
            h.Tokens = tokens;
            return h;
        }

        public static byte GetLastAddressByte(Host h)
        {
            return GetLastAddressByte(h.Address);
        }

        public static byte GetLastAddressByte(IPEndPoint ep)
        {
            return GetLastAddressByte(ep.Address);
        }

        public static byte GetLastAddressByte(IPAddress address)
        {
            return address.GetAddressBytes()[3];
        }

        /// <summary>
        /// Invokes actions in parallel using 1 thread per action
        /// </summary>
        public static void ParallelInvoke(IEnumerable<Action> actions)
        {
            var parallelOptions = new ParallelOptions
            {
                TaskScheduler = new ThreadPerTaskScheduler(), 
                MaxDegreeOfParallelism = 1000
            };
            Parallel.Invoke(parallelOptions, actions.ToArray());
        }

        public static byte[] HexToByteArray(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                             .Where(x => x % 2 == 0)
                             .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                             .ToArray();
        }

        /// <summary>
        /// Invokes the same action multiple times in parallel using 1 thread per action
        /// </summary>
        public static void ParallelInvoke(Action action, int times)
        {
            ParallelInvoke(new List<Action>(Enumerable.Repeat(action, times)));
        }

        /// <summary>
        /// Invokes the same action multiple times serially using the current thread
        /// </summary>
        public static void Invoke(Action action, int times)
        {
            for (var i = 0; i < times; i++)
            {
                action();
            }
        }

        /// <summary>
        /// Invokes the same action multiple times serially using the current thread
        /// </summary>
        public static void Invoke(Action<int> action, int times)
        {
            for (var i = 0; i < times; i++)
            {
                action(i);
            }
        }

        /// <summary>
        /// Waits on the current thread until the condition is met
        /// </summary>
        public static void WaitUntil(Func<bool> condition, int intervals = 500, int attempts = 10)
        {
            for (var i = 0; i < attempts && !condition(); i++)
            {
                Thread.Sleep(intervals);
            }
        }

        public static void AssertPropertiesEqual(object actual, object expected)
        {
            var properties = expected.GetType().GetProperties();
            foreach (var property in properties)
            {
                var expectedValue = property.GetValue(expected, null);
                var actualValue = property.GetValue(actual, null);

                if (actualValue is IList)
                {
                    CollectionAssert.AreEqual((IList) expectedValue, (IList) actualValue, new SimplifiedComparer(), "Values from property {0} do not match", property.Name);
                    continue;
                }
                SimplifyValues(ref actualValue, ref expectedValue);
                Assert.AreEqual(expectedValue, actualValue,
                    // ReSharper disable once PossibleNullReferenceException
                    String.Format("Property {0}.{1} does not match. Expected: {2} but was: {3}", property.DeclaringType.Name, property.Name, expectedValue, actualValue));
            }
        }

        /// <summary>
        /// Gets a DateTimeOffset down to millisecond precision using the same method that C* driver does when storing timestamps.
        /// </summary>
        public static DateTimeOffset ToMillisecondPrecision(this DateTimeOffset dateTime)
        {
            return new DateTimeOffset(dateTime.Ticks - (dateTime.Ticks % TimeSpan.TicksPerMillisecond), dateTime.Offset);
        }

        /// <summary>
        /// Gets a nullable DateTimeOffset down to millisecond precision using the same method that C* driver does when storing timestamps.
        /// </summary>
        public static DateTimeOffset? ToMillisecondPrecision(this DateTimeOffset? dateTime)
        {
            if (dateTime.HasValue == false)
                return null;

            return dateTime.Value.ToMillisecondPrecision();
        }

        public static async Task<T> DelayedTask<T>(T result, int dueTimeMs = 50, Action afterDelay = null)
        {
            await Task.Delay(dueTimeMs).ConfigureAwait(false);
            if (afterDelay != null)
            {
                afterDelay();
            }
            return result;
        }

        public static async Task<T> DelayedTask<T>(Func<T> result, int dueTimeMs = 50)
        {
            await Task.Delay(dueTimeMs).ConfigureAwait(false);
            return result();
        }

        /// <summary>
        /// Uses the precision 
        /// </summary>
        internal static void SimplifyValues(ref object actualValue, ref object expectedValue)
        {
            if (actualValue is DateTimeOffset && expectedValue is DateTimeOffset)
            {
                actualValue = ((DateTimeOffset)actualValue).ToMillisecondPrecision();
                expectedValue = ((DateTimeOffset)expectedValue).ToMillisecondPrecision();
                return;
            }
            if (actualValue is DateTime && expectedValue is DateTime)
            {
                actualValue = ((DateTimeOffset)(DateTime)actualValue).ToMillisecondPrecision();
                expectedValue = ((DateTimeOffset)(DateTime)expectedValue).ToMillisecondPrecision();
                // ReSharper disable once RedundantJumpStatement
                return;
            }
        }

        /// <summary>
        /// Converts an object to a dictionary containing the public properties name and values
        /// </summary>
        public static Dictionary<string, object> ToDictionary(object someObject)
        {
            return someObject.GetType()
                             .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                             .Where(p => !p.GetCustomAttributes(typeof(IgnoreAttribute), true).Any())
                             .ToDictionary(prop => prop.Name, prop => prop.GetValue(someObject, null));
        }

        internal class PropertyComparer : IComparer
        {
            public int Compare(object x, object y)
            {
                AssertPropertiesEqual(y, x);
                return 0;
            }
        }

        internal class SimplifiedComparer : IComparer
        {
            public int Compare(object x, object y)
            {
                SimplifyValues(ref x, ref y);
                return Comparer<object>.Default.Compare(x, y);
            }
        }

        /// <summary>
        /// Gets the path string to home (via HOME or USERPROFILE env variables)
        /// </summary>
        public static string GetHomePath()
        {
            var home = Environment.GetEnvironmentVariable("USERPROFILE");
            if (!string.IsNullOrEmpty(home))
            {
                return home;
            }
            home = Environment.GetEnvironmentVariable("HOME");
            if (string.IsNullOrEmpty(home))
            {
                throw new NotSupportedException("HOME or USERPROFILE are not defined");
            }
            return home;
        }

        /// <summary>
        /// Executes the <see cref="Func{T}"/> provided n times, awaiting for the task to be completed, limiting the
        /// concurrency.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <param name="times"></param>
        /// <param name="limit"></param>
        /// <returns>A Task that can be awaited.</returns>
        public static Task TimesLimit<T>(Func<Task<T>> action, int times, int limit)
        {
            var tcs = new TaskCompletionSource<bool>();
            var counter = new SendReceiveCounter();
            for (var i = 0; i < limit; i++)
            {
                SendNew(action, tcs, counter, times);
            }
            return tcs.Task;
        }

        private static void SendNew<T>(Func<Task<T>> action, TaskCompletionSource<bool> tcs, SendReceiveCounter counter, int maxLength)
        {
            var sendCount = counter.IncrementSent();
            if (sendCount > maxLength)
            {
                return;
            }
            var t1 = action();
            t1.ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    // ReSharper disable once AssignNullToNotNullAttribute
                    tcs.TrySetException(t.Exception.InnerException);
                    return;
                }
                var received = counter.IncrementReceived();
                if (received == maxLength)
                {
                    tcs.TrySetResult(true);
                    return;
                }
                SendNew(action, tcs, counter, maxLength);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private class SendReceiveCounter
        {
            private int _receiveCounter;
            private int _sendCounter;

            public int IncrementSent()
            {
                return Interlocked.Increment(ref _sendCounter);
            }

            public int IncrementReceived()
            {
                return Interlocked.Increment(ref _receiveCounter);
            }
        }
    }
}
