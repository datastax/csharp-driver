using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Concurrent;
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
using Microsoft.DotNet.InternalAbstractions;

namespace Cassandra.Tests
{
    internal static class TestHelper
    {
        /// <summary>
        /// Returns an address that's supposed to be unreachable.
        /// </summary>
        /// <remarks>
        /// Use the last address in the 172.16.0.0 – 172.31.255.255 range
        /// reserved for private networks (not commonly used) see RFC 1918.
        /// </remarks>
        public const string UnreachableHostAddress = "172.31.255.255";

        public static Row CreateRow(ICollection<KeyValuePair<string, object>> valueMap)
        {
            var columns = new List<CqlColumn>();
            var rowValues = new List<object>();
            var serializer = new Serializer(ProtocolVersion.MaxSupported);
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

        public static IEnumerable<Row> CreateRows(IEnumerable<IDictionary<string, object>> valueMapList)
        {
            return valueMapList.Select(CreateRow);
        }

        private static CqlColumn[] CreateColumns(ICollection<KeyValuePair<string, object>> rowValues)
        {
            var columns = new CqlColumn[rowValues.Count];
            var index = 0;
            var serializer = new Serializer(ProtocolVersion.MaxSupported);
            foreach (var kv in rowValues)
            {
                CqlColumn c;
                if (kv.Value != null)
                {
                    IColumnInfo typeInfo;
                    var typeCode = serializer.GetCqlType(kv.Value.GetType(), out typeInfo);
                    c = new CqlColumn
                    {
                        Name = kv.Key, TypeCode = typeCode, TypeInfo = typeInfo,
                        Type = kv.Value.GetType(), Index = index
                    };
                }
                else
                {
                    // Default to type Text
                    c = new CqlColumn
                    {
                        Name = kv.Key, TypeCode = ColumnTypeCode.Text, Type = typeof(string), Index = index
                    };
                }
                columns[index++] = c;
            }
            return columns;
        }

        /// <summary>
        /// Creates a RowSet given a list of rows, each row represented as a collection of keys (column names) and values (cell values).
        /// </summary>
        public static RowSet CreateRowSet(IList<ICollection<KeyValuePair<string, object>>> valueMapList)
        {
            if (valueMapList.Count == 0)
            {
                throw new NotSupportedException("This test helper can't create empty rowsets");
            }
            var rs = new RowSet { Columns = CreateColumns(valueMapList[0]) };
            foreach (var row in valueMapList.Select(CreateRow))
            {
                rs.AddRow(row);
            }
            return rs;
        }

        /// <summary>
        /// Creates a RowSet given a list of rows, each row represented as a collection of keys (column names) and values (cell values).
        /// </summary>
        public static RowSet CreateRowSetFromSingle(ICollection<KeyValuePair<string, object>> valueMap)
        {
            if (valueMap == null)
            {
                throw new ArgumentNullException("valueMap");
            }
            var rs = new RowSet { Columns = CreateColumns(valueMap) };
            rs.AddRow(CreateRow(valueMap));
            return rs;
        }

        public static KeyValuePair<TKey, TValue> CreateKeyValue<TKey, TValue>(TKey key, TValue value)
        {
            return new KeyValuePair<TKey, TValue>(key, value);
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
        /// Waits on the current thread until the condition is met and returns the number of attempts made
        /// </summary>
        public static int WaitUntil(Func<bool> condition, int intervals = 500, int attempts = 10)
        {
            var i = 0;
            for (; i < attempts && !condition(); i++)
            {
                Thread.Sleep(intervals);
            }
            return i;
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
        /// Returns the first value (recursive) converted to string
        /// </summary>
        internal static string FirstString(IEnumerable collection)
        {
            foreach (var p in collection)
            {
                if (p is IEnumerable)
                {
                    return FirstString((IEnumerable)p);
                }
                return Convert.ToString(p);
            }
            return null;
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

        public static bool IsWin
        {
            get
            {
#if !NETCORE
                switch (Environment.OSVersion.Platform)
                {
                    case PlatformID.Win32NT:
                    case PlatformID.Win32S:
                    case PlatformID.Win32Windows:
                        return true;
                }
                return false;
#else
                return RuntimeEnvironment.OperatingSystemPlatform == Platform.Windows;
#endif
            }
        }

        /// <summary>
        /// Determines if we are running under mono.
        /// </summary>
        public static bool IsMono
        {
            get
            {
                return Type.GetType("Mono.Runtime") != null;
            }
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

        internal class TestLoggerHandler : Logger.ILoggerHandler
        {
            private readonly ConcurrentQueue<Tuple<string, string, object[]>> _messages =
                new ConcurrentQueue<Tuple<string, string, object[]>>();

            public void Error(Exception ex)
            {
                _messages.Enqueue(Tuple.Create("error", (string)null, new object[] { ex }));
            }

            public void Error(string message, Exception ex = null)
            {
                _messages.Enqueue(Tuple.Create("error", message, new object[] { ex }));
            }

            public void Error(string message, params object[] args)
            {
                _messages.Enqueue(Tuple.Create("error", message, args));
            }

            public void Verbose(string message, params object[] args)
            {
                _messages.Enqueue(Tuple.Create("verbose", message, args));
            }

            public void Info(string message, params object[] args)
            {
                _messages.Enqueue(Tuple.Create("info", message, args));
            }

            public void Warning(string message, params object[] args)
            {
                _messages.Enqueue(Tuple.Create("warning", message, args));
            }

            public IEnumerable<Tuple<string, string, object[]>> DequeueAllMessages()
            {
                Tuple<string, string, object[]> value;
                while (_messages.TryDequeue(out value))
                {
                    yield return value;
                }
            }
        }
    }
}
