using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using IgnoreAttribute = Cassandra.Mapping.Attributes.IgnoreAttribute;

namespace Cassandra.Tests
{
    internal static class TestHelper
    {
        public static Row CreateRow(IDictionary<string, object> valueMap)
        {
            var columns = new List<CqlColumn>();
            var rowValues = new List<object>();
            foreach (var kv in valueMap)
            {
                if (kv.Value != null)
                {
                    IColumnInfo typeInfo;
                    var typeCode = TypeCodec.GetColumnTypeCodeInfo(kv.Value.GetType(), out typeInfo);
                    columns.Add(new CqlColumn() { Name = kv.Key, TypeCode = typeCode, TypeInfo = typeInfo });
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

        public static Task<T> DelayedTask<T>(T result, int dueTimeMs = 50)
        {
            var tcs = new TaskCompletionSource<T>();
            var timer = new Timer(delegate(object self)
            {
                ((Timer)self).Dispose();
                tcs.TrySetResult(result);
            });

            timer.Change(dueTimeMs, -1);
            return tcs.Task;
        }

        public static Task<T> DelayedTask<T>(Func<T> result, int dueTimeMs = 50)
        {
            var tcs = new TaskCompletionSource<T>();
            var timer = new Timer(delegate(object self)
            {
                ((Timer)self).Dispose();
                try
                {
                    tcs.TrySetResult(result());
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });

            timer.Change(dueTimeMs, -1);
            return tcs.Task;
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
                             .Where(p => p.GetCustomAttributes(typeof(IgnoreAttribute), true).Length == 0)
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
                return Comparer.Default.Compare(x, y);
            }
        }
    }
}
