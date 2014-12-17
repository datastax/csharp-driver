﻿using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using IgnoreAttribute = Cassandra.Mapping.Attributes.IgnoreAttribute;

namespace Cassandra.Tests
{
    internal static class TestHelper
    {
        public static Row CreateRow(IDictionary<string, object> valueMap)
        {
            var columns = new List<CqlColumn>();
            var rowValues = new List<byte[]>();
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
                rowValues.Add(TypeCodec.Encode(2, kv.Value));
            }
            var i = 0;
            return new Row(2, rowValues.ToArray(), columns.ToArray(), valueMap.ToDictionary(kv => kv.Key, kv => i++));
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

        /// <summary>
        /// Invokes the same action multiple times in parallel using 1 thread per action
        /// </summary>
        public static void ParallelInvoke(Action action, int times)
        {
            ParallelInvoke(new List<Action>(Enumerable.Repeat<Action>(action, 100)));
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
