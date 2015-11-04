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

using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Represents an CQL row
    /// </summary>
    public class Row : IEnumerable<object>
    {
        private readonly object[] _rowValues;
        /// <summary>
        /// Gets or sets the index of the columns within the row
        /// </summary>
        protected Dictionary<string, int> ColumnIndexes { get; set; }

        /// <summary>
        /// Gets or sets the columns information
        /// </summary>
        protected CqlColumn[] Columns { get; set; }

        [Obsolete("This property is deprecated and to be removed in future versions.")]
        protected byte[][] Values { get; set; }

        [Obsolete("This property is deprecated and to be removed in future versions.")]
        protected int ProtocolVersion { get; set; }

        /// <summary>
        /// Gets the total amount of values inside the row
        /// </summary>
        public int Length
        {
            get { return _rowValues.Length; }
        }

        /// <summary>
        /// Gets the stored value in the column specified by index
        /// </summary>
        public object this[int index]
        {
            get { return GetValue(typeof(object), index); }
        }

        /// <summary>
        /// Gets the stored value in the column specified by name
        /// </summary>
        public object this[string name]
        {
            get { return this[ColumnIndexes[name]]; }
        }

        /// <summary>
        /// Initializes a new instance of the Cassandra.Row class
        /// </summary>
        public Row()
        {
            //Default constructor for client test and mocking frameworks
        }

        /// <summary>
        /// Initializes a new instance of the Cassandra.Row class
        /// </summary>
        [Obsolete("This constructor is deprecated and to be removed in future versions. " +
                  "If you need to create mock instances of Row, use the parameter-less constructor and override GetValue<T>()")]
        public Row(int protocolVersion, byte[][] values, CqlColumn[] columns, Dictionary<string, int> columnIndexes)
        {
            ProtocolVersion = protocolVersion;
            Values = values;
            Columns = columns;
            ColumnIndexes = columnIndexes;
        }

        internal Row(object[] values, CqlColumn[] columns, Dictionary<string, int> columnIndexes)
        {
            _rowValues = values;
            Columns = columns;
            ColumnIndexes = columnIndexes;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the row values from the first position to the last one.
        /// </summary>
        public IEnumerator<object> GetEnumerator()
        {
            return Columns.Select(c => GetValue(c.Type, c.Index)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            //.NET legacy enumerator
            return GetEnumerator();
        }

        /// <summary>
        /// Determines if the value in the column is null
        /// </summary>
        public bool IsNull(string name)
        {
            return IsNull(ColumnIndexes[name]);
        }

        /// <summary>
        /// Determines if the value in the column is null
        /// </summary>
        public bool IsNull(int idx)
        {
            return _rowValues[idx] == null;
        }

        /// <summary>
        /// Gets a column information by name. Returns null if not found.
        /// </summary>
        public CqlColumn GetColumn(string name)
        {
            int index;
            if (!ColumnIndexes.TryGetValue(name, out index))
            {
                return null;
            }
            return Columns[index];
        }

        /// <summary>
        /// Gets the stored value in the column specified by index
        /// </summary>
        /// <param name="type">Target type</param>
        /// <param name="index">Index of the column</param>
        /// <returns></returns>
        public object GetValue(Type type, int index)
        {
            var value = _rowValues[index];
            return value == null ? null : TryConvertToType(value, Columns[index], type);
        }

        /// <summary>
        /// Gets the stored value in the column specified by name
        /// </summary>
        /// <param name="type">Target type</param>
        /// <param name="name">Name of the column</param>
        /// <returns></returns>
        public object GetValue(Type type, string name)
        {
            return GetValue(type, ColumnIndexes[name]);
        }


        /// <summary>
        /// Gets the stored value in the column specified by index.
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="index">Index of the column</param>
        /// <returns></returns>
        public virtual T GetValue<T>(int index)
        {
            //The method is marked virtual to allow to be mocked
            var type = typeof (T);
            var value = GetValue(type, index);
            //Check that the value is null but the type is not nullable (structs)
            //A little overhead in case of misuse but improved Error message
            if (value == null && default(T) != null)
            {
                throw new NullReferenceException(String.Format("Cannot convert null to {0} because it is a value type, try using Nullable<{0}>", type.Name));
            }
            return (T) value;
        }

        /// <summary>
        /// Gets the stored value in the column specified by name.
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="name">Name of the column</param>
        /// <returns></returns>
        public virtual T GetValue<T>(string name)
        {
            //The method is marked virtual to allow to be mocked
            if (!ColumnIndexes.ContainsKey(name))
            {
                throw new ArgumentException(String.Format("Column {0} not found", name));
            }
            return GetValue<T>(ColumnIndexes[name]);
        }

        /// <summary>
        /// Handle conversions for some types that, for backward compatibility,
        /// the result type can be more than 1 depending on the type provided by the user 
        /// </summary>
        internal static object TryConvertToType(object value, ColumnDesc column, Type targetType)
        {
            if (value == null)
            {
                return null;
            }
            switch (column.TypeCode)
            {
                case ColumnTypeCode.List:
                {
                    //value is an array, according to TypeCodec
                    if (targetType.IsArray || targetType == typeof(object) || Utils.IsIEnumerable(targetType))
                    {
                        //Return the underlying array
                        return value;
                    }
                    var childTypeInfo = (ListColumnInfo) column.TypeInfo;
                    return Utils.ToCollectionType(typeof(List<>), TypeCodec.GetDefaultTypeFromCqlType(childTypeInfo.ValueTypeCode, childTypeInfo.ValueTypeInfo), (Array)value);
                }
                case ColumnTypeCode.Set:
                {
                    //value is an array, according to TypeCodec
                    if (targetType.IsArray || targetType == typeof(object) || Utils.IsIEnumerable(targetType))
                    {
                        //Return the underlying array
                        return value;
                    }
                    var childTypeInfo = (SetColumnInfo)column.TypeInfo;
                    var itemType = TypeCodec.GetDefaultTypeFromCqlType(childTypeInfo.KeyTypeCode, childTypeInfo.KeyTypeInfo);
                    if (targetType.IsGenericType)
                    {
                        var genericType = targetType.GetGenericTypeDefinition();
                        if (genericType == typeof(SortedSet<>))
                        {
                            return Utils.ToCollectionType(typeof(SortedSet<>), itemType, (Array)value);
                        }
                        if (genericType == typeof(HashSet<>))
                        {
                            return Utils.ToCollectionType(typeof(HashSet<>), itemType, (Array)value);
                        }
                    }
                    return Utils.ToCollectionType(typeof(List<>), itemType, (Array)value);
                }
                case ColumnTypeCode.Timestamp:
                    //value is a DateTimeOffset
                    if (targetType == typeof (object) || targetType == typeof (DateTimeOffset))
                    {
                        return value;
                    }
                    return ((DateTimeOffset)value).DateTime;
                case ColumnTypeCode.Timeuuid:
                    //Value is a Uuid
                    if (targetType == typeof (TimeUuid))
                    {
                        return (TimeUuid)(Guid)value;
                    }
                    return value;
                default:
                    return value;
            }
        }
    }
}
