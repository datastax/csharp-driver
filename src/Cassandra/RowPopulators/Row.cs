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
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Cassandra.Mapping.TypeConversion;

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    /// <summary>
    /// Represents an CQL row
    /// </summary>
    public class Row : IEnumerable<object>, IRow
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
        public virtual bool IsNull(int index)
        {
            return _rowValues[index] == null;
        }

        /// <summary>
        /// Gets a column information by name. Returns null if not found.
        /// </summary>
        public CqlColumn GetColumn(string name)
        {
            return ColumnIndexes.TryGetValue(name, out var index)
                ? Columns[index]
                : null;
        }

        /// <summary>
        /// Returns true if the row contains information of the provided column name.
        /// </summary>
        bool IRow.ContainsColumn(string name)
        {
            return ContainsColumn(name);
        }

        /// <summary>
        /// Returns true if the row contains information of the provided column name.
        /// </summary>
        internal bool ContainsColumn(string name)
        {
            return GetColumn(name) != null;
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
            var type = typeof(T);
            var value = GetValue(type, index);
            //Check that the value is null but the type is not nullable (structs)
            //A little overhead in case of misuse but improved Error message
            if (value == null && default(T) != null)
            {
                throw new NullReferenceException(string.Format("Cannot convert null to {0} because it is a value type, try using Nullable<{0}>", type.Name));
            }
            return (T)value;
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
                throw new ArgumentException(string.Format("Column {0} not found", name));
            }
            return GetValue<T>(ColumnIndexes[name]);
        }

        /// <summary>
        /// Handle conversions for some types that, for backward compatibility,
        /// the result type can be more than 1 depending on the type provided by the user 
        /// </summary>
        internal static object TryConvertToType(object value, ColumnDesc column, Type targetType)
        {
            if (value == null || targetType == typeof(object))
            {
                return value;
            }
            switch (column.TypeCode)
            {
                case ColumnTypeCode.List:
                case ColumnTypeCode.Set:
                    return TryConvertToCollection(value, column, targetType);
                case ColumnTypeCode.Map:
                    return TryConvertDictionary((IDictionary)value, column, targetType);
                case ColumnTypeCode.Timestamp:
                    // The type of the value is DateTimeOffset
                    if (targetType == typeof(object) || targetType.GetTypeInfo().IsAssignableFrom(typeof(DateTimeOffset)))
                    {
                        return value;
                    }
                    return ((DateTimeOffset)value).DateTime;
                case ColumnTypeCode.Timeuuid:
                    // The type of the value is a Uuid
                    if (targetType.GetTypeInfo().IsAssignableFrom(typeof(TimeUuid)) && !(value is TimeUuid))
                    {
                        return (TimeUuid)(Guid)value;
                    }
                    return value;
                default:
                    return value;
            }
        }

        private static object TryConvertToCollection(object value, ColumnDesc column, Type targetType)
        {
            var targetTypeInfo = targetType.GetTypeInfo();
            // value is an array, according to TypeCodec
            var childType = value.GetType().GetTypeInfo().GetElementType();
            Type childTargetType;
            if (targetTypeInfo.IsArray)
            {
                childTargetType = targetTypeInfo.GetElementType();
                return childTargetType == childType
                    ? value
                    : Row.GetArray((Array)value, childTargetType, column.TypeInfo);
            }
            if (Utils.IsIEnumerable(targetType, out childTargetType))
            {
                var genericTargetType = targetType.GetGenericTypeDefinition();
                // Is IEnumerable
                if (childTargetType != childType)
                {
                    // Conversion is needed
                    value = Row.GetArray((Array)value, childTargetType, column.TypeInfo);
                }
                if (genericTargetType == typeof(IEnumerable<>))
                {
                    // The target type is an interface
                    return value;
                }
                if (column.TypeCode == ColumnTypeCode.List
                    || genericTargetType == typeof(List<>)
                    || TypeConverter.ListGenericInterfaces.Contains(genericTargetType))
                {
                    // Use List<T> by default when a list is expected and the target type 
                    // is not an object or an array
                    return Utils.ToCollectionType(typeof(List<>), childTargetType, (Array)value);
                }
                if (genericTargetType == typeof(SortedSet<>) || genericTargetType == typeof(ISet<>))
                {
                    return Utils.ToCollectionType(typeof(SortedSet<>), childTargetType, (Array)value);
                }
                if (genericTargetType == typeof(HashSet<>))
                {
                    return Utils.ToCollectionType(typeof(HashSet<>), childTargetType, (Array)value);
                }
            }
            throw new InvalidCastException(string.Format("Unable to cast object of type '{0}' to type '{1}'",
                value.GetType(), targetType));
        }

        private static Array GetArray(Array source, Type childTargetType, IColumnInfo columnInfo)
        {
            // Handle struct type cases manually to prevent unboxing and boxing again
            if (childTargetType.GetTypeInfo().IsAssignableFrom(typeof(TimeUuid)))
            {
                var arrSource = (Guid[])source;
                var result = new TimeUuid[source.Length];
                for (var i = 0; i < arrSource.Length; i++)
                {
                    result[i] = arrSource[i];
                }
                return result;
            }
            if (childTargetType.GetTypeInfo().IsAssignableFrom(typeof(DateTime)))
            {
                var arrSource = (DateTimeOffset[])source;
                var result = new DateTime[source.Length];
                for (var i = 0; i < arrSource.Length; i++)
                {
                    result[i] = arrSource[i].DateTime;
                }
                return result;
            }
            // Other collections
            var childColumnInfo = ((ICollectionColumnInfo)columnInfo).GetChildType();
            var arr = Array.CreateInstance(childTargetType, source.Length);
            bool? isNullable = null;
            for (var i = 0; i < source.Length; i++)
            {
                var value = source.GetValue(i);
                if (value == null)
                {
                    if (isNullable == null)
                    {
                        isNullable = !childTargetType.GetTypeInfo().IsValueType;
                    }

                    if (!isNullable.Value)
                    {
                        var nullableType = typeof(Nullable<>).MakeGenericType(childTargetType);
                        var newResult = Array.CreateInstance(nullableType, source.Length);
                        for (var j = 0; j < i; j++)
                        {
                            newResult.SetValue(arr.GetValue(j), j);
                        }
                        arr = newResult;
                        childTargetType = nullableType;
                        isNullable = true;
                    }
                }

                arr.SetValue(TryConvertToType(source.GetValue(i), childColumnInfo, childTargetType), i);
            }
            return arr;
        }

        private static IDictionary TryConvertDictionary(IDictionary value, ColumnDesc column, Type targetType)
        {
            if (targetType.GetTypeInfo().IsInstanceOfType(value))
            {
                return value;
            }
            var mapColumnInfo = (MapColumnInfo)column.TypeInfo;
            if (!Utils.IsIDictionary(targetType, out Type childTargetKeyType, out Type childTargetValueType))
            {
                throw new InvalidCastException(string.Format("Unable to cast object of type '{0}' to type '{1}'",
                    value.GetType(), targetType));
            }
            var childTypes = value.GetType().GetTypeInfo().GetGenericArguments();
            if (!childTargetKeyType.GetTypeInfo().IsAssignableFrom(childTypes[0]) ||
                !childTargetValueType.GetTypeInfo().IsAssignableFrom(childTypes[1]))
            {
                // Convert to target type
                var type = typeof(SortedDictionary<,>).MakeGenericType(childTargetKeyType, childTargetValueType);
                var result = (IDictionary)Activator.CreateInstance(type);
                foreach (DictionaryEntry entry in value)
                {
                    result.Add(
                        TryConvertToType(entry.Key, new ColumnDesc
                        {
                            TypeCode = mapColumnInfo.KeyTypeCode,
                            TypeInfo = mapColumnInfo.KeyTypeInfo
                        }, childTargetKeyType),
                        TryConvertToType(entry.Value, new ColumnDesc
                        {
                            TypeCode = mapColumnInfo.ValueTypeCode,
                            TypeInfo = mapColumnInfo.ValueTypeInfo
                        }, childTargetValueType));
                }
                return result;
            }
            return value;
        }
    }

    /// <summary>
    /// Internal representation of a Row
    /// </summary>
    internal interface IRow
    {
        T GetValue<T>(string name);

        bool ContainsColumn(string name);

        bool IsNull(string name);

        T GetValue<T>(int index);

        CqlColumn GetColumn(string name);
    }
}
