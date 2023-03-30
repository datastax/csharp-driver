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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Cassandra
{
    internal static class Utils
    {
        public static string ConvertToCqlMap(IDictionary<string, string> source)
        {
            var sb = new StringBuilder("{");
            if (source.Count > 0)
            {
                int counter = 0;
                foreach (KeyValuePair<string, string> elem in source)
                {
                    counter++;
                    sb.Append("'" + elem.Key + "'" + " : " + "'" + elem.Value + "'" + ((source.Count != counter) ? ", " : "}"));
                    //sb.Append("'" + elem.Key + "'" + " : " + (elem.Key == "class" ? "'" + elem.Value + "'" : elem.Value) + ((source.Count != counter) ? ", " : "}"));
                }
            }
            else sb.Append("}");

            return sb.ToString();
        }

        public static IDictionary<string, string> ConvertStringToMap(string source)
        {
            string[] elements = source.Replace("{\"", "").Replace("\"}", "").Replace("\"\"", "\"").Replace("\":", ":").Split(',');
            var map = new SortedDictionary<string, string>();

            if (source != "{}")
                foreach (string elem in elements)
                    map.Add(elem.Split(':')[0].Replace("\"", ""), elem.Split(':')[1].Replace("\"", ""));

            return map;
        }

        public static IDictionary<string, int> ConvertStringToMapInt(string source)
        {
            var elements = source.Replace("{\"", "").Replace("\"}", "").Replace("\"\"", "\"").Replace("\":", ":").Split(',');
            var map = new SortedDictionary<string, int>();

            if (source != "{}")
            { 
                foreach (string elem in elements)
                {
                    if (int.TryParse(elem.Split(':')[1].Replace("\"", ""), out int value))
                        map.Add(elem.Split(':')[0].Replace("\"", ""), value);
                    else
                        throw new FormatException("Value of keyspace strategy option is in invalid format!");
                }
            }
            return map;
        }

        /// <summary>
        /// Performs a getnameinfo() call and returns the primary host name
        /// </summary>
        public static string GetPrimaryHostNameInfo(string address)
        {
            var hostEntry = Dns.GetHostEntry(address);
            return hostEntry.HostName;
        }

        public static bool CompareIDictionary<TKey, TValue>(IDictionary<TKey, TValue> dict1, IDictionary<TKey, TValue> dict2)
        {
            if (dict1 == dict2) return true;
            if ((dict1 == null) || (dict2 == null)) return false;
            if (dict1.Count != dict2.Count) return false;

            EqualityComparer<TValue> comp = EqualityComparer<TValue>.Default;

            foreach (KeyValuePair<TKey, TValue> kvp in dict1)
            {
                if (!dict2.TryGetValue(kvp.Key, out TValue value2))
                    return false;
                if (!comp.Equals(kvp.Value, value2))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Returns a new byte array that is the result of the sum of the 2 byte arrays: [1, 2] + [3, 4] = [1, 2, 3, 4]
        /// </summary>
        public static byte[] JoinBuffers(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            if (buffer1 == null)
            {
                return CopyBuffer(buffer2);
            }
            if (buffer2 == null)
            {
                return CopyBuffer(buffer1);
            }
            var newBuffer = new byte[count1 + count2];
            Buffer.BlockCopy(buffer1, offset1, newBuffer, 0, count1);
            Buffer.BlockCopy(buffer2, offset2, newBuffer, count1, count2);
            return newBuffer;
        }

        /// <summary>
        /// Combines all the buffers in a new buffer.
        /// </summary>
        public static byte[] JoinBuffers(IEnumerable<byte[]> buffers, int totalLength)
        {
            var result = new byte[totalLength];
            var offset = 0;
            foreach (byte[] data in buffers)
            {
                Buffer.BlockCopy(data, 0, result, offset, data.Length);
                offset += data.Length;
            }
            return result;
        }

        /// <summary>
        /// Returns a new buffer as a slice of the provided buffer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="startIndex">zero-based index</param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static byte[] SliceBuffer(byte[] value, int startIndex, int count)
        {
            var newBuffer = new byte[count];
            Buffer.BlockCopy(value, startIndex, newBuffer, 0, count);
            return newBuffer;
        }

        /// <summary>
        /// Returns a new buffer as a slice of the provided buffer, if offset is greater than zero or count does not 
        /// match buffer length. Returns the same instance otherwise.
        /// </summary>
        /// <param name="value">The Buffer to slice</param>
        /// <param name="offset">zero-based index</param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static byte[] FromOffset(byte[] value, int offset, int count)
        {
            if (offset == 0 && value.Length == count)
            {
                return value;
            }
            return SliceBuffer(value, offset, count);
        }

        /// <summary>
        /// Returns a new buffer with the bytes copied from the source buffer
        /// </summary>
        public static byte[] CopyBuffer(byte[] buffer)
        {
            if (buffer == null)
            {
                return null;
            }
            return SliceBuffer(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Reads all the bytes in the stream from a given position
        /// </summary>
        public static byte[] ReadAllBytes(Stream stream, int position)
        {
            var buffer = new byte[stream.Length - position];
            stream.Position = position;
            stream.Read(buffer, 0, buffer.Length - position);
            return buffer;
        }

        /// <summary>
        /// Reads all the bytes in the stream from a given position
        /// </summary>
        public static byte[] ReadAllBytes(IEnumerable<Stream> streamList, long totalLength)
        {
            var buffer = new byte[totalLength];
            var offset = 0;
            foreach (var stream in streamList)
            {
                stream.Position = 0;
                var itemLength = (int)stream.Length;
                stream.Read(buffer, offset, itemLength);
                offset += itemLength;
            }
            return buffer;
        }

        /// <summary>
        /// Copies an stream using the provided buffer to copy chunks
        /// </summary>
        public static void CopyStream(Stream input, Stream output, int length, byte[] buffer)
        {
            int read;
            while (length > 0 && (read = input.Read(buffer, 0, Math.Min(buffer.Length, length))) > 0)
            {
                output.Write(buffer, 0, read);
                length -= read;
            }
        }

        /// <summary>
        /// Detects if the object is an instance of an anonymous type
        /// </summary>
        public static bool IsAnonymousType(object value)
        {
            if (value == null)
            {
                return false;
            }
            return IsAnonymousType(value.GetType());
        }

        /// <summary>
        /// Determines if the type is anonymous
        /// </summary>
        public static bool IsAnonymousType(Type type)
        {
            return type.GetTypeInfo().IsGenericType
                   && (type.GetTypeInfo().Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic
                   && (type.Name.Contains("AnonymousType") || type.Name.Contains("AnonType"))
                   && type.GetTypeInfo().IsDefined(typeof(CompilerGeneratedAttribute), false);
        }

        /// <summary>
        /// Gets the values of a given object in order given by the property names
        /// </summary>
        public static IEnumerable<object> GetValues(IEnumerable<string> propNames, object value)
        {
            if (value == null)
            {
                return new object[0];
            }
            var type = value.GetType();
            var valueList = new List<object>();
            const BindingFlags propFlags = BindingFlags.FlattenHierarchy | BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance;
            foreach (var name in propNames)
            {
                var prop = type.GetTypeInfo().GetProperty(name, propFlags);
                if (prop == null)
                {
                    valueList.Add(null);
                    continue;
                }
                valueList.Add(prop.GetValue(value, null));
            }
            return valueList;
        }

        /// <summary>
        /// Gets the properties and values of a given object
        /// </summary>
        public static IDictionary<string, object> GetValues(object value)
        {
            if (value == null)
            {
                return new Dictionary<string, object>(0);
            }
            var type = value.GetType();
            const BindingFlags propFlags = BindingFlags.FlattenHierarchy | BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance;
            var properties = type.GetTypeInfo().GetProperties(propFlags);
            var valueMap = new SortedList<string, object>(properties.Length);
            foreach (var prop in properties)
            {
                valueMap.Add(prop.Name, prop.GetValue(value, null));
            }
            return valueMap;
        }

        /// <summary>
        /// Combines the hash code based on the value of items.
        /// </summary>
        internal static int CombineHashCode<T>(IEnumerable<T> items)
        {
            unchecked
            {
                var hash = 17;
                foreach (var item in items)
                {
                    hash = hash * 23 + item.GetHashCode();
                }
                return hash;
            }
        }

        /// <summary>
        /// Combines the hash code based on the value of nullable items
        /// </summary>
        internal static int CombineHashCodeWithNulls<T>(IEnumerable<T> items)
        {
            unchecked
            {
                var hash = 17;
                foreach (var item in items)
                {
                    hash = hash * 23 + (item?.GetHashCode() ?? 0);
                }
                return hash;
            }
        }

        /// <summary>
        /// Combines the hash code based on the value of nullable items
        /// </summary>
        internal static int CombineHashCodeWithNulls(params object[] items)
        {
            return Utils.CombineHashCodeWithNulls<object>(items);
        }

        /// <summary>
        /// Returns true if the ConsistencyLevel is either <see cref="ConsistencyLevel.Serial"/> or <see cref="ConsistencyLevel.LocalSerial"/>,
        /// otherwise false.
        /// </summary>
        public static bool IsSerialConsistencyLevel(this ConsistencyLevel consistency)
        {
            return consistency == ConsistencyLevel.Serial || consistency == ConsistencyLevel.LocalSerial;
        }

        /// <summary>
        /// Creates a new instance of the collection type with the values provided
        /// </summary>
        public static object ToCollectionType(Type collectionType, Type valueType, Array value)
        {
            var listType = collectionType.MakeGenericType(valueType);
            return Activator.CreateInstance(listType, value);
        }

        /// <summary>
        /// Returns true if the type is IEnumerable{T} or implements IEnumerable{T}
        /// </summary>
        public static bool IsIEnumerable(Type t, out Type childType)
        {
            var typeInfo = t.GetTypeInfo();
            var isEnumerable = typeInfo.IsGenericType && typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);
            if (isEnumerable)
            {
                childType = typeInfo.GetGenericArguments()[0];
                return true;
            }
            var implementedEnumerable = typeInfo
                .GetInterfaces()
                .FirstOrDefault(
                    i => i.GetTypeInfo().IsGenericType &&
                    i.GetTypeInfo().GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (implementedEnumerable == null)
            {
                childType = null;
                return false;
            }
            childType = implementedEnumerable.GetTypeInfo().GetGenericArguments()[0];
            return true;
        }

        /// <summary>
        /// Returns true if the type is IDictionary{T} or implements IDictionary{T}
        /// </summary>
        public static bool IsIDictionary(Type t, out Type keyType, out Type valueType)
        {
            var typeInfo = t.GetTypeInfo();
            var isIDictionary = typeInfo.IsGenericType &&
                typeInfo.GetGenericTypeDefinition() == typeof(IDictionary<,>);
            Type[] subTypes;
            if (isIDictionary)
            {
                subTypes = typeInfo.GetGenericArguments();
                keyType = subTypes[0];
                valueType = subTypes[1];
                return true;
            }
            var implementedIDictionary = typeInfo
                .GetInterfaces()
                .FirstOrDefault(
                    i => i.GetTypeInfo().IsGenericType &&
                    i.GetTypeInfo().GetGenericTypeDefinition() == typeof(IDictionary<,>));
            if (implementedIDictionary == null)
            {
                keyType = null;
                valueType = null;
                return false;
            }
            subTypes = implementedIDictionary.GetTypeInfo().GetGenericArguments();
            keyType = subTypes[0];
            valueType = subTypes[1];
            return true;
        }

        public static bool IsTuple(Type type)
        {
            return typeof(IStructuralComparable).GetTypeInfo().IsAssignableFrom(type) &&
                type.FullName.StartsWith("System.Tuple");
        }

        /// <summary>
        /// Fill the zeros to the left
        /// </summary>
        public static string FillZeros(int value, int length = 2)
        {
            var textValue = value.ToString();

            return textValue.Length >= length
                 ? textValue
                 : textValue.PadLeft(length, '0');
        }

        public static string[] ParseJsonStringArray(string value)
        {
            if (value == null || value == "[]")
            {
                return new string[0];
            }
            var list = new List<string>();
            string currentItem = null;
            foreach (var c in value)
            {
                switch (c)
                {
                    case '[':
                    case ']':
                        continue;
                    case '"':
                        if (currentItem != null)
                        {
                            list.Add(currentItem);
                            currentItem = null;
                        }
                        else
                        {
                            currentItem = "";
                        }
                        continue;
                    case ' ':
                    case ',':
                        if (currentItem == null)
                        {
                            continue;
                        }
                        break;
                }
                currentItem += c;
            }
            return list.ToArray();
        }

        internal static IDictionary<string, string> ParseJsonStringMap(string jsonValue)
        {
            if (jsonValue == null)
            {
                return new Dictionary<string, string>(0);
            }
            var map = new Dictionary<string, string>();
            string key = null;
            string value = null;
            var isKey = false;
            foreach (var c in jsonValue)
            {
                switch (c)
                {
                    case '{':
                    case '}':
                        continue;
                    case '"':
                        if (key != null)
                        {
                            if (isKey)
                            {
                                //finish the key
                                isKey = false;
                                continue;
                            }
                            if (value == null)
                            {
                                //starting value
                                value = "";
                                continue;
                            }
                            //finished value
                            map.Add(key, value);
                            key = null;
                            value = null;
                        }
                        else
                        {
                            //starting key
                            key = "";
                            isKey = true;
                        }
                        continue;
                }
                if (value != null)
                {
                    value += c;
                }
                else if (isKey)
                {
                    key += c;
                }
            }
            return map;
        }
    }
}
