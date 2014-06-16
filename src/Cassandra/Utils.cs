﻿//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace Cassandra
{
    internal static class Utils
    {
        public static long GetTimestampFromGuid(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();
            bytes[7] &= 0x0f;
            return BitConverter.ToInt64(bytes, 0);
        }

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
            string[] elements = source.Replace("{\"", "").Replace("\"}", "").Replace("\"\"", "\"").Replace("\":", ":").Split(',');
            var map = new SortedDictionary<string, int>();

            if (source != "{}")
                foreach (string elem in elements)
                {
                    int value;
                    if (int.TryParse(elem.Split(':')[1].Replace("\"", ""), out value))
                        map.Add(elem.Split(':')[0].Replace("\"", ""), value);
                    else
                        throw new FormatException("Value of keyspace strategy option is in invalid format!");
                }

            return map;
        }

        public static IEnumerable<IPAddress> ResolveHostByName(string address)
        {
            IPAddress addr;
            if (IPAddress.TryParse(address, out addr))
            {
                return new List<IPAddress> {addr};
            }
            IPHostEntry hst = Dns.GetHostEntry(address);
            return hst.AddressList;
        }

        public static bool CompareIDictionary<TKey, TValue>(IDictionary<TKey, TValue> dict1, IDictionary<TKey, TValue> dict2)
        {
            if (dict1 == dict2) return true;
            if ((dict1 == null) || (dict2 == null)) return false;
            if (dict1.Count != dict2.Count) return false;

            EqualityComparer<TValue> comp = EqualityComparer<TValue>.Default;

            foreach (KeyValuePair<TKey, TValue> kvp in dict1)
            {
                TValue value2;
                if (!dict2.TryGetValue(kvp.Key, out value2))
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
            stream.Read(buffer, position, buffer.Length - position);
            return buffer;
        }
    }
}