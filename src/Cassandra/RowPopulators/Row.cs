//
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
using System.Linq;
using System.Collections;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Represents an CQL row
    /// </summary>
    public class Row : IEnumerable<object>
    {
        /// <summary>
        /// Gets or sets the index of the columns within the row
        /// </summary>
        protected virtual Dictionary<string, int> ColumnIndexes { get; set; }

        /// <summary>
        /// Gets or sets the columns information
        /// </summary>
        protected virtual CqlColumn[] Columns { get; set; }

        protected virtual byte[][] Values { get; set; }

        /// <summary>
        /// Gets the total amount of values inside the row
        /// </summary>
        public int Length
        {
            get { return Values.Length; }
        }

        /// <summary>
        /// Gets the stored value in the colum specified by index
        /// </summary>
        public object this[int index]
        {
            get { return GetValue(typeof(object), index); }
        }

        /// <summary>
        /// Gets the stored value in the colum specified by name
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
        public Row(byte[][] values, CqlColumn[] columns, Dictionary<string, int> columnIndexes)
        {
            Values = values;
            Columns = columns;
            ColumnIndexes = columnIndexes;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the row values from the first position to the last one.
        /// </summary>
        public IEnumerator<object> GetEnumerator()
        {
            return Columns.Select(c => this.GetValue(c.Type, c.Index)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            //.NET legacy enumerator
            return this.GetEnumerator();
        }

        /// <summary>
        /// Determines if the value in the column is null
        /// </summary>
        public bool IsNull(string name)
        {
            return Values[ColumnIndexes[name]] == null;
        }

        /// <summary>
        /// Determines if the value in the column is null
        /// </summary>
        public bool IsNull(int idx)
        {
            return Values[idx] == null;
        }

        /// <summary>
        /// Gets the stored value in the colum specified by index
        /// </summary>
        /// <param name="tpy">Target type</param>
        /// <param name="index">Index of the column</param>
        /// <returns></returns>
        public object GetValue(Type tpy, int index)
        {
            return (Values[index] == null ? null : ConvertToObject(index, Values[index], tpy));
        }

        /// <summary>
        /// Gets the stored value in the colum specified by name
        /// </summary>
        /// <param name="tpy">Target type</param>
        /// <param name="name">Name of the column</param>
        /// <returns></returns>
        public object GetValue(Type tpy, string name)
        {
            return GetValue(tpy, ColumnIndexes[name]);
        }


        /// <summary>
        /// Gets the stored value in the colum specified by index
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="index">Index of the column</param>
        /// <returns></returns>
        public T GetValue<T>(int index)
        {
            return (T)GetValue(typeof(T), index);
        }

        /// <summary>
        /// Gets the stored value in the colum specified by name
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="name">Name of the column</param>
        /// <returns></returns>
        public T GetValue<T>(string name)
        {
            return GetValue<T>(ColumnIndexes[name]);
        }

        internal object ConvertToObject(int i, byte[] buffer, Type cSharpType = null)
        {
            return TypeInterpreter.CqlConvert(buffer, Columns[i].TypeCode, Columns[i].TypeInfo, cSharpType);
        }
    }
}