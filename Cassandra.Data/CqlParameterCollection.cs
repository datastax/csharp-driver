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
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Cassandra.Data
{
    /// <summary>
    /// Represents a collection of <see cref="CqlParameter"/>s.
    /// </summary>
    public class CqlParameterCollection : DbParameterCollection
    {
        private readonly List<CqlParameter> _parameters = new List<CqlParameter>();
        private readonly object _syncLock = new object();

        #region DbParameterCollection Members

        /// <summary>
        /// Specifies the number of items in the collection.
        /// </summary>
        /// <returns>The number of items in the collection.</returns>
        public override int Count
        {
            get { return _parameters.Count; }
        }

        /// <summary>
        /// Specifies the <see cref="T:System.Object" /> to be used to synchronize access to the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Object" /> to be used to synchronize access 
        /// to the <see cref="T:System.Data.Common.DbParameterCollection" />.
        /// </returns>
        public override object SyncRoot
        {
            get { return _syncLock; }
        }

        /// <summary>
        /// Specifies whether the collection is a fixed size.
        /// </summary>
        /// <returns>true if the collection is a fixed size; otherwise false.</returns>
        public override bool IsFixedSize
        {
            get { return IsReadOnly; }
        }

        /// <summary>
        /// Specifies whether the collection is read-only.
        /// </summary>
        /// <returns>true if the collection is read-only; otherwise false.</returns>
        public override bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Specifies whether the collection is synchronized.
        /// </summary>
        /// <returns>true if the collection is synchronized; otherwise false.</returns>
        public override bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Adds the specified <see cref="T:System.Data.Common.DbParameter" /> object
        /// to the <see cref="T:System.Data.Common.DbParameterCollection" />.
        /// </summary>
        /// <param name="value">
        /// The <see cref="P:System.Data.Common.DbParameter.Value" />
        /// of the <see cref="T:System.Data.Common.DbParameter" /> to add to the collection.
        /// </param>
        /// <returns>
        /// The index of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection. 
        /// </returns>
        public override int Add(object value)
        {
            return Add((CqlParameter)value);
        }

        /// <summary>
        /// Adds the specified parameter.
        /// </summary>
        /// <param name="parameter">The parameter.</param>
        /// <returns> </returns>
        public int Add(CqlParameter parameter)
        {
            _parameters.Add(parameter);
            return _parameters.Count - 1;
        }

        /// <summary>
        /// Adds a new parameter with the specified name and value. The name will be
        /// parsed to extract table and keyspace information (if any). The parameter type
        /// will be guessed from the object value.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        /// <returns>The created <see cref="CqlParameter"/>.</returns>
        public CqlParameter Add(string name, object value)
        {
            var parameter = new CqlParameter(name, value);
            Add(parameter);
            return parameter;
        }

        /// <summary>
        /// Indicates whether a <see cref="T:System.Data.Common.DbParameter" /> 
        /// with the specified <see cref="P:System.Data.Common.DbParameter.Value" /> 
        /// is contained in the collection.
        /// </summary>
        /// <param name="value">
        /// The <see cref="P:System.Data.Common.DbParameter.Value" /> 
        /// of the <see cref="T:System.Data.Common.DbParameter" /> to look for in the collection.
        /// </param>
        /// <returns>
        /// true if the <see cref="T:System.Data.Common.DbParameter" /> is in the collection; otherwise false.
        /// </returns>
        public override bool Contains(object value)
        {
            return _parameters.Contains((CqlParameter)value);
        }

        /// <summary>
        /// Removes all <see cref="T:System.Data.Common.DbParameter" /> values
        /// from the <see cref="T:System.Data.Common.DbParameterCollection" />.
        /// </summary>
        public override void Clear()
        {
            _parameters.Clear();
        }

        /// <summary>
        /// Returns the index of the specified <see cref="T:System.Data.Common.DbParameter" /> object.
        /// </summary>
        /// <param name="value">The <see cref="T:System.Data.Common.DbParameter" /> object in the collection.</param>
        /// <returns>The index of the specified <see cref="T:System.Data.Common.DbParameter" /> object.</returns>
        public override int IndexOf(object value)
        {
            return _parameters.IndexOf((CqlParameter)value);
        }

        /// <summary>
        /// Inserts the specified index of the <see cref="T:System.Data.Common.DbParameter" /> object
        /// with the specified name into the collection at the specified index.
        /// </summary>
        /// <param name="index">The index at which to insert the <see cref="T:System.Data.Common.DbParameter" /> object.</param>
        /// <param name="value">The <see cref="T:System.Data.Common.DbParameter" /> object to insert into the collection.</param>
        public override void Insert(int index, object value)
        {
            var param = (CqlParameter)value;
            _parameters.Insert(index, param);
        }

        /// <summary>
        /// Removes the specified <see cref="T:System.Data.Common.DbParameter" /> object from the collection.
        /// </summary>
        /// <param name="value">The <see cref="T:System.Data.Common.DbParameter" /> object to remove.</param>
        public override void Remove(object value)
        {
            var param = (CqlParameter)value;
            _parameters.Remove(param);
        }

        /// <summary>
        /// Removes the <see cref="T:System.Data.Common.DbParameter" /> object at the specified from the collection.
        /// </summary>
        /// <param name="index">
        /// The index where the <see cref="T:System.Data.Common.DbParameter" /> object is located.
        /// </param>
        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        /// <summary>
        /// Removes the <see cref="T:System.Data.Common.DbParameter" /> object 
        /// with the specified name from the collection.
        /// </summary>
        /// <param name="parameterName">
        /// The name of the <see cref="T:System.Data.Common.DbParameter" /> object to remove.
        /// </param>
        public override void RemoveAt(string parameterName)
        {
            int index = GetIndex(parameterName);
            _parameters.RemoveAt(index);
        }

        /// <summary>
        /// Sets the <see cref="T:System.Data.Common.DbParameter" /> object 
        /// at the specified index to a new value.
        /// </summary>
        /// <param name="index">
        /// The index where the <see cref="T:System.Data.Common.DbParameter" /> objectis located. 
        /// </param>
        /// <param name="value">The new <see cref="T:System.Data.Common.DbParameter" /> value.</param>
        protected override void SetParameter(int index, DbParameter value)
        {
            SetParameter(index, (CqlParameter)value);
        }

        /// <summary>
        /// Sets the <see cref="T:System.Data.Common.DbParameter" /> object
        /// with the specified name to a new value.
        /// </summary>
        /// <param name="parameterName">
        /// The name of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection.
        /// </param>
        /// <param name="value">The new <see cref="T:System.Data.Common.DbParameter" /> value.</param>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            SetParameter(parameterName, (CqlParameter)value);
        }

        /// <summary>
        /// Returns the index of the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name.
        /// </summary>
        /// <returns>
        /// <param name="parameterName">
        /// The name of the <see cref="T:System.Data.Common.DbParameter" /> object in the collection.
        /// </param>
        /// The index of the <see cref="T:System.Data.Common.DbParameter" /> object with the specified name.
        /// </returns>
        public override int IndexOf(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException("parameterName");

            var name = parameterName.StartsWith(":") ? parameterName : ":" + parameterName;
            return _parameters.FindIndex(p => p.ParameterName == name);
        }

        /// <summary>
        /// Exposes the <see cref="M:System.Collections.IEnumerable.GetEnumerator" /> method, 
        /// which supports a simple iteration over a collection by a .NET Framework data provider.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator" /> that can be used 
        /// to iterate through the collection.
        /// </returns>
        public override IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        /// <summary>
        /// Returns the <see cref="T:System.Data.Common.DbParameter" /> object at the specified index in the collection.
        /// </summary>
        /// <param name="index">
        /// The index of the <see cref="T:System.Data.Common.DbParameter" />in the collection.
        /// </param>
        /// <returns>
        /// The <see cref="T:System.Data.Common.DbParameter" /> object 
        /// at the specified index in the collection.
        /// </returns>
        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        /// <summary>
        /// Returns <see cref="T:System.Data.Common.DbParameter" /> the object with the specified name.
        /// </summary>
        /// <param name="parameterName">
        /// The name of the <see cref="T:System.Data.Common.DbParameter" /> in the collection.
        /// </param>
        /// <returns>The <see cref="T:System.Data.Common.DbParameter" /> the object with the specified name. </returns>
        protected override DbParameter GetParameter(string parameterName)
        {
            return GetCqlParameter(parameterName);
        }

        /// <summary>
        /// Indicates whether a <see cref="T:System.Data.Common.DbParameter" />
        /// with the specified name exists in the collection.
        /// </summary>
        /// <param name="value">
        /// The name of the <see cref="T:System.Data.Common.DbParameter" /> 
        /// to look for in the collection.
        /// </param>
        /// <returns>
        /// true if the <see cref="T:System.Data.Common.DbParameter" /> is
        /// in the collection; otherwise false.
        /// </returns>
        public override bool Contains(string value)
        {
            return IndexOf(value) > 0;
        }

        /// <summary>
        /// Copies an array of items to the collection starting at the specified index.
        /// </summary>
        /// <param name="array">The array of items to copy to the collection.</param>
        /// <param name="index">The index in the collection to copy the items.</param>
        public override void CopyTo(Array array, int index)
        {
            if (array == null)
                throw new ArgumentNullException("array");

            var c = (ICollection)_parameters;
            c.CopyTo(array, index);
        }

        /// <summary>
        /// Adds an array of items with the specified values 
        /// to the <see cref="T:System.Data.Common.DbParameterCollection" />.
        /// </summary>
        /// <param name="values">
        /// An array of values of type <see cref="T:System.Data.Common.DbParameter" /> 
        /// to add to the collection. 
        /// </param>
        public override void AddRange(Array values)
        {
            if (values == null)
                throw new ArgumentNullException("values");

            foreach (object obj in values)
            {
                if (!(obj is CqlParameter))
                    throw new ArgumentException("All values must be CqlParameter instances");
            }

            foreach (CqlParameter cqlParameter in values)
            {
                _parameters.Add(cqlParameter);
            }
        }

        #endregion

        #region Private Methods

        private void SetParameter(string parameterName, CqlParameter value)
        {
            int index = GetIndex(parameterName);
            _parameters[index] = value;
        }

        private void SetParameter(int index, CqlParameter value)
        {
            _parameters[index] = value;
        }

        private int GetIndex(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index < 0)
                throw new IndexOutOfRangeException("Parameter with the given name is not found");

            return index;
        }

        private CqlParameter GetCqlParameter(string parameterName)
        {
            int index = GetIndex(parameterName);
            return _parameters[index];
        }

        #endregion
    }
}
