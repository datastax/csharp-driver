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
using System.Data;
using System.Data.Common;

namespace Cassandra.Data
{
    /// <summary>
    /// Represents a Cql parameter.
    /// </summary>
    public class CqlParameter : DbParameter
    {
        private string _name;
        private bool _isNullable;
        private object _value;

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        public CqlParameter()
        {
            _isNullable = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// </summary>
        /// <param name="name">The name.</param>
        public CqlParameter(string name)
            : this()
        {
            SetParameterName(name);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CqlParameter" /> class.
        /// The type of the parameter will be guessed from the value.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        public CqlParameter(string name, object value)
            : this(name)
        {
            _value = value;
        }

        #endregion

        #region DbParameter Members

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.DbType" /> of the parameter.
        /// </summary>
        public override DbType DbType { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter is 
        /// input-only, output-only, bidirectional, or a stored procedure return value parameter.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.ParameterDirection" /> values.
        /// The default is Input.
        /// </returns>
        /// <exception cref="System.NotSupportedException">Cql only supports input parameters</exception>
        public override ParameterDirection Direction
        {
            get { return ParameterDirection.Input; }
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Cql only supports input parameters");
            }
        }

        /// <summary>
        /// Gets a value indicating whether the parameter accepts null values.
        /// </summary>
        /// <returns>true if null values are accepted; otherwise, false. The default is false. </returns>
        public override bool IsNullable
        {
            get { return _isNullable; }
            set { _isNullable = value; }
        }

        /// <summary>
        /// Gets or sets the name of the <see cref="T:System.Data.IDataParameter" />.
        /// </summary>
        /// <returns>
        /// The name of the <see cref="T:System.Data.IDataParameter" />.
        /// The default is an empty string.
        /// </returns>
        public override string ParameterName
        {
            get { return _name; }
            set { SetParameterName(value); }
        }

        /// <summary>
        /// Gets or sets the name of the source column that is mapped
        /// to the <see cref="T:System.Data.DataSet" /> and used for loading or 
        /// returning the <see cref="P:System.Data.IDataParameter.Value" />.
        /// </summary>
        /// <returns>
        /// The name of the source column that is mapped to the <see cref="T:System.Data.DataSet" />.
        /// The default is an empty string.
        /// </returns>
        public override string SourceColumn { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.DataRowVersion" />
        /// to use when loading <see cref="P:System.Data.IDataParameter.Value" />.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.DataRowVersion" /> values.
        /// The default is Current.
        /// </returns>
        public override DataRowVersion SourceVersion { get; set; }

        /// <summary>
        /// Gets or sets the value of the parameter. 
        /// If no type information was provided earlier, the type of the parameter will be
        /// guessed from the value's type.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Object" /> that is the value of the parameter.
        /// The default value is null.
        /// </returns>
        public override object Value
        {
            get { return _value; }
            set { _value = value; }
        }

        /// <summary>
        /// The size of the parameter.
        /// </summary>
        /// <returns>Always returns 0.</returns>
        public override int Size
        {
            get { return 0; }
            set { }
        }

        /// <summary>
        /// Sets or gets a value which indicates whether the source column is nullable.
        /// This allows <see cref="T:System.Data.Common.DbCommandBuilder" /> 
        /// to correctly generate Update statements for nullable columns.
        /// </summary>
        /// <returns>true if the source column is nullable; false if it is not. </returns>
        public override bool SourceColumnNullMapping
        {
            get { return IsNullable; }
            set { IsNullable = value; }
        }

        /// <summary>
        /// Resets the DbType property to its original settings.
        /// </summary>
        public override void ResetDbType()
        {
        }

        #endregion

        #region Private Methods

        private void SetParameterName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            _name = name.StartsWith(":") ? name : ":" + name;
        }

        #endregion
    }

}
