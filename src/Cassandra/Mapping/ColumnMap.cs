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
using System.Reflection;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A class for defining how a property or field on a POCO is mapped to a column via a fluent-style interface.
    /// </summary>
    public class ColumnMap : IColumnDefinition
    {
        private readonly MemberInfo _memberInfo;
        private readonly Type _memberInfoType;
        private string _columnName;
        private Type _columnType;
        private bool _ignore;
        private readonly bool _isExplicitlyDefined;
        private bool _secondaryIndex;
        private bool _isCounter;
        private bool _isStatic;
        private bool _isFrozen;
        private bool _hasFrozenKey;
        private bool _hasFrozenValue;

        MemberInfo IColumnDefinition.MemberInfo
        {
            get { return _memberInfo; }
        }

        Type IColumnDefinition.MemberInfoType
        {
            get { return _memberInfoType; }
        }

        string IColumnDefinition.ColumnName
        {
            get { return _columnName; }
        }

        Type IColumnDefinition.ColumnType
        {
            get { return _columnType; }
        }

        bool IColumnDefinition.Ignore
        {
            get { return _ignore; }
        }

        bool IColumnDefinition.IsExplicitlyDefined
        {
            get { return _isExplicitlyDefined; }
        }

        bool IColumnDefinition.SecondaryIndex
        {
            get { return _secondaryIndex; }
        }

        bool IColumnDefinition.IsCounter
        {
            get { return _isCounter; }
        }

        bool IColumnDefinition.IsStatic
        {
            get { return _isStatic; }
        }

        bool IColumnDefinition.IsFrozen
        {
            get { return _isFrozen; }
        }

        bool IColumnDefinition.HasFrozenKey
        {
            get { return _hasFrozenKey; }
        }

        bool IColumnDefinition.HasFrozenValue
        {
            get { return _hasFrozenValue; }
        }

        /// <summary>
        /// Creates a new ColumnMap for the property/field specified by the MemberInfo.
        /// </summary>
        public ColumnMap(MemberInfo memberInfo, Type memberInfoType, bool isExplicitlyDefined)
        {
            _memberInfo = memberInfo ?? throw new ArgumentNullException("memberInfo");
            _memberInfoType = memberInfoType ?? throw new ArgumentNullException("memberInfoType");
            _isExplicitlyDefined = isExplicitlyDefined;
        }

        /// <summary>
        /// Tells the mapper to ignore this property/field when mapping.
        /// </summary>
        public ColumnMap Ignore()
        {
            _ignore = true;
            return this;
        }

        /// <summary>
        /// Tells the mapper to use the column name specified when mapping the property/field.
        /// </summary>
        public ColumnMap WithName(string columnName)
        {
            _columnName = columnName ?? throw new ArgumentNullException("columnName");
            return this;
        }

        /// <summary>
        /// Tells the mapper to convert the data in the property or field to the Type specified when doing an INSERT or UPDATE (i.e. the
        /// column type in Cassandra).  (NOTE: This does NOT affect the Type when fetching/SELECTing data from the database.)
        /// </summary>
        public ColumnMap WithDbType(Type type)
        {
            _columnType = type ?? throw new ArgumentNullException("type");
            return this;
        }

        /// <summary>
        /// Tells the mapper to convert the data in the property or field to Type T when doing an INSERT or UPDATE (i.e. the
        /// column type in Cassandra).  (NOTE: This does NOT affect the Type when fetching/SELECTing data from the database.)
        /// </summary>
        public ColumnMap WithDbType<T>()
        {
            _columnType = typeof (T);
            return this;
        }

        /// <summary>
        /// Tells the mapper that this column is defined also as a secondary index
        /// </summary>
        /// <returns></returns>
        public ColumnMap WithSecondaryIndex()
        {
            _secondaryIndex = true;
            return this;
        }

        /// <summary>
        /// Tells the mapper that this is a counter column
        /// </summary>
        public ColumnMap AsCounter()
        {
            _isCounter = true;
            return this;
        }

        /// <summary>
        /// Tells the mapper that this is a static column
        /// </summary>
        public ColumnMap AsStatic()
        {
            _isStatic = true;
            return this;
        }

        /// <summary>
        /// Tells the mapper that the column type is frozen.
        /// Only valid for collections, tuples, and user-defined types. For example: frozen&lt;address&gt;
        /// </summary>
        public ColumnMap AsFrozen()
        {
            _isFrozen = true;
            return this;
        }

        /// <summary>
        /// Tells the mapper that the key of the column type is frozen.
        /// Only valid for maps and sets, for example: map&lt;frozen&lt;tuple&lt;text, text&gt;&gt;, uuid&gt; .
        /// </summary>
        public ColumnMap WithFrozenKey()
        {
            _hasFrozenKey = true;
            return this;
        }

        /// <summary>
        /// Tells the mapper that the value of the column type is frozen.
        /// Only valid for maps and lists, for example: map&lt;uuid, frozen&lt;tuple&lt;text, text&gt;&gt;&gt; .
        /// </summary>
        public ColumnMap WithFrozenValue()
        {
            _hasFrozenValue = true;
            return this;
        }
    }
}