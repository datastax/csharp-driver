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
using System.Reflection;

namespace Cassandra.Mapping.Attributes
{
    /// <summary>
    /// A column definition that uses attributes on the field/property to get its settings.
    /// </summary>
    internal class AttributeBasedColumnDefinition : IColumnDefinition
    {
        private readonly MemberInfo _memberInfo;
        private readonly Type _memberInfoType;
        private readonly string _columnName;
        private readonly Type _columnType;
        private readonly bool _ignore;
        private readonly bool _isExplicitlyDefined;
        private readonly bool _secondaryIndex;
        private readonly bool _isCounter;
        private readonly bool _isStatic;
        private readonly bool _isFrozen;
        private readonly bool _hasFrozenKey;
        private readonly bool _hasFrozenValue;

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
        /// Creates a new column definition for the field specified using any attributes on the field to determine mapping configuration.
        /// </summary>
        public AttributeBasedColumnDefinition(FieldInfo fieldInfo) 
            : this((MemberInfo) fieldInfo)
        {
            _memberInfoType = fieldInfo.FieldType;
        }

        /// <summary>
        /// Creates a new column definition for the property specified using any attributes on the property to determine mapping configuration.
        /// </summary>
        public AttributeBasedColumnDefinition(PropertyInfo propertyInfo) 
            : this((MemberInfo) propertyInfo)
        {
            _memberInfoType = propertyInfo.PropertyType;
        }

        private AttributeBasedColumnDefinition(MemberInfo memberInfo)
        {
            _memberInfo = memberInfo;

            var columnAttribute = (ColumnAttribute) memberInfo.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();
            if (columnAttribute != null)
            {
                _isExplicitlyDefined = true;
                if (columnAttribute.Name != null)
                {
                    _columnName = columnAttribute.Name;
                }
                if (columnAttribute.Type != null)
                {
                    _columnType = columnAttribute.Type;
                }
            }
            // Column names can be also defined at ClusteringKeyAttribute level
            var clusteringKeyAttribute = (ClusteringKeyAttribute)memberInfo.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault();
            if (clusteringKeyAttribute != null && clusteringKeyAttribute.Name != null)
            {
                // Validation of columnAttribute.Name vs clusteringKeyAttribute.Name is already
                // considered at AttributeBasedTypeDefinition level
                _columnName = clusteringKeyAttribute.Name;
            }
            _ignore = HasAttribute(memberInfo, typeof(IgnoreAttribute));
            _secondaryIndex = HasAttribute(memberInfo, typeof(SecondaryIndexAttribute));
            _isStatic = HasAttribute(memberInfo, typeof(StaticColumnAttribute));
            _isCounter = HasAttribute(memberInfo, typeof(CounterAttribute));
            _isFrozen = HasAttribute(memberInfo, typeof(FrozenAttribute));
            _hasFrozenKey = HasAttribute(memberInfo, typeof(FrozenKeyAttribute));
            _hasFrozenValue = HasAttribute(memberInfo, typeof(FrozenValueAttribute));
        }

        /// <summary>
        /// Determines if the member has an attribute applied
        /// </summary>
        private static bool HasAttribute(MemberInfo memberInfo, Type attributeType)
        {
            return memberInfo.GetCustomAttributes(attributeType, true).FirstOrDefault() != null;
        }
    }
}