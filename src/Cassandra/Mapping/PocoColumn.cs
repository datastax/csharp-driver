//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Reflection;

namespace Cassandra.Mapping
{
    internal class PocoColumn
    {
        /// <summary>
        /// The name of the column in the database.
        /// </summary>
        public string ColumnName { get; private set; }

        /// <summary>
        /// The data type of the column in the database for use when inserting/updating.
        /// </summary>
        public Type ColumnType { get; private set; }

        /// <summary>
        /// The MemberInfo for the POCO field/property.
        /// </summary>
        public MemberInfo MemberInfo { get; private set; }

        /// <summary>
        /// The .NET Type of the POCO field/property (i.e. FieldInfo.FieldType or PropertyInfo.PropertyType)
        /// </summary>
        public Type MemberInfoType { get; private set; }

        /// <summary>
        /// Determines that there is a secondary index defined for this column
        /// </summary>
        public bool SecondaryIndex { get; private set; }

        /// <summary>
        /// Determines that it is a counter column
        /// </summary>
        public bool IsCounter { get; private set; }

        /// <summary>
        /// Determines that it is a static column
        /// </summary>
        public bool IsStatic { get; private set; }

        /// <summary>
        /// Determines if the column is frozen.
        /// Only valid for collections, tuples, and user-defined types. For example: frozen&lt;address&gt;
        /// </summary>
        public bool IsFrozen { get; private set; }

        /// <summary>
        /// Determines if the key of the column type is frozen.
        /// </summary>
        public bool HasFrozenKey { get; private set; }

        /// <summary>
        /// Determines if the value of the column type is frozen.
        /// </summary>
        public bool HasFrozenValue { get; private set; }

        private PocoColumn()
        {
        }

        public static PocoColumn FromColumnDefinition(IColumnDefinition columnDefinition)
        {
            return new PocoColumn
            {
                // Default the column name to the prop/field name if not specified
                ColumnName = columnDefinition.ColumnName ?? columnDefinition.MemberInfo.Name,
                // Default the column type to the prop/field type if not specified
                ColumnType = columnDefinition.ColumnType ?? columnDefinition.MemberInfoType,
                MemberInfo = columnDefinition.MemberInfo,
                MemberInfoType = columnDefinition.MemberInfoType,
                SecondaryIndex = columnDefinition.SecondaryIndex,
                IsCounter = columnDefinition.IsCounter,
                IsStatic = columnDefinition.IsStatic,
                IsFrozen = columnDefinition.IsFrozen,
                HasFrozenKey = columnDefinition.HasFrozenKey,
                HasFrozenValue = columnDefinition.HasFrozenValue
            };
        }
    }
}