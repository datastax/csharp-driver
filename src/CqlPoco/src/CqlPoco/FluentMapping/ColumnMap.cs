using System;
using System.Reflection;
using CqlPoco.Mapping;

namespace CqlPoco.FluentMapping
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

        /// <summary>
        /// Creates a new ColumnMap for the property/field specified by the MemberInfo.
        /// </summary>
        public ColumnMap(MemberInfo memberInfo, Type memberInfoType, bool isExplicitlyDefined)
        {
            if (memberInfo == null) throw new ArgumentNullException("memberInfo");
            if (memberInfoType == null) throw new ArgumentNullException("memberInfoType");
            _memberInfo = memberInfo;
            _memberInfoType = memberInfoType;

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
            if (string.IsNullOrWhiteSpace(columnName)) throw new ArgumentNullException("columnName");

            _columnName = columnName;
            return this;
        }

        /// <summary>
        /// Tells the mapper to convert the data in the property or field to the Type specified when doing an INSERT or UPDATE (i.e. the
        /// column type in Cassandra).  (NOTE: This does NOT affect the Type when fetching/SELECTing data from the database.)
        /// </summary>
        public ColumnMap WithDbType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");

            _columnType = type;
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
    }
}