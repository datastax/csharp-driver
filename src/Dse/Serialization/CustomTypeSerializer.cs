//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Serialization
{
    /// <summary>
    /// Base serializer for custom types.
    /// </summary>
    public abstract class CustomTypeSerializer<T> : TypeSerializer<T>
    {
        private readonly IColumnInfo _typeInfo;

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        /// <summary>
        /// Creates a new instance of the serializer for custom types.
        /// </summary>
        /// <param name="name">Fully qualified name of the custom type</param>
        protected CustomTypeSerializer(string name)
        {
            _typeInfo = new CustomColumnInfo(name);
        }
    }
}
