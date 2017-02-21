using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Serialization
{
    /// <summary>
    /// Contains <see cref="TypeSerializer{T}"/> definitions for the driver to use, replacing the default ones.
    /// </summary>
    public class TypeSerializerDefinitions
    {
        private readonly ICollection<ITypeSerializer> _definitions = new List<ITypeSerializer>();

        internal ICollection<ITypeSerializer> Definitions
        {
            get { return _definitions; }
        }

        /// <summary>
        /// Defines a new <see cref="TypeSerializer{T}"/> instance to use.
        /// <para>
        /// If you try to define a serializer for a type that was already defined, the driver will log a warning and
        /// ignore it.
        /// </para>
        /// </summary>
        /// <exception cref="ArgumentNullException" />
        /// <exception cref="InvalidTypeException">When trying to define a Serializer for types that are not allowed.</exception>
        public TypeSerializerDefinitions Define<T>(TypeSerializer<T> typeSerializer)
        {
            if (typeSerializer == null)
            {
                throw new ArgumentNullException("typeSerializer");
            }
            switch (typeSerializer.CqlType)
            {
                case ColumnTypeCode.List:
                case ColumnTypeCode.Set:
                case ColumnTypeCode.Map:
                case ColumnTypeCode.Tuple:
                    throw new InvalidTypeException("Collection");
                case ColumnTypeCode.Udt:
                    if (!(typeSerializer is UdtSerializer))
                    {
                        throw new InvalidTypeException("Udt type serializer must inherit from UdtSerializer");
                    }
                    break;
                case ColumnTypeCode.Custom:
                    if (!(typeSerializer.TypeInfo is CustomColumnInfo))
                    {
                        throw new InvalidTypeException("Custom serializer must provide additional information." +
                                                       "You must set serializer TypeInfo using an instance of CustomColumnInfo");
                    }
                    break;
            }
            _definitions.Add(typeSerializer);
            return this;
        }
    }
}
