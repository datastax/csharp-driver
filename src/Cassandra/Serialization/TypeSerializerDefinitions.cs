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
using System.Collections.Generic;
using System.Linq;

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
        
        /// <summary>
        /// Adds the serializer if there is none defined yet with the same type.
        /// Checks the serializer type (serializer.GetType()) not the CRL or CQL types)
        /// </summary>
        internal TypeSerializerDefinitions DefineIfNotExists<T>(TypeSerializer<T> serializer)
        {
            return Definitions.All(d => d.GetType() != serializer.GetType()) ? Define(serializer) : this;
        }
    }
}
