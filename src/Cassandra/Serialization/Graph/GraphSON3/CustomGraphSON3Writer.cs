//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Net;

using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.Serialization.Graph.GraphSON2.Tinkerpop;
using Cassandra.Serialization.Graph.GraphSON3.Dse;
using Cassandra.Serialization.Graph.GraphSON3.Tinkerpop;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON3
{
    /// <inheritdoc />
    internal class CustomGraphSON3Writer : CustomGraphSON2Writer
    {
        private static readonly IDictionary<Type, IGraphSONSerializer> CustomGraphSON3SpecificSerializers =
            new Dictionary<Type, IGraphSONSerializer>
            {
                { typeof(IList<object>), new ListSerializer() },
                { typeof(List<object>), new ListSerializer() },
                { typeof(ISet<object>), new SetSerializer() },
                { typeof(HashSet<object>), new SetSerializer() },
                { typeof(IDictionary<object, object>), new MapSerializer() },
                { typeof(Dictionary<object, object>), new MapSerializer() },
                { typeof(IPAddress), new InetAddressSerializer() },
                { typeof(Duration), new Duration3Serializer() },
                { typeof(byte[]), new ByteBufferSerializer() },
            };

        private static Dictionary<Type, IGraphSONSerializer> DefaultSerializers { get; } =
            new EmptyGraphSON2Writer().GetSerializers();

        static CustomGraphSON3Writer()
        {
            CustomGraphSON2Writer.AddGraphSON2Serializers(CustomGraphSON3Writer.DefaultSerializers);
            CustomGraphSON3Writer.AddGraphSON3Serializers(CustomGraphSON3Writer.DefaultSerializers);
        }

        protected static void AddGraphSON3Serializers(IDictionary<Type, IGraphSONSerializer> dictionary)
        {
            foreach (var kv in CustomGraphSON3Writer.CustomGraphSON3SpecificSerializers)
            {
                dictionary[kv.Key] = kv.Value;
            }
        }

        public CustomGraphSON3Writer(
            IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializers,
            IGraphSONWriter writer) :
            base(CustomGraphSON3Writer.DefaultSerializers, customSerializers, writer)
        {
        }

        protected override dynamic DictToGraphSONDict(dynamic dict)
        {
            return Serializers[typeof(IDictionary<object, object>)].Dictify(dict, Writer);
        }

        protected override dynamic SetToGraphSONSet(dynamic collection)
        {
            return Serializers[typeof(ISet<object>)].Dictify(collection, Writer);
        }

        protected override dynamic ListToGraphSONList(dynamic collection)
        {
            return Serializers[typeof(IList<object>)].Dictify(collection, Writer);
        }
    }
}