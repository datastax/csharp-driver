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
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.Serialization.Graph.GraphSON3.Dse;
using Cassandra.Serialization.Graph.GraphSON3.Structure;
using Cassandra.Serialization.Graph.GraphSON3.Tinkerpop;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON3
{
    /// <inheritdoc />
    internal class CustomGraphSON3Reader : CustomGraphSON2Reader
    {
        private static readonly IReadOnlyDictionary<string, IGraphSONStructureDeserializer> CustomGraphSON3SpecificStructureDeserializers =
            new Dictionary<string, IGraphSONStructureDeserializer>
            {
                { Path3Deserializer.TypeName, new Path3Deserializer()}
            };

        private static readonly IDictionary<string, IGraphSONDeserializer> CustomGraphSON3SpecificDeserializers =
            new Dictionary<string, IGraphSONDeserializer>
            {
                { Duration3Serializer.TypeName, new Duration3Serializer() },
                { ByteBufferDeserializer.TypeName, new ByteBufferDeserializer() }
            };

        static CustomGraphSON3Reader()
        {
            CustomGraphSON2Reader.AddGraphSON2Deserializers(CustomGraphSON3Reader.Deserializers);
            CustomGraphSON2Reader.AddGraphSON2StructureDeserializers(CustomGraphSON3Reader.StructureDeserializers);
            CustomGraphSON3Reader.AddGraphSON3Deserializers(CustomGraphSON3Reader.Deserializers);
            CustomGraphSON3Reader.AddGraphSON3StructureDeserializers(CustomGraphSON3Reader.StructureDeserializers);
        }
        
        protected static void AddGraphSON3Deserializers(IDictionary<string, IGraphSONDeserializer> dictionary)
        {
            foreach (var kv in CustomGraphSON3Reader.CustomGraphSON3SpecificDeserializers)
            {
                dictionary[kv.Key] = kv.Value;
            }
        }
        
        protected static void AddGraphSON3StructureDeserializers(IDictionary<string, IGraphSONStructureDeserializer> dictionary)
        {
            foreach (var kv in CustomGraphSON3Reader.CustomGraphSON3SpecificStructureDeserializers)
            {
                dictionary[kv.Key] = kv.Value;
            }
        }

        public CustomGraphSON3Reader(
            Func<JToken, GraphNode> graphNodeFactory, 
            IReadOnlyDictionary<string, IGraphSONDeserializer> customDeserializers, 
            IGraphSONReader reader) 
            : base(
                CustomGraphSON3Reader.Deserializers, 
                CustomGraphSON3Reader.StructureDeserializers, 
                graphNodeFactory, 
                customDeserializers, 
                reader)
        {
        }
        
        private static Dictionary<string, IGraphSONDeserializer> Deserializers { get; } =
            new EmptyGraphSON2Reader().GetDeserializers();
        
        private static Dictionary<string, IGraphSONStructureDeserializer> StructureDeserializers { get; } =
            new Dictionary<string, IGraphSONStructureDeserializer>();
    }
}