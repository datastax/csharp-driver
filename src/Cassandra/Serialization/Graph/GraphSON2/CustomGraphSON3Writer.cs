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
using Cassandra.Serialization.Graph.Dse;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal class CustomGraphSON3Writer : CustomGraphSON2Writer
    {
        private static readonly IDictionary<Type, IGraphSONSerializer> CustomGraphSON3SpecificSerializers =
            new Dictionary<Type, IGraphSONSerializer>
            {
                { typeof(IList<object>), new ListSerializer() },
                { typeof(ISet<object>), new SetSerializer() },
                { typeof(IDictionary<object, object>), new MapSerializer() },
                { typeof(IPAddress), new InetAddressSerializer() },
                { typeof(Duration), new Duration3Serializer() }
            };

        /// <summary>
        ///     Creates a new instance of <see cref="GraphSONReader"/>.
        /// </summary>
        public CustomGraphSON3Writer()
        {
            foreach (var kv in CustomGraphSON3Writer.CustomGraphSON3SpecificSerializers)
            {
                Serializers[kv.Key] = kv.Value;
            }
        }

        protected override dynamic DictToGraphSONDict(dynamic dict)
        {
            return Serializers[typeof(IDictionary<object, object>)].Dictify(dict, this);
        }

        protected override dynamic SetToGraphSONSet(dynamic collection)
        {
            return Serializers[typeof(ISet<object>)].Dictify(collection, this);
        }

        protected override dynamic ListToGraphSONList(dynamic collection)
        {
            return Serializers[typeof(IList<object>)].Dictify(collection, this);
        }
    }
}