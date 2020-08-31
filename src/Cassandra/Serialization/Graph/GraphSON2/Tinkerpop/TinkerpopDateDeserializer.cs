#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#endregion

using System;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2.Tinkerpop
{
    internal class TinkerpopDateDeserializer : IGraphSONDeserializer
    {
        private const string Prefix = "g";
        private const string TypeKey = "Date";
        
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
        
        public static string TypeName =>
            GraphSONUtil.FormatTypeName(TinkerpopDateDeserializer.Prefix, TinkerpopDateDeserializer.TypeKey);

        public dynamic Objectify(JToken graphsonObject, IGraphSONReader reader)
        {
            var milliseconds = graphsonObject.ToObject<long>();
            return TinkerpopDateDeserializer.UnixStart.AddTicks(TimeSpan.TicksPerMillisecond * milliseconds);
        }
    }
}