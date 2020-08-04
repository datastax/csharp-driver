﻿#region License

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
using System.Collections.Generic;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON2.Tinkerpop
{
    internal class TinkerpopTimestampSerializer : IGraphSONSerializer
    {
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
        
        public Dictionary<string, dynamic> Dictify(dynamic objectData, IGraphSONWriter writer)
        {
            TinkerpopTimestamp value = objectData;
            var ticks = (value.AsDateTimeOffset() - TinkerpopTimestampSerializer.UnixStart).Ticks;
            return GraphSONUtil.ToTypedValue("Timestamp", ticks / TimeSpan.TicksPerMillisecond);
        }
    }
}