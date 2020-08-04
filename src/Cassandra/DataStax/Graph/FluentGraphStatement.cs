﻿// 
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
using Cassandra.DataStax.Graph.Internal;

namespace Cassandra.DataStax.Graph
{
    public class FluentGraphStatement : GraphStatement
    {
        private FluentGraphStatement(
            object queryBytecode, 
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>> customSerializers, 
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>> customDeserializers,
            bool deserializeGraphNodes)
        {
            DeserializeGraphNodes = deserializeGraphNodes;
            QueryBytecode = queryBytecode;
            CustomSerializers = customSerializers;
            CustomDeserializers = customDeserializers;
        }
        
        public FluentGraphStatement(
            object queryBytecode, 
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>> customSerializers, 
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>> customDeserializers)
            : this(queryBytecode, customSerializers, customDeserializers, false)
        {
        }
        
        public FluentGraphStatement(
            object queryBytecode,
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>> customSerializers) 
            : this(queryBytecode, customSerializers, null, true)
        {
        }

        public object QueryBytecode { get; }
        
        internal bool DeserializeGraphNodes { get; }

        internal IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>> CustomSerializers { get; }
        
        internal IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>> CustomDeserializers { get; }

        internal override IStatement GetIStatement(GraphOptions options)
        {
            return null;
        }
    }
}