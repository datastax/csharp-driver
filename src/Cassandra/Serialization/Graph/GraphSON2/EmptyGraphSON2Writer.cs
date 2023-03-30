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
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    /// <summary>
    /// This is just to grab the tinkerpop's serializers (fewer code changes to the imported code)
    /// </summary>
    internal class EmptyGraphSON2Writer : GraphSON2Writer
    {
        public Dictionary<Type, IGraphSONSerializer> GetSerializers() => base.Serializers;
    }
}