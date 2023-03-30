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

using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.DataStax.Graph.Internal
{
    /// <summary>
    /// <para>
    /// This interface is not meant to be implemented by users. It is part of the public API so that
    /// the C# Graph Extension can provide custom deserializers for the Tinkerpop GLV types.
    /// </para>
    /// <para>
    /// Implementations of <see cref="IGraphSONDeserializer"/> depend on IGraphSONReader objects
    /// to deserialize inner properties.
    /// </para>
    /// <para>
    /// It's basically an interface for the Tinkerpop's <see cref="GraphSONReader"/> abstract class.
    /// </para>
    /// </summary>
    public interface IGraphSONReader
    {
        dynamic ToObject(JToken jToken);
    }
}