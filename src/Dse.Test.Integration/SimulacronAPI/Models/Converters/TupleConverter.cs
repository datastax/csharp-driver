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
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Test.Integration.SimulacronAPI.Models.Converters
{
    internal class TupleConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var type = value.GetType();
            var array = new List<object>();
            PropertyInfo propertyInfo;
            var i = 1;

            while ((propertyInfo = type.GetProperty($"Item{i++}")) != null)
            {
                array.Add(propertyInfo.GetValue(value));
            }

            serializer.Serialize(writer, array);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var argTypes = objectType.GetGenericArguments();
            var array = serializer.Deserialize<JArray>(reader);
            var items = array.Select((a, index) => a.ToObject(argTypes[index])).ToArray();

            var constructor = objectType.GetConstructor(argTypes);
            return constructor.Invoke(items);
        }

        public override bool CanConvert(Type type)
        {
            return type.Name.StartsWith("ValueTuple`") || type.Name.StartsWith("Tuple`");
        }
    }
}