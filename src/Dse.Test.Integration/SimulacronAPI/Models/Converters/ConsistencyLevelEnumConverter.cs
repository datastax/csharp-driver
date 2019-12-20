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
using Newtonsoft.Json;

namespace Dse.Test.Integration.SimulacronAPI.Models.Converters
{
    public class ConsistencyLevelEnumConverter : JsonConverter
    {
        private static readonly IDictionary<string, ConsistencyLevel> Map = 
            new Dictionary<string, ConsistencyLevel>(StringComparer.OrdinalIgnoreCase)
            {
                { "ONE", ConsistencyLevel.One },
                { "TWO", ConsistencyLevel.Two },
                { "THREE", ConsistencyLevel.Three },
                { "ALL", ConsistencyLevel.All },
                { "ANY", ConsistencyLevel.Any },
                { "QUORUM", ConsistencyLevel.Quorum },
                { "LOCAL_QUORUM", ConsistencyLevel.LocalQuorum },
                { "LOCAL_ONE", ConsistencyLevel.LocalOne },
                { "LOCAL_SERIAL", ConsistencyLevel.LocalSerial },
                { "SERIAL", ConsistencyLevel.Serial }
            };

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var value = (string)reader.Value;

            if (value == null)
            {
                return null;
            }

            return ConsistencyLevelEnumConverter.Map[value];
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(ConsistencyLevel) == objectType || typeof(ConsistencyLevel?) == objectType;
        }

        public override bool CanWrite => false;

        public static string ConvertConsistencyLevelToString(ConsistencyLevel cl)
        {
            return ConsistencyLevelEnumConverter.Map.Single(kvp => kvp.Value == cl).Key;
        }
    }
}