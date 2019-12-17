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
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Newtonsoft.Json;

namespace Cassandra.IntegrationTests.SimulacronAPI.Models.Converters
{
    public class QueryTypeEnumConverter : JsonConverter
    {
        private static readonly IDictionary<string, QueryType> Map = 
            new Dictionary<string, QueryType>(StringComparer.OrdinalIgnoreCase)
            {
                { "EXECUTE", QueryType.Execute },
                { "PREPARE", QueryType.Prepare },
                { "QUERY", QueryType.Query },
                { "BATCH", QueryType.Batch },
                { "OPTIONS", QueryType.Options },
                { "STARTUP", QueryType.Startup },
                { "REGISTER", QueryType.Register }
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

            return QueryTypeEnumConverter.Map[value];
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(QueryType) == objectType || typeof(QueryType?) == objectType;
        }

        public override bool CanWrite => false;

        public static string ConvertQueryTypeToString(QueryType queryType)
        {
            return QueryTypeEnumConverter.Map.Single(kvp => kvp.Value == queryType).Key;
        }
    }
}