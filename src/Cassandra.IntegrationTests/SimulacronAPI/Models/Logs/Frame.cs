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
using Newtonsoft.Json;

namespace Cassandra.IntegrationTests.SimulacronAPI.Models.Logs
{
    public class Frame
    {
        [JsonProperty("protocol_version")]
        public int ProtocolVersion { get; set; }

        [JsonProperty("beta")]
        public bool Beta { get; set; }

        [JsonProperty("stream_id")]
        public int StreamId { get; set; }

        [JsonProperty("tracing_id")]
        public Guid? TracingId { get; set; }

        [JsonProperty("custom_payload")]
        public Dictionary<string, string> CustomPayload { get; set; }

        [JsonProperty("warnings")]
        public List<string> Warnings { get; set; }

        [JsonProperty("message")]
        public object Message
        {
            get => _message;
            set
            {
                _message = value;
                MessageJson = value == null ? null : JsonConvert.SerializeObject(value);
            }
        }

        private object _message;

        private string MessageJson { get; set; }

        public BaseMessage GetBaseMessage()
        {
            return GetTypedMessage<BaseMessage>();
        }

        public QueryMessage GetQueryMessage()
        {
            return GetTypedMessage<QueryMessage>();
        }

        public BatchMessage GetBatchMessage()
        {
            return GetTypedMessage<BatchMessage>();
        }

        private T GetTypedMessage<T>()
        {
            return MessageJson == null ? default : JsonConvert.DeserializeObject<T>(MessageJson);
        }
    }
}