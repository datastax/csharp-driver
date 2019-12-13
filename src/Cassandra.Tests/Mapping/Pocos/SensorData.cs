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

using System.Collections.Generic;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class SensorData
    {
        public string Id { get; set; }

        public string Bucket { get; set; }

        public TimeUuid Timestamp { get; set; }

        public double Value { get; set; }

        public static IEnumerable<SensorData> GetDefaultEntities()
        {
            return new List<SensorData>
            {
                new SensorData { Bucket = "bucket1", Id = "sensor1", Timestamp = TimeUuid.NewId(), Value = 1.5 },
                new SensorData { Bucket = "bucket1", Id = "sensor1", Timestamp = TimeUuid.NewId(), Value = 2 },
                new SensorData { Bucket = "bucket1", Id = "sensor1", Timestamp = TimeUuid.NewId(), Value = 2.5 },
                new SensorData { Bucket = "bucket2", Id = "sensor1", Timestamp = TimeUuid.NewId(), Value = 1 },
                new SensorData { Bucket = "bucket2", Id = "sensor1", Timestamp = TimeUuid.NewId(), Value = 1.5 },
                new SensorData { Bucket = "bucket2", Id = "sensor1", Timestamp = TimeUuid.NewId(), Value = 0.5 }
            };
        }
    }
}