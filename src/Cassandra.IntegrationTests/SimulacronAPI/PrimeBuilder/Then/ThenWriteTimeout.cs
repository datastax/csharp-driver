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

namespace Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then
{
    public class ThenWriteTimeout : BaseThen
    {
        private readonly string _message;
        private readonly int _consistencyLevel;
        private readonly int _received;
        private readonly int _blockFor;
        private readonly string _writeType;

        public ThenWriteTimeout(string message, int consistencyLevel, int received, int blockFor, string writeType)
        {
            _message = message;
            _consistencyLevel = consistencyLevel;
            _received = received;
            _blockFor = blockFor;
            _writeType = writeType;
        }

        public override object Render()
        {
            return new
            {
                result = "write_timeout",
                consistency_level = _consistencyLevel,
                received = _received,
                block_for = _blockFor,
                delay_in_ms = DelayInMs,
                message = _message,
                ignore_on_prepare = IgnoreOnPrepare,
                write_type = _writeType
            };
        }
    }
}