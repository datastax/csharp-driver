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

namespace Cassandra.IntegrationTests.SimulacronAPI.Then
{
    public class ThenUnavailableError : BaseThen
    {
        private readonly string _message;
        private readonly int _consistencyLevel;
        private readonly int _required;
        private readonly int _alive;

        public ThenUnavailableError(string message, int consistencyLevel, int required, int alive)
        {
            _message = message;
            _consistencyLevel = consistencyLevel;
            _required = required;
            _alive = alive;
        }

        public override object Render()
        {
            return new
            {
                result = "unavailable",
                consistency_level = _consistencyLevel,
                required = _required,
                alive = _alive,
                delay_in_ms = DelayInMs,
                message = _message,
                ignore_on_prepare = IgnoreOnPrepare
            };
        }
    }
}