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
    public class ServerError
    {
        private ServerError(string value)
        {
            Value = value;
        }

        public string Value { get; }

        public static ServerError IsBootstrapping { get; } = new ServerError("is_bootstrapping");

        public static ServerError Overloaded { get; } = new ServerError("overloaded");

        public static ServerError Invalid { get; } = new ServerError("invalid");

        public static ServerError ConfigError { get; } = new ServerError("config_error");
    }
}