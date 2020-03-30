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

namespace Cassandra
{
    /// <summary>
    /// Parses a FunctionFailureException from a function failure error
    /// </summary>
    internal class OutputFunctionFailure : OutputError
    {
        private FunctionFailureException _exception;

        public override DriverException CreateException()
        {
            return _exception;
        }

        protected override void Load(FrameReader reader)
        {
            _exception = new FunctionFailureException(Message)
            {
                Keyspace = reader.ReadString(),
                Name = reader.ReadString(),
                ArgumentTypes = reader.ReadStringList()
            };
        }
    }
}
