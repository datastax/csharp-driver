//
//      Copyright (C) 2012-2014 DataStax Inc.
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
    internal class OutputWriteTimeout : OutputError
    {
        private readonly WriteTimeoutInfo _info = new WriteTimeoutInfo();

        protected override void Load(BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel) cb.ReadInt16();
            _info.Received = cb.ReadInt32();
            _info.BlockFor = cb.ReadInt32();
            _info.WriteType = cb.ReadString();
        }

        public override DriverException CreateException()
        {
            return new WriteTimeoutException(_info.ConsistencyLevel, _info.Received, _info.BlockFor, _info.WriteType);
        }
    }
}