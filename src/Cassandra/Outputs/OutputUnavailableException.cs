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
    internal class OutputUnavailableException : OutputError
    {
        private ConsistencyLevel _consistency;
        private int _required;
        private int _alive;

        protected override void Load(FrameReader cb)
        {
            _consistency = (ConsistencyLevel) cb.ReadInt16();
            _required = cb.ReadInt32();
            _alive = cb.ReadInt32();
        }

        public override DriverException CreateException()
        {
            return new UnavailableException(_consistency, _required, _alive);
        }
    }
}