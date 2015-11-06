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
        private int _blockFor;
        private ConsistencyLevel _consistencyLevel;
        private int _received;
        private string _writeType;
        private readonly bool _isFailure;
        private int _failures;

        internal OutputWriteTimeout(bool isFailure)
        {
            _isFailure = isFailure;
        }

        protected override void Load(FrameReader reader)
        {
            _consistencyLevel = (ConsistencyLevel) reader.ReadInt16();
            _received = reader.ReadInt32();
            _blockFor = reader.ReadInt32();
            if (_isFailure)
            {
                _failures = reader.ReadInt32();
            }
            _writeType = reader.ReadString();
        }

        public override DriverException CreateException()
        {
            if (_isFailure)
            {
                return new WriteFailureException(_consistencyLevel, _received, _blockFor, _writeType, _failures);
            }
            return new WriteTimeoutException(_consistencyLevel, _received, _blockFor, _writeType);
        }
    }
}