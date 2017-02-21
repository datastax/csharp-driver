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
    /// <summary>
    /// Handles the parsing of the read timeout and read failure errors
    /// </summary>
    internal class OutputReadTimeout : OutputError
    {
        private int _blockFor;
        private ConsistencyLevel _consistency;
        private bool _dataPresent;
        private int _received;
        private int _failures;
        private readonly bool _isFailure;

        internal OutputReadTimeout(bool isFailure)
        {
            _isFailure = isFailure;
        }

        protected override void Load(FrameReader reader)
        {
            _consistency = (ConsistencyLevel) reader.ReadInt16();
            _received = reader.ReadInt32();
            _blockFor = reader.ReadInt32();
            if (_isFailure)
            {
                _failures = reader.ReadInt32();
            }
            _dataPresent = reader.ReadByte() != 0;
        }

        public override DriverException CreateException()
        {
            if (_isFailure)
            {
                return new ReadFailureException(_consistency, _received, _blockFor, _dataPresent, _failures);
            }
            return new ReadTimeoutException(_consistency, _received, _blockFor, _dataPresent);
        }
    }
}