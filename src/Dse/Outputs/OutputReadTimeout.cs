//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
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