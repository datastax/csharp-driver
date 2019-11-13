//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;

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
        private IDictionary<IPAddress, int> _reasons;

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

                if (reader.Serializer.ProtocolVersion.SupportsFailureReasons())
                {
                    _reasons = GetReasonsDictionary(reader, _failures);
                }
            }

            _dataPresent = reader.ReadByte() != 0;
        }

        /// <summary>
        /// 
        /// </summary>
        internal static IDictionary<IPAddress, int> GetReasonsDictionary(FrameReader reader, int length)
        {
            var reasons = new Dictionary<IPAddress, int>(length);
            for (var i = 0; i < length; i++)
            {
                var buffer = new byte[reader.ReadByte()];
                reader.Read(buffer, 0, buffer.Length);
                reasons[new IPAddress(buffer)] = reader.ReadUInt16();
            }

            return new ReadOnlyDictionary<IPAddress, int>(reasons);
        }

        public override DriverException CreateException()
        {
            if (_isFailure)
            {
                if (_reasons != null)
                {
                    // The message in this protocol provided a full map with the reasons of the failures.
                    return new ReadFailureException(_consistency, _received, _blockFor, _dataPresent, _reasons);
                }
                return new ReadFailureException(_consistency, _received, _blockFor, _dataPresent, _failures);
            }
            return new ReadTimeoutException(_consistency, _received, _blockFor, _dataPresent);
        }
    }
}