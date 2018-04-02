//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.IO;
using Dse.Serialization;

namespace Dse.Requests
{
    internal class StartupRequest : IRequest
    {
        public const byte OpCode = 0x01;
        private readonly IDictionary<string, string> _options;

        public StartupRequest(CompressionType compression, bool noCompact)
        {
            _options = new Dictionary<string, string>
            {
                {"CQL_VERSION", "3.0.0"}
            };

            string compressionName = null;
            switch (compression)
            {
                case CompressionType.LZ4:
                    compressionName = "lz4";
                    break;
                case CompressionType.Snappy:
                    compressionName = "snappy";
                    break;
            }

            if (compressionName != null)
            {
                _options.Add("COMPRESSION", compressionName);
            }

            if (noCompact)
            {
                _options.Add("NO_COMPACT", "true");
            }
        }

        public int WriteFrame(short streamId, MemoryStream stream, Serializer serializer)
        {
            var wb = new FrameWriter(stream, serializer);
            wb.WriteFrameHeader(0x00, streamId, OpCode);
            wb.WriteUInt16((ushort) _options.Count);
            foreach (var kv in _options)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.Close();
        }
    }
}
