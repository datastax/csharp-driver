//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra
{
    /// <summary>
    ///  Compression supported by the Cassandra binary protocol.
    /// </summary>
    public enum CompressionType
    {
        NoCompression = 0x00,
        Snappy = 0x01,
        LZ4 = 0x02
    }
}