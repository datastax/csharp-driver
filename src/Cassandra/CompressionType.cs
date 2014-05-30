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