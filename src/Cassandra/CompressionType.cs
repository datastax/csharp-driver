namespace Cassandra
{
    /// <summary>
    ///  Compression supported by the Cassandra binary protocol.
    /// </summary>
    public enum CompressionType
    {
        NoCompression,
        Snappy,
        LZ4
    }
}