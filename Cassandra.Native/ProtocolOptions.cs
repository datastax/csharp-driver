using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  Compression supported by the Cassandra binary protocol.
    /// </summary>
    public enum CompressionType
    {
        NoCompression,
        Snappy
    }

    /// <summary>
    ///  Options of the Cassandra __native__ binary protocol.
    /// </summary>
    public class ProtocolOptions
    {
        /// <summary>
        ///  The default port for Cassandra __native__ binary protocol: 9042.
        /// </summary>
        public const int DefaultPort = 9042;

        private readonly int _port;
        private CompressionType _compression = CompressionType.NoCompression;
        private readonly bool _withoutBuffering = false;
        private readonly int _queryAbortTimeout = Timeout.Infinite;

        /// <summary>
        ///  Creates a new <code>ProtocolOptions</code> instance using the
        ///  <code>DEFAULT_PORT</code>.
        /// </summary>

        public ProtocolOptions()
            : this(DefaultPort, false, Timeout.Infinite)
        {
        }

        /// <summary>
        ///  Creates a new <code>ProtocolOptions</code> instance using the provided port.
        /// </summary>
        /// <param name="port"> the port to use for the binary protocol.</param>

        public ProtocolOptions(int port,bool withoutBuffering, int queryAbortTimeout)
        {
            this._port = port;
            this._withoutBuffering = withoutBuffering;
            this._queryAbortTimeout = queryAbortTimeout;
        }

        /// <summary>
        ///  The port used to connect to the Cassandra hosts.
        /// </summary>
        /// 
        /// <returns>the port used to connect to the Cassandra hosts.</returns>

        public int Port
        {
            get { return _port; }
        }

        /// <summary>
        ///  Returns the compression used by the protocol. <p> The default compression is
        ///  <code>Compression.SNAPPY</code>.
        /// </summary>
        /// 
        /// <returns>the compression used.</returns>

        public CompressionType Compression
        {
            get { return _compression; }
        }

        /// <summary>
        ///  Sets the compression to use. <p> Note that while this setting can be changed
        ///  at any time, it will only apply to newly created connections.</p>
        /// </summary>
        /// <param name="compression"> the compression algorithm to use (or <code>Compression.NONE</code> to disable compression).
        ///  </param>
        /// 
        /// <returns>this <code>ProtocolOptions</code> object.</returns>

        public ProtocolOptions SetCompression(CompressionType compression)
        {
            this._compression = compression;
            return this;
        }

        public bool WithoutBuffering
        {
            get { return _withoutBuffering; }
        }

        public int QueryAbortTimeout
        {
            get { return _queryAbortTimeout; }
        }
    }
}

// end namespace