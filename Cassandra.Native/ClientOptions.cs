using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  Additional options of the .net Cassandra driver.
    /// </summary>
    public class ClientOptions
    {
        private readonly bool _withoutRowSetBuffering = false;
        private readonly int _queryAbortTimeout = Timeout.Infinite;
        private readonly string _defaultKeyspace = null;
        private readonly int _asyncCallAbortTimeout = Timeout.Infinite;

        public ClientOptions()
            : this(false, Timeout.Infinite,null,Timeout.Infinite)
        {
        }

        public ClientOptions( bool withoutRowSetBuffering, int queryAbortTimeout, string defaultKeyspace, int asyncCallAbortTimeout)
        {
            this._withoutRowSetBuffering = withoutRowSetBuffering;
            this._queryAbortTimeout = queryAbortTimeout;
            this._defaultKeyspace = defaultKeyspace;
            this._asyncCallAbortTimeout = asyncCallAbortTimeout;
        }

        public bool WithoutRowSetBuffering
        {
            get { return _withoutRowSetBuffering; }
        }

        public int QueryAbortTimeout
        {
            get { return _queryAbortTimeout; }
        }

        public string DefaultKeyspace
        {
            get { return _defaultKeyspace; }
        }

        public int AsyncCallAbortTimeout
        {
            get { return _asyncCallAbortTimeout; }
        }
    }
}

// end namespace