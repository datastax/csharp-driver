//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Dse;

namespace Dse.Graph
{
    /// <summary>
    /// The default graph options to use for a DSE cluster.
    /// <para>
    /// These options will be used for all graph statements sent to the cluster, unless they have been explicitly overridden
    /// at the statement level.
    /// </para>
    /// </summary>
    public class GraphOptions
    {
        /// <summary>
        /// Default value for graph language.
        /// </summary>
        public const string DefaultLanguage = "gremlin-groovy";
        
        /// <summary>
        /// Default value for graph language.
        /// </summary>
        internal const string GraphSON2Language = "bytecode-json";

        /// <summary>
        /// Default value for graph source.
        /// </summary>
        public const string DefaultSource = "g";

        /// <summary>
        /// Default value for read timeout.
        /// </summary>
        public const int DefaultReadTimeout = Timeout.Infinite;

        private static class PayloadKey
        {
            public const string Language = "graph-language";
            public const string Name = "graph-name";
            public const string Source = "graph-source";
            public const string ReadConsistencyLevel = "graph-read-consistency";
            public const string WriteConsitencyLevel = "graph-write-consistency";
            public const string RequestTimeout = "request-timeout";
        }

        private volatile IDictionary<string, byte[]> _defaultPayload;
        private volatile string _language = DefaultLanguage;
        private volatile string _name;
        private volatile string _source = DefaultSource;
        private long _readConsistencyLevel = long.MinValue;
        private long _writeConsistencyLevel = long.MinValue;
        private volatile int _readTimeout = DefaultReadTimeout;

        /// <summary>
        /// The consistency levels names that are different from ConsistencyLevel.ToString().ToUpper()
        /// </summary>
        private static readonly IDictionary<ConsistencyLevel, string> ConsistencyLevelNames =
            new Dictionary<ConsistencyLevel, string>
            {
                { ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM" },
                { ConsistencyLevel.EachQuorum, "EACH_QUORUM" },
                { ConsistencyLevel.LocalSerial, "LOCAL_SERIAL" },
                { ConsistencyLevel.LocalOne, "LOCAL_ONE" }
            };

        /// <summary>
        /// Gets the graph language to use in graph queries.
        /// </summary>
        public string Language
        {
            get { return _language; }
        }

        /// <summary>
        /// Gets the graph name to use in graph queries.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Gets the consistency level used for read queries
        /// </summary>
        public ConsistencyLevel? ReadConsistencyLevel
        {
            get
            {
                //4 bytes for consistency representation and 1 bit for null flag
                long value = Interlocked.Read(ref _readConsistencyLevel);
                if (value == long.MinValue)
                {
                    return null;
                }
                return (ConsistencyLevel) value;
            }
        }

        /// <summary>
        /// Gets the value that overrides the 
        /// <see href="http://docs.datastax.com/en/drivers/csharp/3.0/html/P_Cassandra_SocketOptions_ReadTimeoutMillis.htm">
        /// default per-host read timeout</see> in milliseconds for all graph queries.
        /// <para>Default: <c>Timeout.Infinite</c> (-1).</para>
        /// </summary>
        /// <seealso cref="SetReadTimeoutMillis"/>
        public int ReadTimeoutMillis
        {
            get { return _readTimeout; }
        }

        /// <summary>
        /// Gets the graph traversal source name in graph queries.
        /// </summary>
        public string Source
        {
            get { return _source; }
        }

        /// <summary>
        /// Gets the consistency level used for read queries
        /// </summary>
        public ConsistencyLevel? WriteConsistencyLevel
        {
            get
            {
                //4 bytes for consistency representation and 1 bit for null flag
                long value = Interlocked.Read(ref _writeConsistencyLevel);
                if (value == long.MinValue)
                {
                    return null;
                }
                return (ConsistencyLevel)value;
            }
        }

        /// <summary>
        /// Creates a new instance of <see cref="GraphOptions"/>.
        /// </summary>
        public GraphOptions()
        {
            RebuildDefaultPayload();
        }

        internal bool IsAnalyticsQuery(IGraphStatement statement)
        {
            return (statement.GraphSource ?? Source) == "a";
        }

        /// <summary>
        /// Sets the graph language to use in graph queries.
        /// If you don't call this method, it defaults to <see cref="DefaultLanguage"/>.
        /// </summary>
        /// <param name="language"></param>
        /// <returns></returns>
        public GraphOptions SetLanguage(string language)
        {
            _language = language ?? DefaultLanguage;
            RebuildDefaultPayload();
            return this;
        }

        /// <summary>
        /// Sets the graph name to use in graph queries.
        /// </summary>
        public GraphOptions SetName(string name)
        {
            _name = name;
            RebuildDefaultPayload();
            return this;
        }

        /// <summary>
        /// Sets the consistency level for the read graph queries. 
        /// </summary>
        /// <param name="consistency">The consistency level to use in read graph queries.</param>
        public GraphOptions SetReadConsistencyLevel(ConsistencyLevel consistency)
        {
            Interlocked.Exchange(ref _readConsistencyLevel, (long)consistency);
            RebuildDefaultPayload();
            return this;
        }

        /// <summary>
        /// Sets the default per-host read timeout in milliseconds for all graph queries.
        /// <para>
        /// When setting a value of less than or equals to zero (<see cref="Timeout.Infinite"/>),
        /// it will use an infinite timeout.
        /// </para>
        /// </summary>
        public GraphOptions SetReadTimeoutMillis(int timeout)
        {
            _readTimeout = timeout;
            RebuildDefaultPayload();
            return this;
        }

        /// <summary>
        /// Sets the graph traversal source name to use in graph queries.
        /// If you don't call this method, it defaults to <see cref="DefaultSource"/>.
        /// </summary>
        public GraphOptions SetSource(string source)
        {
            _source = source ?? DefaultSource;
            RebuildDefaultPayload();
            return this;
        }

        /// <summary>
        /// Sets the graph source to the server-defined analytic traversal source ('a')
        /// </summary>
        public GraphOptions SetSourceAnalytics()
        {
            return SetSource("a");
        }

        /// <summary>
        /// Sets the consistency level for the write graph queries. 
        /// </summary>
        /// <param name="consistency">The consistency level to use in write graph queries.</param>
        public GraphOptions SetWriteConsistencyLevel(ConsistencyLevel consistency)
        {
            Interlocked.Exchange(ref _writeConsistencyLevel, (long)consistency);
            RebuildDefaultPayload();
            return this;
        }

        internal IDictionary<string, byte[]> BuildPayload(IGraphStatement statement)
        {
            if (statement.GraphLanguage == null && statement.GraphName == null &&
                statement.GraphSource == null && statement.ReadTimeoutMillis == 0)
            {
                if (!statement.IsSystemQuery || !_defaultPayload.ContainsKey(PayloadKey.Name))
                {
                    //The user has not used the graph settings at statement level
                    //Or is a system query but there isn't a name defined at GraphOptions level
                    return _defaultPayload;   
                }
            }
            var payload = new Dictionary<string, byte[]>();
            Add(payload, PayloadKey.Language, statement.GraphLanguage, null);
            if (!statement.IsSystemQuery)
            {
                Add(payload, PayloadKey.Name, statement.GraphName, null);
            }
            Add(payload, PayloadKey.Source, statement.GraphSource, null);
            Add(payload, PayloadKey.ReadConsistencyLevel,
                statement.GraphReadConsistencyLevel == null ? null : GetConsistencyName(statement.GraphReadConsistencyLevel.Value), null);
            Add(payload, PayloadKey.WriteConsitencyLevel,
                statement.GraphWriteConsistencyLevel == null ? null : GetConsistencyName(statement.GraphWriteConsistencyLevel.Value), null);
            var readTimeout = statement.ReadTimeoutMillis != 0 ? statement.ReadTimeoutMillis : ReadTimeoutMillis;
            if (readTimeout > 0)
            {
                // only non-infinite timeouts needs to be included in the payload
                Add<long>(payload, PayloadKey.RequestTimeout, statement.ReadTimeoutMillis, 0, ToBuffer);
            }
            return payload;
        }

        private void Add<T>(IDictionary<string, byte[]> payload, string key, T value, T empty, Func<T, byte[]> converter = null)
        {
            if (converter == null)
            {
                converter = v => ToUtf8Buffer((string)(object)v);
            }
            byte[] payloadValue;
            if (value != null && !value.Equals(empty))
            {
                payloadValue = converter(value);
            }
            else
            {
                _defaultPayload.TryGetValue(key, out payloadValue);
            }
            if (payloadValue == null)
            {
                return;
            }
            payload.Add(key, payloadValue);
        }

        private void RebuildDefaultPayload()
        {
            var payload = new Dictionary<string, byte[]>
            {
                { PayloadKey.Language, ToUtf8Buffer(_language) },
                { PayloadKey.Source, ToUtf8Buffer(_source) }
            };
            if (_name != null)
            {
                payload.Add(PayloadKey.Name, ToUtf8Buffer(_name));
            }
            var readConsistencyLevel = ReadConsistencyLevel;
            if (readConsistencyLevel != null)
            {
                payload.Add(PayloadKey.ReadConsistencyLevel, ToUtf8Buffer(
                    GetConsistencyName(readConsistencyLevel.Value)));
            }
            var writeConsistencyLevel = WriteConsistencyLevel;
            if (writeConsistencyLevel != null)
            {
                payload.Add(PayloadKey.WriteConsitencyLevel, ToUtf8Buffer(
                    GetConsistencyName(writeConsistencyLevel.Value)));
            }
            if (ReadTimeoutMillis > 0)
            {
                payload.Add(PayloadKey.RequestTimeout, ToBuffer(ReadTimeoutMillis));
            }
            _defaultPayload = payload;
        }

        private static string GetConsistencyName(ConsistencyLevel consistency)
        {
            string name;
            if (!ConsistencyLevelNames.TryGetValue(consistency, out name))
            {
                //If not defined, use upper case representation
                name = consistency.ToString().ToUpper();
            }
            return name;
        }

        private static byte[] ToUtf8Buffer(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        private static byte[] ToBuffer(long value)
        {
            var serializer = Serialization.TypeSerializer.PrimitiveLongSerializer;
            return serializer.Serialize((ushort) Cluster.MaxProtocolVersion, value);
        }
    }
}
