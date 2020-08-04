//
//      Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cassandra.DataStax.Graph
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
        public const string DefaultLanguage = GraphOptions.GremlinGroovy;
        
        /// <summary>
        /// Default value for graph language.
        /// </summary>
        internal const string BytecodeJson = "bytecode-json";

        
        internal const string GremlinGroovy = "gremlin-groovy";

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
            public const string Results = "graph-results";
            public const string Language = "graph-language";
            public const string Name = "graph-name";
            public const string Source = "graph-source";
            public const string ReadConsistencyLevel = "graph-read-consistency";
            public const string WriteConsitencyLevel = "graph-write-consistency";
            public const string RequestTimeout = "request-timeout";
        }

        private volatile IDictionary<string, byte[]> _defaultPayload;
        private volatile string _language = GraphOptions.DefaultLanguage;
        private volatile string _name;
        private volatile string _source = GraphOptions.DefaultSource;
        private long _readConsistencyLevel = long.MinValue;
        private long _writeConsistencyLevel = long.MinValue;
        private volatile int _readTimeout = GraphOptions.DefaultReadTimeout;

        private volatile object _nullableGraphProtocol = null;

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
        /// Gets the graph language to use in graph queries. The default is <see cref="DefaultLanguage"/>.
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
        /// If not explicitly specified, the following rules apply:
        /// <list type=""></list>
        /// <list type="bullet">
        /// <item>
        /// <description>If <see cref="Name"/> is specified and is a Core graph,
        /// set <see cref="GraphProtocol.GraphSON3"/>.</description>
        /// </item>
        /// <item>
        /// <description>If <see cref="Language"/> is gremlin-groovy,
        /// set <see cref="GraphProtocol.GraphSON1"/>.</description>
        /// </item>
        /// <item>
        /// <description>Otherwise set <see cref="GraphProtocol.GraphSON2"/>.</description>
        /// </item>
        /// </list>
        /// </summary>
        public GraphProtocol? GraphProtocolVersion => (GraphProtocol?) _nullableGraphProtocol;

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

        /// <summary>
        /// Clone a graph options object, replacing the graph protocol version.
        /// </summary>
        internal GraphOptions(GraphOptions other, GraphProtocol version)
        {
            _language = other._language;
            _name = other._name;
            _source = other._source;
            _readConsistencyLevel = Interlocked.Read(ref other._readConsistencyLevel);
            _writeConsistencyLevel = Interlocked.Read(ref other._writeConsistencyLevel);
            _readTimeout = other._readTimeout;
            _nullableGraphProtocol = version;
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
            _language = language ?? GraphOptions.DefaultLanguage;
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
        /// Sets the graph protocol version to use in graph queries. See <see cref="GraphProtocolVersion"/>.
        /// </summary>
        public GraphOptions SetGraphProtocolVersion(GraphProtocol version)
        {
            _nullableGraphProtocol = version;
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
            _source = source ?? GraphOptions.DefaultSource;
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
            if (statement.GraphProtocolVersion == null && GraphProtocolVersion == null)
            {
                throw new DriverInternalError(
                    "Could not resolve graph protocol version. This is a bug, please report.");
            }

            if (statement.GraphLanguage == null && statement.GraphName == null &&
                statement.GraphSource == null && statement.ReadTimeoutMillis == 0
                && statement.GraphProtocolVersion == null)
            {
                if (!statement.IsSystemQuery || !_defaultPayload.ContainsKey(PayloadKey.Name))
                {
                    //The user has not used the graph settings at statement level
                    //Or is a system query but there isn't a name defined at GraphOptions level
                    return _defaultPayload;   
                }
            }
            var payload = new Dictionary<string, byte[]>();
            Add(payload, PayloadKey.Results, statement.GraphProtocolVersion?.GetInternalRepresentation(), null);
            Add(payload, PayloadKey.Language, statement.GraphLanguage, null);
            if (!statement.IsSystemQuery)
            {
                Add(payload, PayloadKey.Name, statement.GraphName, null);
            }
            Add(payload, PayloadKey.Source, statement.GraphSource, null);
            Add(payload, PayloadKey.ReadConsistencyLevel,
                statement.GraphReadConsistencyLevel == null ? null : GraphOptions.GetConsistencyName(statement.GraphReadConsistencyLevel.Value), null);
            Add(payload, PayloadKey.WriteConsitencyLevel,
                statement.GraphWriteConsistencyLevel == null ? null : GraphOptions.GetConsistencyName(statement.GraphWriteConsistencyLevel.Value), null);
            var readTimeout = statement.ReadTimeoutMillis != 0 ? statement.ReadTimeoutMillis : ReadTimeoutMillis;
            if (readTimeout > 0)
            {
                // only non-infinite timeouts needs to be included in the payload
                Add<long>(payload, PayloadKey.RequestTimeout, statement.ReadTimeoutMillis, 0, GraphOptions.ToBuffer);
            }
            return payload;
        }

        private void Add<T>(IDictionary<string, byte[]> payload, string key, T value, T empty, Func<T, byte[]> converter = null)
        {
            if (converter == null)
            {
                converter = v => GraphOptions.ToUtf8Buffer((string)(object)v);
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
                { PayloadKey.Language, GraphOptions.ToUtf8Buffer(_language) },
                { PayloadKey.Source, GraphOptions.ToUtf8Buffer(_source) }
            };
            if (_name != null)
            {
                payload.Add(PayloadKey.Name, GraphOptions.ToUtf8Buffer(_name));
            }
            var readConsistencyLevel = ReadConsistencyLevel;
            if (readConsistencyLevel != null)
            {
                payload.Add(PayloadKey.ReadConsistencyLevel, GraphOptions.ToUtf8Buffer(
                    GraphOptions.GetConsistencyName(readConsistencyLevel.Value)));
            }
            var writeConsistencyLevel = WriteConsistencyLevel;
            if (writeConsistencyLevel != null)
            {
                payload.Add(PayloadKey.WriteConsitencyLevel, GraphOptions.ToUtf8Buffer(
                    GraphOptions.GetConsistencyName(writeConsistencyLevel.Value)));
            }
            if (ReadTimeoutMillis > 0)
            {
                payload.Add(PayloadKey.RequestTimeout, GraphOptions.ToBuffer(ReadTimeoutMillis));
            }

            if (GraphProtocolVersion != null)
            {
                payload.Add(
                    PayloadKey.Results, 
                    GraphOptions.ToUtf8Buffer(GraphProtocolVersion.Value.GetInternalRepresentation()));
            }
            _defaultPayload = payload;
        }

        private static string GetConsistencyName(ConsistencyLevel consistency)
        {
            if (!GraphOptions.ConsistencyLevelNames.TryGetValue(consistency, out string name))
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
