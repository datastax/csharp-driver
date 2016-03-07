using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
        /// Default value for graph source.
        /// </summary>
        public const string DefaultSource = "default";

        private static class PayloadKey
        {
            public const string Alias = "graph-alias";
            public const string Language = "graph-language";
            public const string Name = "graph-name";
            public const string Source = "graph-source";
        }

        private volatile IDictionary<string, byte[]> _defaultPayload;
        private volatile string _language = DefaultLanguage;
        private volatile string _alias;
        private volatile string _name;
        private volatile string _source = DefaultSource;

        /// <summary>
        /// Gets the graph rebinding name to use in graph queries.
        /// </summary>
        public string Alias
        {
            get { return _alias; }
        }

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
        /// Sets the graph traversal source name in graph queries.
        /// </summary>
        public string Source
        {
            get { return _source; }
        }

        /// <summary>
        /// Creates a new instance of <see cref="GraphOptions"/>.
        /// </summary>
        public GraphOptions()
        {
            RebuildDefaultPayload();
        }

        /// <summary>
        /// Sets the graph alias to use in graph queries.
        /// If you don't call this method, it is left unset.
        /// </summary>
        public GraphOptions SetAlias(string alias)
        {
            _alias = alias;
            RebuildDefaultPayload();
            return this;
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
        /// Sets the graph traversal source name to use in graph queries.
        /// If you don't call this method, it defaults to <see cref="DefaultSource"/>.
        /// </summary>
        public GraphOptions SetSource(string source)
        {
            _source = source ?? DefaultSource;
            RebuildDefaultPayload();
            return this;
        }

        internal IDictionary<string, byte[]> BuildPayload(IGraphStatement statement)
        {
            if (statement.GraphAlias == null && statement.GraphLanguage == null && statement.GraphName == null &&
                statement.GraphSource == null)
            {
                if (!statement.IsSystemQuery || !_defaultPayload.ContainsKey(PayloadKey.Name))
                {
                    //The user has not used the graph settings at statement level
                    //Or is a system query but there isn't a name defined at GraphOptions level
                    return _defaultPayload;   
                }
            }
            var payload = new Dictionary<string, byte[]>();
            Add(payload, PayloadKey.Alias, statement.GraphAlias);
            Add(payload, PayloadKey.Language, statement.GraphLanguage);
            if (!statement.IsSystemQuery)
            {
                Add(payload, PayloadKey.Name, statement.GraphName);
            }
            Add(payload, PayloadKey.Source, statement.GraphSource);
            return payload;
        }

        private void Add(IDictionary<string, byte[]> payload, string key, string value)
        {
            byte[] payloadValue = null;
            if (value != null)
            {
                payloadValue = ToUtf8Buffer(value);
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
            if (_alias != null)
            {
                payload.Add(PayloadKey.Alias, ToUtf8Buffer(_alias));
            }
            _defaultPayload = payload;
        }

        private static byte[] ToUtf8Buffer(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }
    }
}
