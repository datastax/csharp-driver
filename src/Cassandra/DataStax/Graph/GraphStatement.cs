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
using System.Reflection;
using System.Runtime.CompilerServices;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Graph
{
    /// <summary>
    /// Base class for graph statements.
    /// </summary>
    public abstract class GraphStatement : IGraphStatement
    {
        /// <inheritdoc/>
        public ConsistencyLevel? ConsistencyLevel { get; protected set; }

        /// <inheritdoc/>
        public string GraphAlias { get; protected set; }

        /// <inheritdoc/>
        public string GraphLanguage { get; protected set; }

        /// <inheritdoc/>
        public string GraphName { get; protected set; }

        public GraphProtocol? GraphProtocolVersion { get; protected set; }

        /// <inheritdoc/>
        public ConsistencyLevel? GraphReadConsistencyLevel { get; protected set; }

        /// <inheritdoc/>
        public string GraphSource { get; protected set; }

        /// <inheritdoc/>
        public ConsistencyLevel? GraphWriteConsistencyLevel { get; protected set; }

        /// <inheritdoc/>
        public bool IsSystemQuery { get; protected set; }

        /// <inheritdoc />
        public int ReadTimeoutMillis { get; protected set; }

        /// <inheritdoc />
        public DateTimeOffset? Timestamp { get; protected set; }

        /// <inheritdoc />
        internal abstract IStatement GetIStatement(GraphOptions options);

        /// <summary>
        /// Determines whether the object is anonymous.
        /// </summary>
        /// <exception cref="ArgumentNullException" />
        protected bool IsAnonymous(object obj)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj");
            }
            var type = obj.GetType();
            return type.GetTypeInfo().IsGenericType
                   && (type.GetTypeInfo().Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic
                   && (type.Name.Contains("AnonymousType") || type.Name.Contains("AnonType"))
                   && type.GetTypeInfo().IsDefined(typeof(CompilerGeneratedAttribute), false);
        }

        /// <inheritdoc />
        public IGraphStatement SetConsistencyLevel(ConsistencyLevel consistency)
        {
            ConsistencyLevel = consistency;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetGraphLanguage(string language)
        {
            GraphLanguage = language;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetGraphName(string name)
        {
            GraphName = name;
            return this;
        }

        public IGraphStatement SetGraphProtocolVersion(GraphProtocol graphProtocol)
        {
            GraphProtocolVersion = graphProtocol;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetGraphReadConsistencyLevel(ConsistencyLevel consistency)
        {
            GraphReadConsistencyLevel = consistency;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetGraphSource(string source)
        {
            GraphSource = source;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetGraphSourceAnalytics()
        {
            return SetGraphSource("a");
        }

        /// <inheritdoc />
        public IGraphStatement SetGraphWriteConsistencyLevel(ConsistencyLevel consistency)
        {
            GraphWriteConsistencyLevel = consistency;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetReadTimeoutMillis(int timeout)
        {
            ReadTimeoutMillis = timeout;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetSystemQuery()
        {
            IsSystemQuery = true;
            GraphName = null;
            return this;
        }

        /// <inheritdoc />
        public IGraphStatement SetTimestamp(DateTimeOffset timestamp)
        {
            Timestamp = timestamp;
            return this;
        }

        /// <inheritdoc />
        IStatement IGraphStatement.ToIStatement(GraphOptions options)
        {
            return GetIStatement(options);
        }
    }
}
