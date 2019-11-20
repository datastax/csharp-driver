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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Serialization.Graph.GraphSON1;
using Cassandra.Serialization.Graph.GraphSON2;

namespace Cassandra.DataStax.Graph
{
    /// <summary>
    /// Represents the result set containing the Graph nodes returned from a query.
    /// </summary>
    public class GraphResultSet : IEnumerable<GraphNode>
    {
        private readonly RowSet _rs;
        private readonly Func<Row, GraphNode> _factory;
        
        /// <summary>
        /// Gets the execution information for the query execution.
        /// </summary>
        public ExecutionInfo Info => _rs.Info;

        /// <summary>
        /// Creates a new instance of <see cref="GraphResultSet"/>.
        /// </summary>
        public GraphResultSet(RowSet rs) : this(rs, null)
        {

        }

        private GraphResultSet(RowSet rs, string language)
        {
            _rs = rs ?? throw new ArgumentNullException(nameof(rs));
            Func<Row, GraphNode> factory = GraphResultSet.GetGraphSON1Node;
            if (language == GraphOptions.GraphSON2Language)
            {
                factory = GraphResultSet.GetGraphSON2Node;
            }
            _factory = factory;
        }

        internal static GraphResultSet CreateNew(RowSet rs, IGraphStatement statement, GraphOptions options)
        {
            return new GraphResultSet(rs, statement.GraphLanguage ?? options.Language);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<GraphNode> GetEnumerator()
        {
            return YieldNodes().GetEnumerator();
        }

        /// <summary>
        /// Yields the nodes considering "bulk" property, by returning bulked results more than once.
        /// </summary>
        private IEnumerable<GraphNode> YieldNodes()
        {
            foreach (var node in _rs.Select(_factory))
            {
                for (var i = 0; i < node.Bulk; i++)
                {
                    yield return node;
                }
            }
        }

        /// <summary>
        /// Applies a conversion to each element of the sequence.
        /// </summary>
        /// <returns>
        /// An IEnumerable{T} that contains each element of the source sequence converted to the specified type.
        /// </returns>
        public IEnumerable<T> To<T>()
        {
            return this.Select(node => node.To<T>());
        }

        private static GraphNode GetGraphSON1Node(Row row)
        {
            return new GraphNode(new GraphSON1Node(row.GetValue<string>("gremlin")));
        }

        private static GraphNode GetGraphSON2Node(Row row)
        {
            return new GraphNode(new GraphSON2Node(row.GetValue<string>("gremlin")));
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
