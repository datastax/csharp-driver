//
//  Copyright (C) 2016-2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Dse.Serialization.Graph;
using Dse.Serialization.Graph.GraphSON1;
using Dse.Serialization.Graph.GraphSON2;
using Newtonsoft.Json;

namespace Dse.Graph
{
    /// <summary>
    /// Represents the result set containing the Graph nodes returned from a query.
    /// </summary>
    public class GraphResultSet : IEnumerable<GraphNode>
    {
        private static readonly JsonSerializer GraphSON1Serializer =
            JsonSerializer.CreateDefault(GraphSON1ContractResolver.Settings);
        private static readonly JsonSerializer GraphSON2Serializer =
            JsonSerializer.CreateDefault(GraphSON2ContractResolver.Settings);
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
            Func<Row, GraphNode> factory = GetGraphSON1Node;
            if (language == GraphOptions.GraphSON2Language)
            {
                factory = GetGraphSON2Node;
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
            return _rs.Select(_factory).GetEnumerator();
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
