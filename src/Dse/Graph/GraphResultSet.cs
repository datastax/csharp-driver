//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse.Graph
{
    /// <summary>
    /// Represents the result set containing the Graph results returned from a query.
    /// </summary>
    public class GraphResultSet : IEnumerable<GraphNode>
    {
        private readonly RowSet _rs;

        /// <summary>
        /// Gets the execution information for the query execution.
        /// </summary>
        public ExecutionInfo Info
        {
            get { return _rs.Info; }
        }

        /// <summary>
        /// Creates a new instance of <see cref="GraphResultSet"/>.
        /// </summary>
        public GraphResultSet(RowSet rs)
        {
            if (rs == null)
            {
                throw new ArgumentNullException("rs");
            }
            _rs = rs;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<GraphNode> GetEnumerator()
        {
            return _rs.Select(ParseRow).GetEnumerator();
        }

        private static GraphNode ParseRow(Row row)
        {
            return new GraphNode(row.GetValue<string>("gremlin"));
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
