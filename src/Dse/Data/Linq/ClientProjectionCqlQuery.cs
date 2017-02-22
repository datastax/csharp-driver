//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents a <see cref="CqlQuery{TResult}"/> that uses client projects of a given <see cref="CqlQuery{TSource}"/>.
    /// </summary>
    /// <typeparam name="TSource">Source type</typeparam>
    /// <typeparam name="TResult">Target type</typeparam>
    internal class ClientProjectionCqlQuery<TSource, TResult> : CqlQuery<TResult>, IClientProjectionCqlQuery
    {
        private readonly Expression<Func<TSource, TResult>> _projectionExpression;
        private readonly CqlQuery<TSource> _source;

        /// <summary>
        /// Creates a new instance of <see cref="ClientProjectionCqlQuery{TSource, TResult}"/>.
        /// </summary>
        /// <param name="expression">The complete query expression</param>
        /// <param name="source">The source <see cref="CqlQuery{TSource}"/></param>
        /// <param name="projectionExpression">The projection expression</param>
        internal ClientProjectionCqlQuery(Expression expression, CqlQuery<TSource> source, Expression<Func<TSource, TResult>> projectionExpression) : 
            base(expression, source.Table, source.MapperFactory, source.StatementFactory, source.PocoData)
        {
            _source = source;
            _projectionExpression = projectionExpression;
        }

        internal override IEnumerable<TResult> AdaptResult(string cql, RowSet rs)
        {
            IEnumerable<TSource> sourceData = _source.AdaptResult(cql, rs).ToArray();
            var func = _projectionExpression.Compile();
            return sourceData.Select(func);
        }
    }

    /// <summary>
    /// Represents an internal projection query
    /// </summary>
    internal interface IClientProjectionCqlQuery { }
}
