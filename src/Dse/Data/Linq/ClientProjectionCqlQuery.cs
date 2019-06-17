//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Dse.Data.Linq
{
    /// <summary>
    /// Represents a <see cref="CqlQuery{TResult}"/> that uses client projects of a given <see cref="CqlQuery{TSource}"/>.
    /// </summary>
    /// <typeparam name="TSource">Source type</typeparam>
    /// <typeparam name="TResult">Target type</typeparam>
    internal class ClientProjectionCqlQuery<TSource, TResult> : CqlQuery<TResult>, IClientProjectionCqlQuery
    {
        private readonly Expression<Func<TSource, TResult>> _projectionExpression;
        private readonly bool _canCompile;
        private readonly CqlQuery<TSource> _source;

        /// <summary>
        /// Creates a new instance of <see cref="ClientProjectionCqlQuery{TSource, TResult}"/>.
        /// </summary>
        /// <param name="expression">The complete query expression</param>
        /// <param name="source">The source <see cref="CqlQuery{TSource}"/></param>
        /// <param name="projectionExpression">The projection expression</param>
        /// <param name="canCompile">Determines if the projection can be compiled and the delegate called.</param>
        internal ClientProjectionCqlQuery(Expression expression, CqlQuery<TSource> source, 
            Expression<Func<TSource, TResult>> projectionExpression, bool canCompile) :
            base(expression, source.Table, source.MapperFactory, source.StatementFactory, source.PocoData)
        {
            _source = source;
            _projectionExpression = projectionExpression;
            _canCompile = canCompile;
        }

        internal override IEnumerable<TResult> AdaptResult(string cql, RowSet rs)
        {
            IEnumerable<TResult> result;
            if (!_canCompile)
            {
                var mapper = MapperFactory.GetMapperWithProjection<TResult>(cql, rs, _projectionExpression);
                result = rs.Select(mapper);
            }
            else
            {
                IEnumerable<TSource> sourceData = _source.AdaptResult(cql, rs);
                var func = _projectionExpression.Compile();
                result = sourceData.Select(func);
            }
            var enumerator = result.GetEnumerator();
            // Eagerly evaluate the first one in order to fail fast
            var hasFirstItem = enumerator.MoveNext();
            return YieldFromFirst(enumerator, hasFirstItem);
        }

        private static IEnumerable<TResult> YieldFromFirst(IEnumerator<TResult> enumerator, bool hasFirstItem)
        {
            if (!hasFirstItem)
            {
                yield break;
            }
            yield return enumerator.Current;
            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }
    }

    /// <summary>
    /// Represents an internal projection query
    /// </summary>
    internal interface IClientProjectionCqlQuery { }
}
