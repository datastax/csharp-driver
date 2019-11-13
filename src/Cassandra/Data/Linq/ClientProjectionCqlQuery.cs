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
using System.Linq;
using System.Linq.Expressions;

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
