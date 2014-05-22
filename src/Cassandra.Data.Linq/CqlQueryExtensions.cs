//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Linq.Expressions;

namespace Cassandra.Data.Linq
{
    public static class CqlQueryExtensions
    {
        internal static void CopyQueryPropertiesTo(this Statement src, Statement dst)
        {
            dst.EnableTracing(src.IsTracing)
               .SetConsistencyLevel(src.ConsistencyLevel)
               .SetPageSize(src.PageSize)
               .SetPagingState(src.PagingState)
               .SetRetryPolicy(src.RetryPolicy);
            if (src.SerialConsistencyLevel != ConsistencyLevel.Any)
                dst.SetSerialConsistencyLevel(src.SerialConsistencyLevel);
        }

        /// <summary>
        /// Projects each element of a sequence into a new form.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by selector.</typeparam>
        /// <param name="source">A CqlQuery&lt;TSource&gt; which after execution returns a sequence of values to invoke a transform function on.</param>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution will return an IEnumerable&lt;TSource&gt; whose elements
        /// are the result of invoking the transform function on each element of source.
        /// To execute this CqlQuery use <c>Execute()</c> method.</returns>
        public static CqlQuery<TResult> Select<TSource, TResult>(this CqlQuery<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            var ret = (CqlQuery<TResult>) source.Provider.CreateQuery<TResult>(Expression.Call(
                null, CqlMthHelps.SelectMi,
                new[] {source.Expression, selector}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        ///  Returns a CqlQuery which after execution returns filtered sequence of values based on a predicate.
        ///  To execute this CqlQuery use <c>Execute()</c> method.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The CqlQuery&lt;TSource&gt; to filter.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution will return an IEnumerable&lt;TSource&gt;
        /// that contains elements from the input sequence that satisfy the condition.</returns>
        public static CqlQuery<TSource> Where<TSource>(this CqlQuery<TSource> source, Expression<Func<TSource, bool>> predicate)
        {
            var ret = (CqlQuery<TSource>) source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.WhereMi,
                new[] {source.Expression, predicate}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Returns a CqlScalar which after execution returns the number of elements in a sequence.
        /// To execute this CqlScalar use <c>Execute()</c> method.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The CqlQuery&lt;TSource&gt; to return the first element of.</param>
        /// <returns>a CqlScalar&lt;long&gt; which after execution returns the number of elements in a sequence.</returns>
        public static CqlScalar<long> Count<TSource>(this CqlQuery<TSource> source)
        {
            var ret = new CqlScalar<long>(source.Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Returns a CqlQuery which after execution returns the first element in a sequence that satisfies a specified condition.
        /// To execute this CqlQuery use <c>Execute()</c> method.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The Table&lt;TSource&gt; to return the first element of.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution will return first element in the sequence
        /// that passes the test in the specified predicate function.</returns>
        public static CqlQuerySingleElement<TSource> First<TSource>(this Table<TSource> source, Expression<Func<TSource, bool>> predicate)
        {
            var ret = new CqlQuerySingleElement<TSource>(source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.First_ForCQLTableMi,
                new[] {source.Expression, Expression.Constant(1), predicate})).Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Returns a CqlQuery which after execution will return the first element of the sequence that satisfies a condition
        /// or a default value if no such element is found.
        /// To execute this CqlQuery use <c>Execute()</c> method.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The Table&lt;TSource&gt;  to return the first element of.</param>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution will return <c>default(TSource)</c> if source is empty
        /// or if no element passes the test specified by predicate,
        /// otherwise the first element in source that passes the test specified by predicate.</returns>
        public static CqlQuerySingleElement<TSource> FirstOrDefault<TSource>(this Table<TSource> source, Expression<Func<TSource, bool>> predicate)
        {
            var ret = new CqlQuerySingleElement<TSource>(source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.FirstOrDefault_ForCQLTableMi,
                new[] {source.Expression, Expression.Constant(1), predicate})).Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Returns a CqlQuery which after execution will return the first element in a sequence.
        /// To execute this CqlQuery use <c>Execute()</c> method.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The CqlQuery&lt;TSource&gt; to return the first element of.</param>        
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution will return first element in the sequence.</returns>
        public static CqlQuerySingleElement<TSource> First<TSource>(this CqlQuery<TSource> source)
        {
            var ret = new CqlQuerySingleElement<TSource>(source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.FirstMi,
                new[] {source.Expression, Expression.Constant(1)})).Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Returns a CqlQuery which after execution will return the first element of a sequence,
        /// or a default value if the sequence contains no elements.
        /// To execute this CqlQuery use <c>Execute()</c> method.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The CqlQuery&lt;TSource&gt; to return the first element of.</param>        
        /// <returns><c>a CqlQuery&lt;TSource&gt; which after execution will return default(TSource)</c> if source is empty,
        /// otherwise the first element in source.</returns>
        public static CqlQuerySingleElement<TSource> FirstOrDefault<TSource>(this CqlQuery<TSource> source)
        {
            var ret = new CqlQuerySingleElement<TSource>(source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.FirstOrDefaultMi,
                new[] {source.Expression, Expression.Constant(1)})).Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        public static CqlDelete Delete<TSource>(this CqlQuery<TSource> source)
        {
            var ret = new CqlDelete(source.Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        public static CqlDelete DeleteIf<TSource>(this CqlQuery<TSource> source, Expression<Func<TSource, bool>> predicate)
        {
            var ret = new CqlDelete(Expression.Call(
                null, CqlMthHelps.DeleteIfMi,
                 new Expression[] { source.Expression, predicate }), source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        public static CqlUpdate Update<TSource>(this CqlQuery<TSource> source)
        {
            var ret = new CqlUpdate(source.Expression, source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        public static CqlUpdate UpdateIf<TSource>(this CqlQuery<TSource> source, Expression<Func<TSource, bool>> predicate)
        {
            var ret = new CqlUpdate(Expression.Call(
                null, CqlMthHelps.UpdateIfMi,
                new[] {source.Expression, predicate}), source.Provider);
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Returns a CqlQuery which after execution will return IEnumerable&lt;TSource&gt;
        /// with specified number of contiguous elements from the start of a sequence.
        /// To execute this CqlQuery use <c>Execute()</c> method.
        /// </summary>        
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">The CqlQuery&lt;TSource&gt; to return the first element of.</param>
        /// <param name="count">The number of elements to return.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution will return IEnumerable&lt;TSource&gt;
        /// with specified number of contiguous elements from the start of a sequence.</returns>
        public static CqlQuery<TSource> Take<TSource>(this CqlQuery<TSource> source, int count)
        {
            var ret = (CqlQuery<TSource>) source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.TakeMi,
                new[] {source.Expression, Expression.Constant(count)}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Sorts the elements, which are returned from CqlQuery, in ascending order according to a key.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TKey">The type of the key returned by keySelector.</typeparam>
        /// <param name="source">A sequence of values to order, returned from CqlQuery&lt;TSource&gt;.</param>
        /// <param name="keySelector">A function to extract a key from an element.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution returns an IEnumerable&lt;TSource&gt; sorted in ascending manner according to a key.</returns>
        public static CqlQuery<TSource> OrderBy<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            var ret = (CqlQuery<TSource>) source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.OrderByMi,
                new[] {source.Expression, keySelector}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        /// <summary>
        /// Sorts the elements, which are returned from CqlQuery, in ascending order according to a key.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <typeparam name="TKey">The type of the key returned by keySelector.</typeparam>
        /// <param name="source">A sequence of values to order, returned from CqlQuery&lt;TSource&gt;.</param>
        /// <param name="func">A function to extract a key from an element.</param>
        /// <returns>a CqlQuery&lt;TSource&gt; which after execution returns an IEnumerable&lt;TSource&gt; sorted in descending manner according to a key.</returns>
        public static CqlQuery<TSource> OrderByDescending<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            var ret = (CqlQuery<TSource>) source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.OrderByDescendingMi,
                new[] {source.Expression, func}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }


        public static CqlQuery<TSource> ThenBy<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            var ret = (CqlQuery<TSource>) source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.ThenByMi,
                new[] {source.Expression, func}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }

        public static CqlQuery<TSource> ThenByDescending<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            var ret = (CqlQuery<TSource>) source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.ThenByDescendingMi,
                new[] {source.Expression, func}));
            source.CopyQueryPropertiesTo(ret);
            return ret;
        }
    }
}
