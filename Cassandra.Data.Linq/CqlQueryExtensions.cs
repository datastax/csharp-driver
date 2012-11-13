using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra.Data
{
    public static class CqlQueryExtensions
    {

        public static CqlQuery<TResult> Select<TSource, TResult>(this CqlQuery<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return (CqlQuery<TResult>)source.Provider.CreateQuery<TResult>(Expression.Call(
                null, CqlMthHelps.SelectMi,
                 new Expression[] { source.Expression, selector }));
        }

        public static CqlQuery<TSource> Where<TSource>(this CqlQuery<TSource> source, Expression<Func<TSource, bool>> predicate)
        {
            return (CqlQuery<TSource>)source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.WhereMi,
                 new Expression[] { source.Expression, predicate }));
        }

        public static CqlScalar<long> Count<TSource>(this CqlQuery<TSource> source)
        {
            return new CqlScalar<long>(source.Expression, source.Provider);
        }

        public static CqlQuery<TSource> Take<TSource>(this CqlQuery<TSource> source, int count)
        {
            return (CqlQuery<TSource>)source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.TakeMi,
                 new Expression[] { source.Expression, Expression.Constant(count) }));
        }

        public static CqlQuery<TSource> OrderBy<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            return (CqlQuery<TSource>)source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.OrderByMi,
                 new Expression[] { source.Expression, func }));
        }

        public static CqlQuery<TSource> OrderByDescending<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            return (CqlQuery<TSource>)source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.OrderByDescendingMi,
                 new Expression[] { source.Expression, func }));
        }

        public static CqlQuery<TSource> ThenBy<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            return (CqlQuery<TSource>)source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.ThenByMi,
                 new Expression[] { source.Expression, func }));
        }

        public static CqlQuery<TSource> ThenByDescending<TSource, TKey>(this CqlQuery<TSource> source, Expression<Func<TSource, TKey>> func)
        {
            return (CqlQuery<TSource>)source.Provider.CreateQuery<TSource>(Expression.Call(
                null, CqlMthHelps.ThenByDescendingMi,
                 new Expression[] { source.Expression, func }));
        }
    }
}
