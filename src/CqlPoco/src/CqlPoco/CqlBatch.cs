using System;
using System.Collections.Generic;
using CqlPoco.Mapping;
using CqlPoco.Statements;

namespace CqlPoco
{
    /// <summary>
    /// A batch of CQL statements.
    /// </summary>
    internal class CqlBatch : ICqlBatch
    {
        private readonly MapperFactory _mapperFactory;
        private readonly CqlGenerator _cqlGenerator;

        private readonly List<Cql> _statements;

        public IEnumerable<Cql> Statements
        {
            get { return _statements; }
        }

        public CqlBatch(MapperFactory mapperFactory, CqlGenerator cqlGenerator)
        {
            if (mapperFactory == null) throw new ArgumentNullException("mapperFactory");
            if (cqlGenerator == null) throw new ArgumentNullException("cqlGenerator");
            _mapperFactory = mapperFactory;
            _cqlGenerator = cqlGenerator;

            _statements = new List<Cql>();
        }

        public void Insert<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateInsert<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            _statements.Add(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public void Update<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateUpdate<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql);
            object[] values = getBindValues(poco);

            _statements.Add(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public void Update<T>(string cql, params object[] args)
        {
            Update<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Update<T>(Cql cql)
        {
            _cqlGenerator.PrependUpdate<T>(cql);
            _statements.Add(cql);
        }

        public void Delete<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get the statement and bind values from POCO
            string cql = _cqlGenerator.GenerateDelete<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesOnly: true);
            object[] values = getBindValues(poco);

            _statements.Add(Cql.New(cql, values, queryOptions ?? CqlQueryOptions.None));
        }

        public void Delete<T>(string cql, params object[] args)
        {
            Delete<T>(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Delete<T>(Cql cql)
        {
            _cqlGenerator.PrependDelete<T>(cql);
            _statements.Add(cql);
        }

        public void Execute(string cql, params object[] args)
        {
            _statements.Add(Cql.New(cql, args, CqlQueryOptions.None));
        }

        public void Execute(Cql cql)
        {
            _statements.Add(cql);
        }

        public TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value)
        {
            return _mapperFactory.TypeConverter.ConvertCqlArgument<TValue, TDatabase>(value);
        }
    }
}