using System;
using System.Collections.Generic;
using Cassandra.Mapping.Statements;

namespace Cassandra.Mapping
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

        public BatchType BatchType { get; private set; }

        public CqlBatch(MapperFactory mapperFactory, CqlGenerator cqlGenerator)
            :this(mapperFactory, cqlGenerator, BatchType.Logged)
        {
        }

        public CqlBatch(MapperFactory mapperFactory, CqlGenerator cqlGenerator, BatchType type)
        {
            if (mapperFactory == null) throw new ArgumentNullException("mapperFactory");
            if (cqlGenerator == null) throw new ArgumentNullException("cqlGenerator");
            _mapperFactory = mapperFactory;
            _cqlGenerator = cqlGenerator;

            _statements = new List<Cql>();
            BatchType = type;
        }

        public void Insert<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            Insert(poco, true, queryOptions);
        }

        public void Insert<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            Insert(false, insertNulls, poco, queryOptions, null);
        }

        public void Insert<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            Insert(false, insertNulls, poco, queryOptions, ttl);
        }


        public void InsertIfNotExists<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            InsertIfNotExists(poco, true, queryOptions);
        }

        public void InsertIfNotExists<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null)
        {
            Insert(true, insertNulls, poco, queryOptions);
        }

        public void InsertIfNotExists<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null)
        {
            Insert(true, insertNulls, poco, queryOptions, ttl);
        }

        private void Insert<T>(bool ifNotExists, bool insertNulls, T poco, CqlQueryOptions queryOptions = null, int? ttl = null)
        {
            var pocoData = _mapperFactory.PocoDataFactory.GetPocoData<T>();
            var queryIdentifier = string.Format("INSERT ID {0}/{1}", pocoData.KeyspaceName, pocoData.TableName);
            var getBindValues = _mapperFactory.GetValueCollector<T>(queryIdentifier);
            //get values first to identify null values
            var values = getBindValues(poco);
            //generate INSERT query based on null values (if insertNulls set)
            object[] queryParameters;
            var timestamp = queryOptions == null ? null : queryOptions.Timestamp;
            var cql = _cqlGenerator.GenerateInsert<T>(insertNulls, values, out queryParameters, ifNotExists, ttl, timestamp);

            _statements.Add(Cql.New(cql, queryParameters, queryOptions ?? CqlQueryOptions.None));
        }

        public void Update<T>(T poco, CqlQueryOptions queryOptions = null)
        {
            // Get statement and bind values from POCO
            string cql = _cqlGenerator.GenerateUpdate<T>();
            Func<T, object[]> getBindValues = _mapperFactory.GetValueCollector<T>(cql, primaryKeyValuesLast: true);
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