//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Text;
using System.Threading.Tasks;
using Dse.Tasks;

namespace Dse.Data.Linq
{
    internal class BatchV1 : Batch
    {
        private readonly StringBuilder _batchScript = new StringBuilder();

        public override bool IsEmpty
        {
            get { return _batchScript.Length == 0; }
        }

        internal BatchV1(ISession session, BatchType batchType) : base(session, batchType)
        {
        }

        public override void Append(CqlCommand cqlCommand)
        {
            if (cqlCommand.GetTable().GetTableType() == TableType.Counter)
                _batchType = BatchType.Counter;
            _batchScript.Append(cqlCommand);
            _batchScript.AppendLine(";");
        }

        protected override Task<RowSet> InternalExecuteAsync()
        {
            return InternalExecuteAsync(Configuration.DefaultExecutionProfileName);
        }
        
        protected override Task<RowSet> InternalExecuteAsync(string executionProfile)
        {
            if (_batchScript.Length == 0)
            {
                return TaskHelper.FromException<RowSet>(new RequestInvalidException("The Batch must contain queries to execute"));
            }
            string cqlQuery = GetCql();
            var stmt = new SimpleStatement(cqlQuery);
            this.CopyQueryPropertiesTo(stmt);
            return _session.ExecuteAsync(stmt, executionProfile);
        }

        private string GetCql()
        {
            var bt = BatchTypeString();
            return "BEGIN " + bt + "BATCH\r\n" +
                   ((_timestamp == null)
                        ? ""
                        : ("USING TIMESTAMP " + (_timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10 + " ")) +
                   _batchScript + "APPLY BATCH";
        }

        public override string ToString()
        {
            return GetCql();
        }
    }
}
