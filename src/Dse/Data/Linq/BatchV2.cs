//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
{
    internal class BatchV2 : Batch
    {
        private readonly BatchStatement _batchScript = new BatchStatement();

        public override bool IsEmpty
        {
            get { return _batchScript.IsEmpty; }
        }

        internal BatchV2(ISession session) : base(session)
        {
        }

        public override void Append(CqlCommand cqlCommand)
        {
            if (cqlCommand.GetTable().GetTableType() == TableType.Counter)
            {
                _batchType = BatchType.Counter;
            }
            _batchScript.Add(cqlCommand);
        }

        protected override Task<RowSet> InternalExecuteAsync()
        {
            if (_batchScript.IsEmpty)
            {
                return TaskHelper.FromException<RowSet>(new RequestInvalidException("The Batch must contain queries to execute"));
            }
            _batchScript.SetBatchType(_batchType);
            this.CopyQueryPropertiesTo(_batchScript);
            return _session.ExecuteAsync(_batchScript);

        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("BEGIN " + (_batchType == BatchType.Counter ? "COUNTER " : "") + "BATCH");
            foreach (Statement q in _batchScript.Queries)
                sb.AppendLine(q + ";");
            sb.Append("APPLY BATCH");
            return sb.ToString();
        }
    }
}
