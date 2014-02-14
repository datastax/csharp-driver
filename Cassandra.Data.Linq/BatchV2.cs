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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    internal class BatchV2 : Batch
    {
        internal BatchV2(Session session) : base(session) {}

        private readonly BatchStatement _batchScript = new BatchStatement();

        public override void Append(CqlCommand cqlCommand)
        {
            if (cqlCommand.GetTable().GetTableType() == TableType.Counter)
                _batchType = BatchType.Counter;
            _batchScript.AddQuery(cqlCommand);
        }

        public override bool IsEmpty { get { return _batchScript.IsEmpty; } }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("BEGIN " + (_batchType == BatchType.Counter ? "COUNTER " : "") + "BATCH");
            foreach (var q in _batchScript.Queries)
                sb.AppendLine(q.ToString() + ";");
            sb.Append("APPLY BATCH");
            return sb.ToString();
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            if (_batchScript.IsEmpty)
                throw new ArgumentException("Batch is empty");

            return _session.BeginExecute(_batchScript.SetBatchType(_batchType).EnableTracing(IsTracing).SetConsistencyLevel(ConsistencyLevel),
                                    new CqlQueryTag() { Session = _session }, callback, state);
        }

    }
}
