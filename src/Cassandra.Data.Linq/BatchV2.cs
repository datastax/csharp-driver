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
using System.Text;

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
                _batchType = BatchType.Counter;
            _batchScript.Add(cqlCommand);
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

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            //TODO: Inherit from BatchStatement; replace new instance with this
            //TODO: Batch to interface
            if (_batchScript.IsEmpty)
                throw new ArgumentException("Batch is empty");

            _batchScript.SetBatchType(_batchType);
            this.CopyQueryPropertiesTo(_batchScript);
            return _session.BeginExecute(_batchScript,
                                         new CqlQueryTag {Session = _session}, callback, state);
        }
    }
}