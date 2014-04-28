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
    internal class BatchV1 : Batch
    {
        private readonly StringBuilder _batchScript = new StringBuilder();

        public override bool IsEmpty
        {
            get { return _batchScript.Length == 0; }
        }

        internal BatchV1(ISession session) : base(session)
        {
        }

        public override void Append(CqlCommand cqlCommand)
        {
            if (cqlCommand.GetTable().GetTableType() == TableType.Counter)
                _batchType = BatchType.Counter;
            _batchScript.Append(cqlCommand);
            _batchScript.AppendLine(";");
        }

        public override IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            //TODO: Inherit from SimpleStatement
            if (_batchScript.Length != 0)
            {
                var ctx = _session;
                string cqlQuery = GetCql();
                var stmt = new SimpleStatement(cqlQuery);
                this.CopyQueryPropertiesTo(stmt);
                return ctx.BeginExecute(stmt,
                                        new CqlQueryTag {Session = ctx}, callback, state);
            }
            throw new ArgumentOutOfRangeException();
        }

        private string GetCql()
        {
            string bt = _batchType == BatchType.Counter ? "COUNTER" : "";
            return "BEGIN " + bt + "BATCH\r\n" +
                   ((_timestamp == null)
                        ? ""
                        : ("USING TIMESTAMP " + (_timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10 + " ")) +
                   _batchScript + "APPLY " + bt + "BATCH";
        }

        public override string ToString()
        {
            return GetCql();
        }
    }
}