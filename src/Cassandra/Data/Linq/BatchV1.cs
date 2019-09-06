﻿//
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
using System.Text;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra.Data.Linq
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
