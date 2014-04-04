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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Cassandra.Data
{
    public sealed class CqlBatchTransaction : DbTransaction
    {
        private readonly List<CqlCommand> commands = new List<CqlCommand>();
        internal CqlConnection CqlConnection;

        protected override DbConnection DbConnection
        {
            get { return CqlConnection; }
        }

        public override IsolationLevel IsolationLevel
        {
            get { return IsolationLevel.Unspecified; }
        }

        public CqlBatchTransaction(CqlConnection cqlConnection)
        {
            CqlConnection = cqlConnection;
        }

        public void Append(CqlCommand cmd)
        {
            if (!ReferenceEquals(CqlConnection, cmd.Connection))
                throw new InvalidOperationException();

            commands.Add(cmd);
        }

        public override void Commit()
        {
            foreach (CqlCommand cmd in commands)
                cmd.ExecuteNonQuery();
            commands.Clear();
            CqlConnection.ClearDbTransaction();
        }

        public override void Rollback()
        {
            commands.Clear();
            CqlConnection.ClearDbTransaction();
        }
    }
}