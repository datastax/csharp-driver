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
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;

namespace Cassandra.Data
{
    public sealed class CqlCommand : DbCommand
    {
        internal CqlConnection CqlConnection;
        internal CqlBatchTransaction CqlTransaction;
        private string commandText;
        private ConsistencyLevel consistencyLevel = ConsistencyLevel.One;

        public override string CommandText
        {
            get { return commandText; }
            set { commandText = value; }
        }

        /// <summary>
        /// Gets or sets the ConsistencyLevel when executing the current <see cref="CqlCommand"/>.
        /// </summary>
        public ConsistencyLevel ConsistencyLevel
        {
            get { return consistencyLevel; }
            set { consistencyLevel = value; }
        }

        public override int CommandTimeout
        {
            get { return Timeout.Infinite; }
            set { }
        }

        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set { }
        }

        protected override DbConnection DbConnection
        {
            get { return CqlConnection; }
            set
            {
                if (!(value is CqlConnection))
                    throw new InvalidOperationException();

                CqlConnection = (CqlConnection) value;
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { throw new NotSupportedException(); }
        }

        protected override DbTransaction DbTransaction
        {
            get { return CqlTransaction; }
            set { CqlTransaction = (DbTransaction as CqlBatchTransaction); }
        }

        public override bool DesignTimeVisible
        {
            get { return true; }
            set { }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { return UpdateRowSource.FirstReturnedRecord; }
            set { }
        }

        public override void Cancel()
        {
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            RowSet outp = CqlConnection.ManagedConnection.Execute(commandText, ConsistencyLevel);
            return new CqlReader(outp);
        }

        public override int ExecuteNonQuery()
        {
            string cm = commandText.ToUpper().TrimStart();
            if (cm.StartsWith("CREATE ")
                || cm.StartsWith("DROP ")
                || cm.StartsWith("ALTER "))
                CqlConnection.ManagedConnection.WaitForSchemaAgreement(CqlConnection.ManagedConnection.Execute(commandText, ConsistencyLevel));
            else
                CqlConnection.ManagedConnection.Execute(commandText, ConsistencyLevel);
            return -1;
        }

        public override object ExecuteScalar()
        {
            RowSet rowSet = CqlConnection.ManagedConnection.Execute(commandText, ConsistencyLevel);

            // return the first field value of the first row if exists
            if (rowSet == null)
            {
                return null;
            }
            Row row = rowSet.GetRows().FirstOrDefault();
            if (row == null || !row.Any())
            {
                return null;
            }
            return row[0];
        }

        public override void Prepare()
        {
            throw new NotSupportedException();
        }
    }
}