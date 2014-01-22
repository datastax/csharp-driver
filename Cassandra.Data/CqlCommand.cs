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
using System.Text;
using System.Data;
using System.Data.Common;
using Cassandra;
using System.Threading;

namespace Cassandra.Data
{
    public sealed class CqlCommand : DbCommand
    {
        internal CqlConnection CqlConnection;
        internal CqlBatchTransaction CqlTransaction;
        private string commandText;

        public override void Cancel()
        {
        }

        public override string CommandText
        {
            get
            {
                return commandText;
            }
            set
            {
                commandText = value;
            }
        }

        public override int CommandTimeout
        {
            get
            {
                return Timeout.Infinite;
            }
            set
            {
            }
        }

        public override CommandType CommandType
        {
            get
            {
                return CommandType.Text;
            }
            set
            {
            }
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException();
        }

        protected override DbConnection DbConnection
        {
            get
            {
                return CqlConnection;
            }
            set
            {
                if (!(value is CqlConnection))
                    throw new InvalidOperationException();

                CqlConnection = (CqlConnection)value;
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { throw new NotSupportedException(); }
        }

        protected override DbTransaction DbTransaction
        {
            get
            {
                return CqlTransaction;
            }
            set
            {
                CqlTransaction = (DbTransaction as CqlBatchTransaction);
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                return true;
            }
            set
            {
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var outp = CqlConnection.ManagedConnection.Execute(commandText);
            return new CqlReader(outp);
        }

        public override int ExecuteNonQuery()
        {
            var cm = commandText.ToUpper().TrimStart();
            if (cm.StartsWith("CREATE ")
                || cm.StartsWith("DROP ")
                || cm.StartsWith("ALTER "))
                CqlConnection.ManagedConnection.WaitForSchemaAgreement(CqlConnection.ManagedConnection.Execute(commandText));
            else
                CqlConnection.ManagedConnection.Execute(commandText);
            return -1;
        }

        public override object ExecuteScalar()
        {
            return CqlConnection.ManagedConnection.Execute(commandText);
        }

        public override void Prepare()
        {
            throw new NotSupportedException();
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                return UpdateRowSource.FirstReturnedRecord;
            }
            set
            {
            }
        }
    }
}
