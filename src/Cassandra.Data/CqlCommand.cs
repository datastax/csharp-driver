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
﻿using System.Linq;
﻿using System.Text;
using System.Data;
using System.Data.Common;
﻿using Cassandra;
﻿using System.Text.RegularExpressions;
using System.Threading;

namespace Cassandra.Data
{
    /// <summary>
    /// Represents an CQL statement to execute against Cassandra
    /// </summary>
    public sealed class CqlCommand : DbCommand
    {
        internal CqlConnection CqlConnection;
        internal CqlBatchTransaction CqlTransaction;
        private string _commandText;
        private ConsistencyLevel _consistencyLevel = ConsistencyLevel.One;
        private static readonly Regex RegexParseParameterName = new Regex(@"\B:[a-zA-Z][a-zA-Z0-9_]*", RegexOptions.Compiled | RegexOptions.Multiline);
        private PreparedStatement _preparedStatement;
        private readonly CqlParameterCollection _parameters = new CqlParameterCollection();

        public override void Cancel()
        {
        }

        /// <inheritdoc />
        public override string CommandText
        {
            get
            {
                return _commandText;
            }
            set
            {
                _preparedStatement = null;
                _commandText = value;
            }
        }

        /// <summary>
        /// Gets or sets the ConsistencyLevel when executing the current <see cref="CqlCommand"/>.
        /// </summary>
        public ConsistencyLevel ConsistencyLevel
        {
            get { return _consistencyLevel; }
            set { _consistencyLevel = value; }
        }

        /// <summary>
        /// Gets whether this command has been prepared.
        /// </summary>
        public bool IsPrepared {
            get { return Parameters.Count == 0 || _preparedStatement != null; }
        }

        /// <summary>
        /// Gets the <see cref="CqlParameter"/>s.
        /// </summary>
        public new CqlParameterCollection Parameters
        {
            get { return _parameters; }
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
            return new CqlParameter();
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
            get { return _parameters; }
        }

        protected override DbTransaction DbTransaction
        {
            get
            {
                return CqlTransaction;
            }
            set
            {
                CqlTransaction = (value as CqlBatchTransaction);
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
            Prepare();

            RowSet rowSet;
            if (_preparedStatement == null)
            {
                rowSet = CqlConnection.ManagedConnection.Execute(_commandText, ConsistencyLevel);
            }
            else //if _preparedStatement != null
            {
                
                var query = _preparedStatement.Bind(GetParameterValues());
                query.SetConsistencyLevel(ConsistencyLevel);
                rowSet = CqlConnection.ManagedConnection.Execute(query);
            }
            
            return new CqlReader(rowSet);
        }

        public override int ExecuteNonQuery()
        {
            Prepare();

            var cm = _commandText.ToUpper().TrimStart();
            var managedConnection = CqlConnection.ManagedConnection;

            if (_preparedStatement == null)
            {
                if (cm.StartsWith("CREATE ")
                    || cm.StartsWith("DROP ")
                    || cm.StartsWith("ALTER "))
                    managedConnection.WaitForSchemaAgreement(managedConnection.Execute(_commandText, ConsistencyLevel));
                else
                    managedConnection.Execute(_commandText, ConsistencyLevel);
            }
            else //if _preparedStatement != null
            {
                var query = _preparedStatement.Bind(GetParameterValues());
                query.SetConsistencyLevel(ConsistencyLevel);
                if (cm.StartsWith("CREATE ")
                    || cm.StartsWith("DROP ")
                    || cm.StartsWith("ALTER "))
                    managedConnection.WaitForSchemaAgreement(managedConnection.Execute(query));
                else
                    managedConnection.Execute(query);
            }

            return -1;
        }

        public override object ExecuteScalar()
        {
            Prepare();

            RowSet rowSet;
            if (_preparedStatement == null)
            {
                rowSet = CqlConnection.ManagedConnection.Execute(_commandText, ConsistencyLevel);
            }
            else //if _preparedStatement != null
            {
                var query = _preparedStatement.Bind(GetParameterValues());
                query.SetConsistencyLevel(ConsistencyLevel);
                rowSet = CqlConnection.ManagedConnection.Execute(query);
            }

            // return the first field value of the first row if exists
            if (rowSet == null)
            {
                return null;
            }
            var row = rowSet.GetRows().FirstOrDefault();
            if (row == null || !row.Any())
            {
                return null;
            }
            return row[0];
        }

        public override void Prepare()
        {
            if (CqlConnection == null)
            {
                throw new InvalidOperationException("No connection bound to this command!");
            }

            if (!IsPrepared && Parameters.Count > 0)
            {
                // replace all named parameter names to ? before preparing the statement 
                // when binding the parameter values got from GetParameterValues() for executing
                // GetParameterValues() returns the array of parameter values
                // order by the occurence of parameter names in cql
                // so that we could support cases that parameters of the same name occur multiple times in cql
                var cqlQuery = RegexParseParameterName.Replace(CommandText, "?");
                _preparedStatement = CqlConnection.CreatePreparedStatement(cqlQuery);
            }
        }

        private object[] GetParameterValues()
        {
            if (Parameters.Count == 0)
            {
                return null;
            }

            // returns the parameter values as an array order by the occurence of parameter names in CommandText
            var matches = RegexParseParameterName.Matches(CommandText);
            var values = new List<object>();
            foreach (Match match in matches)
            {
                object value = null;
                foreach (IDataParameter p in Parameters)
                {
                    if (string.Compare(match.Value, p.ParameterName, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        value = p.Value;
                        break;
                    }
                }
                values.Add(value);
            }
            return values.ToArray();
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
