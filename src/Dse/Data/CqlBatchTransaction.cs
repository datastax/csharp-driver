//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Dse.Data
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