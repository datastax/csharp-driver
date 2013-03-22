using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace Cassandra.Data
{
    public sealed class CqlBatchTransaction : DbTransaction
    {
        internal CqlConnection CqlConnection;
        List<CqlCommand> commands = new List<CqlCommand>();

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
            foreach (var cmd in commands)
                cmd.ExecuteNonQuery();
            commands.Clear();
            CqlConnection.ClearDbTransaction();
        }

        protected override DbConnection DbConnection
        {
            get { return CqlConnection; }
        }

        public override System.Data.IsolationLevel IsolationLevel
        {
            get { return System.Data.IsolationLevel.Unspecified; }
        }

        public override void Rollback()
        {
            commands.Clear();
            CqlConnection.ClearDbTransaction();
        }
    }
}
