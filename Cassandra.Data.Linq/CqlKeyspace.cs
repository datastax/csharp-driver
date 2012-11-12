using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Native;

namespace Cassandra.Data
{
    public class CqlKeyspace
    {
        CqlContext context;
        string keyspace;
        bool selected;

        internal CqlKeyspace(CqlContext context, string keyspace)
        {
            this.keyspace = keyspace;
            this.context = context;
            selected = false;
        }

        public void Create()
        {
            context.CreateKeyspace(keyspace);
        }

        public void CreateIfNotExists()
        {
            try
            {
                Create();
            }
            catch (CassandraOutputException<OutputInvalid>)
            {
                //already exists
            }
        }

        public void Delete()
        {
            context.ExecuteNonQuery(CqlQueryTools.GetDropKeyspaceCQL(keyspace));
        }

        internal void Select()
        {
            if (!selected)
            {
                try
                {
                    selected = true;
                    var ks = context.ExecuteScalar(CqlQueryTools.GetUseKeyspaceCQL(keyspace));
                    if (!ks.Equals(keyspace))
                        throw new InvalidOperationException();
                }
                catch
                {
                    selected = false;
                }
            }
        }

    }
}
