using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse
{
    public interface IDseSession : ISession
    {
        //public GraphResult ExecuteGraph(string query, params object[] parameters);
    }
}
