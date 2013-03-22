using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;

namespace Cassandra.Data
{
    class CqlCommandBuilder : DbCommandBuilder
    {
        protected override void ApplyParameterInfo(DbParameter parameter, System.Data.DataRow row, System.Data.StatementType statementType, bool whereClause)
        {
            throw new NotSupportedException();
        }

        protected override string GetParameterName(string parameterName)
        {
            throw new NotSupportedException();
        }

        protected override string GetParameterName(int parameterOrdinal)
        {
            throw new NotSupportedException();
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            throw new NotSupportedException();
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            throw new NotSupportedException();
        }
    }
}
