//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

#if !NETCORE
using System;
using System.Data;
using System.Data.Common;

namespace Dse.Data
{
    internal class CqlCommandBuilder : DbCommandBuilder
    {
        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
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
#endif