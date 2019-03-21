// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Threading.Tasks;

namespace Dse.Insights
{
    internal interface IInsightsClient : IDisposable
    {
        void Init();

        Task ShutdownAsync();
    }
}