//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Threading.Tasks;
using Dse.SessionManagement;
using Dse.Tasks;

namespace Dse.SessionManagement
{
    internal class DseSessionManager : ISessionManager
    {
        public Task OnInitializationAsync()
        {
            return TaskHelper.Completed;
        }

        public Task OnShutdownAsync()
        {
            return TaskHelper.Completed;
        }
    }
}