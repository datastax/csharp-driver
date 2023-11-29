using System;
using System.Threading.Tasks;

namespace Cassandra
{
    public interface IRequestTracker
    {
        Task OnStartAsync(RequestTrackingInfo request);

        Task OnSuccessAsync(RequestTrackingInfo request);

        Task OnErrorAsync(RequestTrackingInfo request, Exception ex);

        Task OnNodeSuccessAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo);

        Task OnNodeErrorAsync(RequestTrackingInfo request, HostTrackingInfo hostInfo, Exception ex);
    }
}
