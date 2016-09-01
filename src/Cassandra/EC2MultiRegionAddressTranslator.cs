using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace Cassandra
{
    public interface IDnsWrapper
    {
        IPHostEntry GetHostEntry(string hostNameOrAddress);
        IPHostEntry GetHostEntry(IPAddress address);
    }

    public class DnsWrapper : IDnsWrapper
    {
        public IPHostEntry GetHostEntry(string hostNameOrAddress)
        {
            return Dns.GetHostEntry(hostNameOrAddress);
        }

        public IPHostEntry GetHostEntry(IPAddress address)
        {
            return Dns.GetHostEntry(address);
        }
    }

    public class EC2MultiRegionAddressTranslator : IAddressTranslator
    {
        private static readonly Logger _logger = new Logger(typeof(EC2MultiRegionAddressTranslator));

        private readonly IDnsWrapper _dns;

        public EC2MultiRegionAddressTranslator()
        {
            _dns = new DnsWrapper();
        }

        public EC2MultiRegionAddressTranslator(IDnsWrapper dnsWrapper)
        {
            _dns = dnsWrapper;
        }

        public IPEndPoint Translate(IPEndPoint address)
        {
            try
            {
                string DomainName = _dns.GetHostEntry(address.Address).HostName;
                IPAddress translatedAddress = _dns.GetHostEntry(DomainName).AddressList.First();
                _logger.Verbose("Resolved {0} to {1}", address.Address, translatedAddress);
                return new IPEndPoint(translatedAddress, address.Port);
            }
            catch (SocketException)
            {
                _logger.Warning("Domain name not found for {0}, returning it as-is.", address.Address);
                return address;
            }
            catch (Exception ex)
            {
                _logger.Warning("Error translating {0}, returning it as-is. Exception: {1}", address.Address, ex.Message);
                return address;
            }
        }
    }
}
