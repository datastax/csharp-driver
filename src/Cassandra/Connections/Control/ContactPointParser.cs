// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System;
using System.Collections.Generic;
using System.Net;

namespace Cassandra.Connections.Control
{
    internal class ContactPointParser : IContactPointParser
    {
        private readonly IDnsResolver _dnsResolver;
        private readonly ProtocolOptions _protocolOptions;
        private readonly IServerNameResolver _serverNameResolver;
        private readonly bool _keepContactPointsUnresolved;

        public ContactPointParser(
            IDnsResolver dnsResolver, 
            ProtocolOptions protocolOptions, 
            IServerNameResolver serverNameResolver,
            bool keepContactPointsUnresolved)
        {
            _dnsResolver = dnsResolver ?? throw new ArgumentNullException(nameof(dnsResolver));
            _protocolOptions = protocolOptions ?? throw new ArgumentNullException(nameof(protocolOptions));
            _serverNameResolver = serverNameResolver ?? throw new ArgumentNullException(nameof(serverNameResolver));
            _keepContactPointsUnresolved = keepContactPointsUnresolved;
        }

        public IEnumerable<IContactPoint> ParseContactPoints(IEnumerable<object> providedContactPoints)
        {
            var result = new HashSet<IContactPoint>();
            foreach (var contactPoint in providedContactPoints)
            {
                IContactPoint parsedContactPoint;

                switch (contactPoint)
                {
                    case IContactPoint typedContactPoint:
                        parsedContactPoint = typedContactPoint;
                        break;
                    case IPEndPoint ipEndPointContactPoint:
                        parsedContactPoint = new IpLiteralContactPoint(ipEndPointContactPoint, _serverNameResolver);
                        break;
                    case IPAddress ipAddressContactPoint:
                        parsedContactPoint = new IpLiteralContactPoint(ipAddressContactPoint, _protocolOptions, _serverNameResolver);
                        break;
                    case string contactPointText:
                    {
                        if (IPAddress.TryParse(contactPointText, out var ipAddress))
                        {
                            parsedContactPoint = new IpLiteralContactPoint(ipAddress, _protocolOptions, _serverNameResolver);
                        }
                        else
                        {
                            parsedContactPoint = new HostnameContactPoint(
                                _dnsResolver, 
                                _protocolOptions,
                                _serverNameResolver, 
                                _keepContactPointsUnresolved, 
                                contactPointText);
                        }

                        break;
                    }
                    default:
                        throw new InvalidOperationException("Contact points should be either string or IPEndPoint instances");
                }
                
                if (result.Contains(parsedContactPoint))
                {
                    Cluster.Logger.Warning("Found duplicate contact point: {0}. Ignoring it.", contactPoint.ToString());
                    continue;
                }

                result.Add(parsedContactPoint);
            }

            return result;
        }
    }
}