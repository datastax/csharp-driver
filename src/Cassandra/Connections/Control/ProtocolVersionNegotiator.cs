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

using System.Threading.Tasks;
using Cassandra.Serialization;

namespace Cassandra.Connections.Control
{
    internal class ProtocolVersionNegotiator : IProtocolVersionNegotiator
    {
        public async Task<IConnection> NegotiateVersionAsync(
            Configuration config,
            Metadata metadata,
            IConnection connection,
            ISerializerManager serializer)
        {
            var commonVersion = serializer.CurrentProtocolVersion.GetHighestCommon(config, metadata.Hosts);
            if (commonVersion != serializer.CurrentProtocolVersion)
            {
                // Current connection will be closed and reopened
                connection = await ChangeProtocolVersion(
                    config, serializer, commonVersion, connection).ConfigureAwait(false);
            }

            return connection;
        }

        public async Task<IConnection> ChangeProtocolVersion(
            Configuration config,
            ISerializerManager serializer,
            ProtocolVersion nextVersion,
            IConnection previousConnection,
            UnsupportedProtocolVersionException ex = null,
            ProtocolVersion? previousVersion = null)
        {
            if (!nextVersion.IsSupported(config) || nextVersion == previousVersion)
            {
                nextVersion = nextVersion.GetLowerSupported(config);
            }

            if (nextVersion == 0)
            {
                if (ex != null)
                {
                    // We have downgraded the version until is 0 and none of those are supported
                    throw ex;
                }

                // There was no exception leading to the downgrade, signal internal error
                throw new DriverInternalError("Connection was unable to STARTUP using protocol version 0");
            }

            ControlConnection.Logger.Info(ex != null
                ? $"{ex.Message}, trying with version {nextVersion:D}"
                : $"Changing protocol version to {nextVersion:D}");

            serializer.ChangeProtocolVersion(nextVersion);

            previousConnection.Dispose();

            var c = config.ConnectionFactory.CreateUnobserved(
                serializer.GetCurrentSerializer(),
                previousConnection.EndPoint,
                config);
            try
            {
                await c.Open().ConfigureAwait(false);
                return c;
            }
            catch
            {
                c.Dispose();
                throw;
            }
        }
    }
}