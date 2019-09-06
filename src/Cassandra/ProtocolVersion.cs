//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

using System;
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Specifies the different protocol versions and provides methods (via extension methods) to check whether a
    /// feature is supported in an specific version.
    /// </summary>
    public enum ProtocolVersion : byte
    {
        /// <summary>
        /// Cassandra protocol v1, supported in Apache Cassandra 1.2-->2.2.
        /// </summary>
        V1 = 0x01,

        /// <summary>
        /// Cassandra protocol v2, supported in Apache Cassandra 2.0-->2.2.
        /// </summary>
        V2 = 0x02,

        /// <summary>
        /// Cassandra protocol v3, supported in Apache Cassandra 2.1-->3.x.
        /// </summary>
        V3 = 0x03,

        /// <summary>
        /// Cassandra protocol v4, supported in Apache Cassandra 2.2-->3.x.
        /// </summary>
        V4 = 0x04,

        /// <summary>
        /// Cassandra protocol v5, in beta from Apache Cassandra 3.x+. Currently not supported by the driver.
        /// </summary>
        V5 = 0x05,

        /// <summary>
        /// The higher protocol version that is supported by this driver.
        /// <para>When acquiring the first connection, it will use this version to start protocol negotiation.</para>
        /// </summary>
        MaxSupported = V4,

        /// <summary>
        /// The lower protocol version that is supported by this driver.
        /// </summary>
        MinSupported = V1
    }

    internal static class ProtocolVersionExtensions
    {
        private static readonly Logger Logger = new Logger(typeof(ProtocolVersion));
        private static readonly Version Version30 = new Version(3, 0);
        private static readonly Version Version22 = new Version(2, 2);
        private static readonly Version Version21 = new Version(2, 1);
        private static readonly Version Version20 = new Version(2, 0);

        /// <summary>
        /// Determines if the protocol version is supported by this driver.
        /// </summary>
        public static bool IsSupported(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V1 && version <= ProtocolVersion.V4;
        }

        /// <summary>
        /// Gets the first version number that is supported, lower than the one provided.
        /// Returns zero when there isn't a lower supported version.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        public static ProtocolVersion GetLowerSupported(this ProtocolVersion version)
        {
            if (version >= ProtocolVersion.V5)
            {
                return ProtocolVersion.V4;
            }
            if (version <= ProtocolVersion.V1)
            {
                return 0;
            }
            return version - 1;
        }

        /// <summary>
        /// Gets the highest supported protocol version collectively by the given hosts.
        /// </summary>
        public static ProtocolVersion GetHighestCommon(this ProtocolVersion version, IEnumerable<Host> hosts)
        {
            var maxVersion = (byte)version;
            var v3Requirement = false;
            var maxVersionWith3OrMore = maxVersion;

            foreach (var host in hosts)
            {
                var cassandraVersion = host.CassandraVersion;
                if (cassandraVersion >= Version30)
                {
                    // Anything 3.0.0+ has a max protocol version of V4 and requires at least V3.
                    v3Requirement = true;
                    maxVersion = Math.Min((byte)ProtocolVersion.V4, maxVersion);
                    maxVersionWith3OrMore = maxVersion;
                }
                else if (cassandraVersion >= Version22)
                {
                    // Cassandra 2.2.x has a max protocol version of V4.
                    maxVersion = Math.Min((byte)ProtocolVersion.V4, maxVersion);
                    maxVersionWith3OrMore = maxVersion;
                }
                else if (cassandraVersion >= Version21)
                {
                    // Cassandra 2.1.x has a max protocol version of V3.
                    maxVersion = Math.Min((byte)ProtocolVersion.V3, maxVersion);
                    maxVersionWith3OrMore = maxVersion;
                }
                else if (cassandraVersion >= Version20)
                {
                    // Cassandra 2.0.x has a max protocol version of V2.
                    maxVersion = Math.Min((byte)ProtocolVersion.V2, maxVersion);
                }
                else
                {
                    // Anything else is < 2.x and requires protocol version V1.
                    maxVersion = Math.Min((byte)ProtocolVersion.V1, maxVersion);
                }
            }

            if (v3Requirement && maxVersion < (byte)ProtocolVersion.V3)
            {
                Logger.Error($"Detected hosts with maximum protocol version of {maxVersion} but there are some hosts " +
                             $"that require at least version 3. Will not be able to connect to these older hosts");
                maxVersion = maxVersionWith3OrMore;
            }

            return (ProtocolVersion) maxVersion;
        }

        /// <summary>
        /// Determines whether the protocol supports partition key indexes in the `prepared` RESULT responses.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        public static bool SupportsPreparedPartitionKey(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V4;
        }

        /// <summary>
        /// Determines whether the protocol supports up to 4 strings (ie: change_type, target, keyspace and table) in
        /// the schema change responses.
        /// </summary>
        public static bool SupportsSchemaChangeFullMetadata(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V3;
        }

        /// <summary>
        /// Determines whether the protocol supports timestamps parameters in BATCH, QUERY and EXECUTE requests.
        /// </summary>
        public static bool SupportsTimestamp(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V3;
        }

        /// <summary>
        /// Determines whether the protocol supports unset parameters.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        public static bool SupportsUnset(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V4;
        }

        /// <summary>
        /// Determines whether the protocol supports BATCH requests.
        /// </summary>
        public static bool SupportsBatch(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V2;
        }

        /// <summary>
        /// Determines if streamIds are serialized used 2 bytes
        /// </summary>
        public static bool Uses2BytesStreamIds(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V3;
        }

        /// <summary>
        /// Determines whether the collection length is encoded using 32 bits.
        /// </summary>
        public static bool Uses4BytesCollectionLength(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V3;
        }

        /// <summary>
        /// Determines whether the QUERY, EXECUTE and BATCH flags are encoded using 32 bits.
        /// </summary>
        public static bool Uses4BytesQueryFlags(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V5;
        }

        /// <summary>
        /// Startup responses using protocol v4+ can be a SERVER_ERROR wrapping a ProtocolException,
        /// this method returns true when is possible to receive such error.
        /// </summary>
        public static bool CanStartupResponseErrorBeWrapped(this ProtocolVersion version)
        {
            return version >= ProtocolVersion.V4;
        }
    }
}
