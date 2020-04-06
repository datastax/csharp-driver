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

using System;
using System.Threading.Tasks;

namespace Cassandra.MetadataHelpers
{
    internal class SchemaParserFactory : ISchemaParserFactory
    {
        private static readonly Version Version30 = new Version(3, 0);

        private static readonly Version Version40 = new Version(4, 0);

        /// <summary>
        /// Creates a new instance if the currentInstance is not valid for the given Cassandra version
        /// </summary>
        public ISchemaParser Create(Version cassandraVersion, Metadata parent,
                                               Func<string, string, Task<UdtColumnInfo>> udtResolver,
                                               ISchemaParser currentInstance = null)
        {
            if (cassandraVersion >= Version40 && !(currentInstance is SchemaParserV3))
            {
                return new SchemaParserV3(parent, udtResolver);
            }
            if (cassandraVersion >= Version30 && !(currentInstance is SchemaParserV2))
            {
                return new SchemaParserV2(parent, udtResolver);
            }
            if (cassandraVersion < Version30 && !(currentInstance is SchemaParserV1))
            {
                return new SchemaParserV1(parent);
            }
            if (currentInstance == null)
            {
                throw new ArgumentNullException(nameof(currentInstance));
            }
            return currentInstance;
        }
    }
}