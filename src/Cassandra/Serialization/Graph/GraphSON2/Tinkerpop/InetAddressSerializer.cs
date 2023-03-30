#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion License

using System.Net;

using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON2.Tinkerpop
{
    internal class InetAddressSerializer : StringBasedSerializer
    {
        private const string Prefix = "gx";
        private const string TypeKey = "InetAddress";

        public InetAddressSerializer() : base(InetAddressSerializer.Prefix, InetAddressSerializer.TypeKey)
        {
        }

        public static string TypeName =>
            GraphSONUtil.FormatTypeName(InetAddressSerializer.Prefix, InetAddressSerializer.TypeKey);

        protected override string ToString(dynamic obj)
        {
            IPAddress addr = obj;
            return addr.ToString();
        }

        protected override dynamic FromString(string str)
        {
            return IPAddress.Parse(str);
        }
    }
}