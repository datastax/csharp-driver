﻿#region License

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

#endregion

using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.GraphSON2.Tinkerpop
{
    internal class LocalTimeSerializer : StringBasedSerializer
    {
        private const string Prefix = "gx";
        private const string TypeKey = "LocalTime";

        public LocalTimeSerializer() : base(LocalTimeSerializer.Prefix, LocalTimeSerializer.TypeKey)
        {
        }

        public static string TypeName =>
            GraphSONUtil.FormatTypeName(LocalTimeSerializer.Prefix, LocalTimeSerializer.TypeKey);

        protected override string ToString(dynamic obj)
        {
            LocalTime time = obj;
            return time.ToString();
        }

        protected override dynamic FromString(string str)
        {
            return LocalTime.Parse(str);
        }
    }
}