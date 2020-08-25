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

#endregion

using System;
using System.Collections.Generic;

namespace Cassandra.DataStax.Graph
{
#pragma warning disable 1591

    public class TEnum : EnumWrapper
    {
        private TEnum(string enumValue)
            : base("T", enumValue)
        {
        }

        public static TEnum Id => new TEnum("id");

        public static TEnum Key => new TEnum("key");

        public static TEnum Label => new TEnum("label");

        public static TEnum Value => new TEnum("value");

        private static readonly IDictionary<string, TEnum> Properties = new Dictionary<string, TEnum>
        {
            { "id", TEnum.Id },
            { "key", TEnum.Key },
            { "label", TEnum.Label },
            { "value", TEnum.Value },
        };

        /// <summary>
        /// Gets the T enumeration by value.
        /// </summary>
        public static TEnum GetByValue(string value)
        {
            if (!TEnum.Properties.TryGetValue(value, out var property))
            {
                throw new ArgumentException($"No matching T for value '{value}'");
            }
            return property;
        }
    }


#pragma warning restore 1591
}