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
//

using System;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.Mapping.TypeConversion
{
    /// <summary>
    /// A class that maps strings to enum values.  Uses a cache internally to speed lookups.
    /// </summary>
    public static class EnumStringMapper<T>
    {
        private static readonly Dictionary<string, T> StringToEnumCache;

        static EnumStringMapper()
        {
            StringToEnumCache = Enum.GetValues(typeof(T)).Cast<T>().ToDictionary(val => val.ToString());
        }

        /// <summary>
        /// Converts a string value to an enum of Type T.
        /// </summary>
        public static T MapStringToEnum(string value)
        {
            return StringToEnumCache[value];
        }
    }
}
