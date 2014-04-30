//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;
using System.Net;

namespace Cassandra
{

    internal partial class TypeInterpreter
    {
        public static object ConvertFromInet(IColumnInfo type_info, byte[] value, Type cSharpType)
        {
            if (value.Length == 4 || value.Length == 16)
            {
                var ip = new IPAddress(value);
                return ip;
            }
            throw new DriverInternalError("Invalid lenght of Inet Addr");
        }

        public static Type GetDefaultTypeFromInet(IColumnInfo type_info)
        {
            return typeof(IPAddress);
        }

        /// <summary>
        /// Converts a value from CLR IPAddress to byte BE Byte array
        /// </summary>
        public static byte[] InvConvertFromInet(IColumnInfo type_info, object value)
        {
            CheckArgument<IPAddress>(value);
            return (value as IPAddress).GetAddressBytes();
        }
    }
}
