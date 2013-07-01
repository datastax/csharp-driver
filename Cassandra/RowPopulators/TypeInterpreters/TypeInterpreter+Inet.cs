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
                return new IPEndPoint(ip, 0);
            }
            else
            {
                var length = value[0];
                IPAddress ip;
                int port;
                var buf = new byte[length];
                if (length == 4)
                {
                    Buffer.BlockCopy(value, 1, buf, 0, 4);
                    ip = new IPAddress(buf);
                    port = BytesToInt32(buf, 1 + 4);
                    return new IPEndPoint(ip, port);
                }
                else if (length == 16)
                {
                    Buffer.BlockCopy(value, 1, buf, 0, 16);
                    ip = new IPAddress(buf);
                    port = BytesToInt32(buf, 1 + 16);
                    return new IPEndPoint(ip, port);
                }
            }
            throw new DriverInternalError("Invalid lenght of Inet Addr");
        }

        public static Type GetDefaultTypeFromInet(IColumnInfo type_info)
        {
            return typeof(IPEndPoint);
        }

        public static byte[] InvConvertFromInet(IColumnInfo type_info, object value)
        {
            CheckArgument<IPEndPoint>(value);
            var addrbytes = (value as IPEndPoint).Address.GetAddressBytes();
            var port = Int32ToBytes((value as IPEndPoint).Port);
            var ret = new byte[addrbytes.Length + 4 + 1];
            ret[0] = (byte)addrbytes.Length;
            Buffer.BlockCopy(addrbytes, 0, ret, 1, addrbytes.Length);
            Buffer.BlockCopy(port, 0, ret, 1 + addrbytes.Length, port.Length);
            return ret;
        }
    }
}
