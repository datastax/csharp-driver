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
using System.Reflection;

namespace Cassandra
{

    public class ServerErrorException : QueryValidationException
    {
        public ServerErrorException(string Message) : base(Message) { }
    }

    public class ProtocolErrorException : QueryValidationException
    {
        public ProtocolErrorException(string Message) : base(Message) { }
    }

    public class OverloadedException : QueryValidationException
    {
        public OverloadedException(string Message) : base(Message) { }
    }

    public class IsBootstrappingException : QueryValidationException
    {
        public IsBootstrappingException(string Message) : base(Message) { }
    }

}

namespace Cassandra
{
    internal enum CassandraErrorType
    {
        ServerError = 0x0000,
        ProtocolError = 0x000A,
        BadCredentials = 0x0100,
        UnavailableException = 0x1000,
        Overloaded = 0x1001,
        IsBootstrapping = 0x1002,
        TruncateError = 0x1003,        
        WriteTimeout = 0x1100,
        ReadTimeout = 0x1200,
        SyntaxError = 0x2000,
        Unauthorized = 0x2100,
        Invalid = 0x2200,
        ConfigError = 0x2300,
        AlreadyExists = 0x2400,
        Unprepared = 0x2500
    }

    internal abstract class OutputError : IOutput, IWaitableForDispose
    {
        public CassandraErrorType CassandraErrorType = CassandraErrorType.Invalid;
        public string Message = "";
        internal OutputError() { }

        internal static OutputError CreateOutputError(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            var tpy = Assembly.GetExecutingAssembly().GetType("Cassandra.Output" + code.ToString());
            if (tpy == null)
                throw new DriverInternalError("unknown error" + code.ToString());
            var cnstr = tpy.GetConstructor(new Type[] { });
            var outp = (OutputError)cnstr.Invoke(new object[] { });
            tpy.GetField("CassandraErrorType").SetValue(outp, code);
            tpy.GetField("Message").SetValue(outp, message);
            var loadM = tpy.GetMethod("Load", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, null, new Type[] { typeof(CassandraErrorType), typeof(string), typeof(BEBinaryReader) }, null);
            if (loadM != null)
                loadM.Invoke(outp, new object[] { code, message, cb });
            return outp;
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }

        public abstract DriverException CreateException();
    }


    internal class OutputServerError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ServerErrorException(Message);
        }
    }

    internal class OutputProtocolError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ProtocolErrorException(Message);
        }
    }

    internal class OutputBadCredentials : OutputError
    {
        public override DriverException CreateException()
        {
            return new AuthenticationException(Message);  
        }
    }

    internal class UnavailableInfo
    {
        public ConsistencyLevel ConsistencyLevel;
        public int Required;
        public int Alive;
    };

    internal class OutputUnavailableException : OutputError
    {
        readonly UnavailableInfo _info = new UnavailableInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            _info.Required = cb.ReadInt32();
            _info.Alive = cb.ReadInt32();
        }
        public override DriverException CreateException()
        {
            return new UnavailableException(_info.ConsistencyLevel, _info.Required, _info.Alive);
        }
    }

    internal class OutputOverloaded : OutputError
    {
        public override DriverException CreateException()
        {
            return new OverloadedException(Message);
        }
    }

    internal class OutputIsBootstrapping : OutputError
    {
        public override DriverException CreateException()
        {
            return new IsBootstrappingException(Message);
        }
    }

    internal class OutputTruncateError : OutputError
    {
        public override DriverException CreateException()
        {
            return new TruncateException(Message);
        }
    }


    internal class WriteTimeoutInfo
    {
        public ConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public string WriteType;
    };

    internal class OutputWriteTimeout : OutputError
    {
        readonly WriteTimeoutInfo _info = new WriteTimeoutInfo();

        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            _info.Received = cb.ReadInt32();
            _info.BlockFor = cb.ReadInt32();
            _info.WriteType = cb.ReadString();
        }

        public override DriverException CreateException()
        {
            return new WriteTimeoutException(_info.ConsistencyLevel, _info.Received, _info.BlockFor, _info.WriteType);
        }
    }


    internal class ReadTimeoutInfo
    {
        public ConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public bool IsDataPresent;
    };

    internal class OutputReadTimeout : OutputError
    {
        readonly ReadTimeoutInfo _info = new ReadTimeoutInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            _info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            _info.Received = cb.ReadInt32();
            _info.BlockFor = cb.ReadInt32();
            _info.IsDataPresent = cb.ReadByte() != 0;
        }
        public override DriverException CreateException()
        {
            return new ReadTimeoutException(_info.ConsistencyLevel, _info.Received, _info.BlockFor, _info.IsDataPresent);
        }
    }

    internal class OutputSyntaxError : OutputError
    {
        public override DriverException CreateException()
        {
            return new SyntaxError(Message);
        }
    }

    internal class OutputUnauthorized : OutputError
    {
        public override DriverException CreateException()
        {
            return new UnauthorizedException(Message);
        }
    }

    internal class OutputInvalid : OutputError
    {
        public override DriverException CreateException()
        {
            return new InvalidQueryException(Message);
        }
    }


    internal class OutputConfigError : OutputError
    {
        public override DriverException CreateException()
        {
            return new InvalidConfigurationInQueryException(Message);
        }
    }

    internal class AlreadyExistsInfo
    {
        public string Ks;
        public string Table;
    };

    internal class OutputAlreadyExists : OutputError
    {
        readonly AlreadyExistsInfo _info = new AlreadyExistsInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            _info.Ks = cb.ReadString();
            _info.Table = cb.ReadString();
        }
        public override DriverException CreateException()
        {
            return new AlreadyExistsException(_info.Ks, _info.Table);
        }
    }

    internal class PreparedQueryNotFoundInfo
    {
        public byte[] UnknownID;
    };

    internal class OutputUnprepared : OutputError
    {
        readonly PreparedQueryNotFoundInfo _info = new PreparedQueryNotFoundInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            var len = cb.ReadInt16();
            _info.UnknownID = new byte[len];
            cb.Read(_info.UnknownID, 0, len);
        }
        public override DriverException CreateException()
        {
            return new PreparedQueryNotFoundException(Message, _info.UnknownID);
        }
    }

}
