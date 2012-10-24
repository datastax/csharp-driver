using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Cassandra
{
    public class CassandraErrorException<ErrorInfoT> : CassandraException
    {
        public ErrorInfoT ErrorInfo;
        public CassandraErrorException(string Message, ErrorInfoT ErrorInfo)
            : base(Message)
        {
            this.ErrorInfo = ErrorInfo;
        }
    }

    public class CassandraEmptyErrorInfo
    {
        public static CassandraEmptyErrorInfo Value = new CassandraEmptyErrorInfo();
    };

    public class CassandraServerErrorException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraServerErrorException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraProtocolErrorException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraProtocolErrorException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }


    public class CassandraUnavailableInfo
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Required;
        public int Alive;
    };

    public class CassandraUnavailableException : CassandraErrorException<CassandraUnavailableInfo>
    {
        public CassandraUnavailableException(string Message, CassandraUnavailableInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraOverloadedException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraOverloadedException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraIsBootstrappingException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraIsBootstrappingException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraTruncateException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraTruncateException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraWriteTimeoutInfo
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public string WriteType;
    };

    public class CassandraWriteTimeoutException : CassandraErrorException<CassandraWriteTimeoutInfo>
    {
        public CassandraWriteTimeoutException(string Message, CassandraWriteTimeoutInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraReadTimeoutInfo
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public bool IsDataPresent;
    };

    public class CassandraReadTimeoutException : CassandraErrorException<CassandraReadTimeoutInfo>
    {
        public CassandraReadTimeoutException(string Message, CassandraReadTimeoutInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraSyntaxErrorException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraSyntaxErrorException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }


    public class CassandraUnauthorizedException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraUnauthorizedException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraInvalidException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraInvalidException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraConfigErrorException : CassandraErrorException<CassandraEmptyErrorInfo>
    {
        public CassandraConfigErrorException(string Message) : base(Message, CassandraEmptyErrorInfo.Value) { }
    }

    public class CassandraAlreadyExistsInfo
    {
        public string Ks;
        public string Table;
    };

    public class CassandraAlreadyExistsException : CassandraErrorException<CassandraAlreadyExistsInfo>
    {
        public CassandraAlreadyExistsException(string Message, CassandraAlreadyExistsInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraUnpreparedInfo
    {
        public byte[] UnknownID;
    };

    public class CassandraUnpreparedException : CassandraErrorException<CassandraUnpreparedInfo>
    {
        public CassandraUnpreparedException(string Message, CassandraUnpreparedInfo Info) :
            base(Message, Info) { }
    }
}

namespace Cassandra.Native
{
    internal enum CassandraErrorType
    {
        ServerError = 0x0000,
        ProtocolError = 0x000A,
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
        public CassandraErrorType CassandraErrorType;
        public string Message;
        internal OutputError() { }

        internal static OutputError CreateOutputError(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            var tpy = Assembly.GetExecutingAssembly().GetType("Cassandra.Native.Output" + code.ToString());
            var cnstr = tpy.GetConstructor(new Type[] { });
            var outp = (OutputError)cnstr.Invoke(new object[] { });
            tpy.GetField("CassandraErrorType").SetValue(outp, code);
            tpy.GetField("Message").SetValue(outp, message);
            var loadM = tpy.GetMethod("Load", new Type[] { typeof(CassandraErrorType), typeof(string), typeof(BEBinaryReader) });
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

        public abstract CassandraException CreateException();
    }


    internal class OutputServerError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraServerErrorException(Message);
        }
    }

    internal class OutputProtocolError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraProtocolErrorException(Message);
        }
    }

    internal class OutputUnavailableException : OutputError
    {
        CassandraUnavailableInfo info = new CassandraUnavailableInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            info.Required = cb.ReadInt32();
            info.Alive = cb.ReadInt32();
        }
        public override CassandraException CreateException()
        {
            return new CassandraUnavailableException(Message, info);
        }
    }

    internal class OutputOverloaded : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOverloadedException(Message);
        }
    }

    internal class OutputIsBootstrapping : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraIsBootstrappingException(Message);
        }
    }

    internal class OutputTruncateError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraTruncateException(Message);
        }
    }

    internal class OutputWriteTimeout : OutputError
    {
        CassandraWriteTimeoutInfo info = new CassandraWriteTimeoutInfo();

        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            info.Received = cb.ReadInt32();
            info.BlockFor = cb.ReadInt32();
            info.WriteType = cb.ReadString();
        }

        public override CassandraException CreateException()
        {
            return new CassandraWriteTimeoutException(Message, info);
        }
    }

    internal class OutputReadTimeout : OutputError
    {
        CassandraReadTimeoutInfo info = new CassandraReadTimeoutInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            info.Received = cb.ReadInt32();
            info.BlockFor = cb.ReadInt32();
            info.IsDataPresent = cb.ReadByte() != 0;
        }
        public override CassandraException CreateException()
        {
            return new CassandraReadTimeoutException(Message, info);
        }
    }

    internal class OutputSyntaxError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraSyntaxErrorException(Message);
        }
    }

    internal class OutputUnauthorized : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraUnauthorizedException(Message);
        }
    }

    internal class OutputInvalid : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraInvalidException(Message);
        }
    }


    internal class OutputConfigError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraConfigErrorException(Message);
        }
    }

    internal class OutputAlreadyExists : OutputError
    {
        CassandraAlreadyExistsInfo info = new CassandraAlreadyExistsInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.Ks = cb.ReadString();
            info.Table = cb.ReadString();
        }
        public override CassandraException CreateException()
        {
            return new CassandraAlreadyExistsException(Message, info);
        }
    }

    internal class OutputUnprepared : OutputError
    {
        CassandraUnpreparedInfo info = new CassandraUnpreparedInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.UnknownID = new byte[2];
            cb.Read(info.UnknownID, 0, 2);
        }
        public override CassandraException CreateException()
        {
            return new CassandraUnpreparedException(Message, info);
        }
    }

}
