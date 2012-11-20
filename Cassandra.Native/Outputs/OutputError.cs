using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Cassandra
{
    public class CassandraClusterException<ErrorInfoT> : CassandraException
    {
        public ErrorInfoT ErrorInfo;
        public CassandraClusterException(string Message, ErrorInfoT ErrorInfo)
            : base(Message)
        {
            this.ErrorInfo = ErrorInfo;
        }
    }

    public class CassandraClusterEmptyErrorInfo
    {
        public static CassandraClusterEmptyErrorInfo Value = new CassandraClusterEmptyErrorInfo();
    };

    public class CassandraClusterServerErrorException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterServerErrorException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterProtocolErrorException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterProtocolErrorException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }


    public class CassandraClusterUnavailableInfo
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Required;
        public int Alive;
    };

    public class CassandraClusterUnavailableException : CassandraClusterException<CassandraClusterUnavailableInfo>
    {
        public CassandraClusterUnavailableException(string Message, CassandraClusterUnavailableInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraClusterOverloadedException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterOverloadedException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterIsBootstrappingException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterIsBootstrappingException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterTruncateException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterTruncateException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterWriteTimeoutInfo
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public string WriteType;
    };

    public class CassandraClusterWriteTimeoutException : CassandraClusterException<CassandraClusterWriteTimeoutInfo>
    {
        public CassandraClusterWriteTimeoutException(string Message, CassandraClusterWriteTimeoutInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraClusterReadTimeoutInfo
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public bool IsDataPresent;
    };

    public class CassandraClusterReadTimeoutException : CassandraClusterException<CassandraClusterReadTimeoutInfo>
    {
        public CassandraClusterReadTimeoutException(string Message, CassandraClusterReadTimeoutInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraClusterSyntaxErrorException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterSyntaxErrorException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }


    public class CassandraClusterUnauthorizedException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterUnauthorizedException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterInvalidException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterInvalidException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterConfigErrorException : CassandraClusterException<CassandraClusterEmptyErrorInfo>
    {
        public CassandraClusterConfigErrorException(string Message) : base(Message, CassandraClusterEmptyErrorInfo.Value) { }
    }

    public class CassandraClusterAlreadyExistsInfo
    {
        public string Ks;
        public string Table;
    };

    public class CassandraClusterAlreadyExistsException : CassandraClusterException<CassandraClusterAlreadyExistsInfo>
    {
        public CassandraClusterAlreadyExistsException(string Message, CassandraClusterAlreadyExistsInfo Info) :
            base(Message, Info) { }
    }

    public class CassandraClusterUnpreparedInfo
    {
        public byte[] UnknownID;
    };

    public class CassandraClusterUnpreparedException : CassandraClusterException<CassandraClusterUnpreparedInfo>
    {
        public CassandraClusterUnpreparedException(string Message, CassandraClusterUnpreparedInfo Info) :
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
            if (tpy == null)
                throw new CassandraClientProtocolViolationException("unknown error" + code.ToString());
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

        public abstract CassandraException CreateException();
    }


    internal class OutputServerError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterServerErrorException(Message);
        }
    }

    internal class OutputProtocolError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterProtocolErrorException(Message);
        }
    }

    internal class OutputUnavailableException : OutputError
    {
        CassandraClusterUnavailableInfo info = new CassandraClusterUnavailableInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            info.Required = cb.ReadInt32();
            info.Alive = cb.ReadInt32();
        }
        public override CassandraException CreateException()
        {
            return new CassandraClusterUnavailableException(Message, info);
        }
    }

    internal class OutputOverloaded : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterOverloadedException(Message);
        }
    }

    internal class OutputIsBootstrapping : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterIsBootstrappingException(Message);
        }
    }

    internal class OutputTruncateError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterTruncateException(Message);
        }
    }

    internal class OutputWriteTimeout : OutputError
    {
        CassandraClusterWriteTimeoutInfo info = new CassandraClusterWriteTimeoutInfo();

        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            info.Received = cb.ReadInt32();
            info.BlockFor = cb.ReadInt32();
            info.WriteType = cb.ReadString();
        }

        public override CassandraException CreateException()
        {
            return new CassandraClusterWriteTimeoutException(Message, info);
        }
    }

    internal class OutputReadTimeout : OutputError
    {
        CassandraClusterReadTimeoutInfo info = new CassandraClusterReadTimeoutInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            info.Received = cb.ReadInt32();
            info.BlockFor = cb.ReadInt32();
            info.IsDataPresent = cb.ReadByte() != 0;
        }
        public override CassandraException CreateException()
        {
            return new CassandraClusterReadTimeoutException(Message, info);
        }
    }

    internal class OutputSyntaxError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterSyntaxErrorException(Message);
        }
    }

    internal class OutputUnauthorized : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterUnauthorizedException(Message);
        }
    }

    internal class OutputInvalid : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterInvalidException(Message);
        }
    }


    internal class OutputConfigError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraClusterConfigErrorException(Message);
        }
    }

    internal class OutputAlreadyExists : OutputError
    {
        CassandraClusterAlreadyExistsInfo info = new CassandraClusterAlreadyExistsInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.Ks = cb.ReadString();
            info.Table = cb.ReadString();
        }
        public override CassandraException CreateException()
        {
            return new CassandraClusterAlreadyExistsException(Message, info);
        }
    }

    internal class OutputUnprepared : OutputError
    {
        CassandraClusterUnpreparedInfo info = new CassandraClusterUnpreparedInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.UnknownID = new byte[2];
            cb.Read(info.UnknownID, 0, 2);
        }
        public override CassandraException CreateException()
        {
            return new CassandraClusterUnpreparedException(Message, info);
        }
    }

}
