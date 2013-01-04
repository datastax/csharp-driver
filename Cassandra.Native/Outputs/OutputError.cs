using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using Cassandra;

namespace Cassandra
{

    public class ServerErrorException : QueryValidationException
    {
        public ServerErrorException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.Rethrow();
        }
    }

    public class ProtocolErrorException : QueryValidationException
    {
        public ProtocolErrorException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.Rethrow();
        }
    }

    public class OverloadedException : QueryValidationException
    {
        public OverloadedException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.Retry(null);
        }
    }

    public class IsBootstrappingException : QueryValidationException
    {
        public IsBootstrappingException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.Retry(null);
        }
    }

    public class InvalidException : QueryValidationException
    {
        public InvalidException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.Rethrow();
        }
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

        public abstract QueryValidationException CreateException();
    }


    internal class OutputServerError : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new ServerErrorException(Message);
        }
    }

    internal class OutputProtocolError : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new ProtocolErrorException(Message);
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
        UnavailableInfo info = new UnavailableInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            info.Required = cb.ReadInt32();
            info.Alive = cb.ReadInt32();
        }
        public override QueryValidationException CreateException()
        {
            return new UnavailableException(Message, info.ConsistencyLevel, info.Required, info.Alive);
        }
    }

    internal class OutputOverloaded : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new OverloadedException(Message);
        }
    }

    internal class OutputIsBootstrapping : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new IsBootstrappingException(Message);
        }
    }

    internal class OutputTruncateError : OutputError
    {
        public override QueryValidationException CreateException()
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
        WriteTimeoutInfo info = new WriteTimeoutInfo();

        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            info.Received = cb.ReadInt32();
            info.BlockFor = cb.ReadInt32();
            info.WriteType = cb.ReadString();
        }

        public override QueryValidationException CreateException()
        {
            return new WriteTimeoutException(Message, info.ConsistencyLevel, info.Received, info.BlockFor, info.WriteType);
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
        ReadTimeoutInfo info = new ReadTimeoutInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.ConsistencyLevel = (ConsistencyLevel)cb.ReadInt16();
            info.Received = cb.ReadInt32();
            info.BlockFor = cb.ReadInt32();
            info.IsDataPresent = cb.ReadByte() != 0;
        }
        public override QueryValidationException CreateException()
        {
            return new ReadTimeoutException(Message, info.ConsistencyLevel, info.Received, info.BlockFor, info.IsDataPresent);
        }
    }

    internal class OutputSyntaxError : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new SyntaxError(Message);
        }
    }

    internal class OutputUnauthorized : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new UnauthorizedException(Message);
        }
    }

    internal class OutputInvalid : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new InvalidException(Message);
        }
    }


    internal class OutputConfigError : OutputError
    {
        public override QueryValidationException CreateException()
        {
            return new CassandraClusterConfigErrorException(Message);
        }
    }

    internal class AlreadyExistsInfo
    {
        public string Ks;
        public string Table;
    };

    internal class OutputAlreadyExists : OutputError
    {
        AlreadyExistsInfo info = new AlreadyExistsInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            info.Ks = cb.ReadString();
            info.Table = cb.ReadString();
        }
        public override QueryValidationException CreateException()
        {
            return new AlreadyExistsException(Message, info.Ks, info.Table);
        }
    }

    internal class PreparedQueryNotFoundInfo
    {
        public byte[] UnknownID;
    };

    internal class OutputUnprepared : OutputError
    {
        PreparedQueryNotFoundInfo info = new PreparedQueryNotFoundInfo();
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            var len = cb.ReadInt16();
            info.UnknownID = new byte[len];
            cb.Read(info.UnknownID, 0, len);
        }
        public override QueryValidationException CreateException()
        {
            return new PreparedQueryNotFoundException(Message, info.UnknownID);
        }
    }

}
