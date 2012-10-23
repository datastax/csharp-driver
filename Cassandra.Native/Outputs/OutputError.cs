using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Cassandra.Native
{
    public enum CassandraErrorType
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

    public abstract class OutputError : IOutput, IWaitableForDispose
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

    public class CassandraOutputException<OutputErrorT> : CassandraException where OutputErrorT : OutputError
    {
        public OutputErrorT OutputError;
        public CassandraOutputException(OutputErrorT OutputError)
            : base(OutputError.Message)
        {
            this.OutputError = OutputError;
        }
    }

    public class OutputServerError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputServerError>(this);
        }
    }


    public class OutputProtocolError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputProtocolError>(this);
        }
    }

    public class OutputUnavailableException : OutputError
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Required;
        public int Alive;
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            Required = cb.ReadInt32();
            Alive = cb.ReadInt32();
        }
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputUnavailableException>(this);
        }
    }

    public class OutputOverloaded : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputOverloaded>(this);
        }
    }

    public class OutputIsBootstrapping : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputIsBootstrapping>(this);
        }
    }

    public class OutputTruncateError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputTruncateError>(this);
        }
    }

    public class OutputWriteTimeout : OutputError
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public string WriteType;

        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            Received = cb.ReadInt32();
            BlockFor = cb.ReadInt32();
            WriteType = cb.ReadString();
        }

        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputWriteTimeout>(this);
        }
    }

    public class OutputReadTimeout : OutputError
    {
        public CqlConsistencyLevel ConsistencyLevel;
        public int Received;
        public int BlockFor;
        public bool IsDataPresent;
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            ConsistencyLevel = (CqlConsistencyLevel)cb.ReadInt16();
            Received = cb.ReadInt32();
            BlockFor = cb.ReadInt32();
            IsDataPresent = cb.ReadByte() != 0;
        }
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputReadTimeout>(this);
        }
    }

    public class OutputSyntaxError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputSyntaxError>(this);
        }
    }

    public class OutputUnauthorized : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputUnauthorized>(this);
        }
    }

    public class OutputInvalid : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputInvalid>(this);
        }
    }

    public class OutputConfigError : OutputError
    {
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputConfigError>(this);
        }
    }

    public class OutputAlreadyExists : OutputError
    {
        public string Ks;
        public string Table;
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            Ks = cb.ReadString();
            Table = cb.ReadString();
        }
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputAlreadyExists>(this);
        }
    }

    public class OutputUnprepared : OutputError
    {
        public byte[] UnknownID;
        internal void Load(CassandraErrorType code, string message, BEBinaryReader cb)
        {
            UnknownID = new byte[2];
            cb.Read(UnknownID,0,2);       
        }
        public override CassandraException CreateException()
        {
            return new CassandraOutputException<OutputUnprepared>(this);
        }
    }

}
