using System;
using System.Reflection;

namespace Cassandra
{
    internal class FrameParser
    {
        delegate AbstractResponse MyDel(ResponseFrame frame);
        static readonly MyDel[] RegisteredResponses = new MyDel[sbyte.MaxValue + 1];

        static FrameParser()
        {
            Register(typeof(AuthenticateResponse));
            Register(typeof(ErrorResponse));
            Register(typeof(EventResponse));
            Register(typeof(ReadyResponse));
            Register(typeof(ResultResponse));
            Register(typeof(SupportedResponse));
        }

        static void Register(Type response)
        {
            var obj = response.GetField("OpCode").GetValue(response);
            var mth = response.GetMethod("Create", BindingFlags.NonPublic | BindingFlags.Static, null, new Type[] { typeof(ResponseFrame) }, null);
            RegisteredResponses[(byte)obj] = (MyDel)Delegate.CreateDelegate(typeof(MyDel), mth);
        }

        public AbstractResponse Parse(ResponseFrame frame)
        {
            var opcode = frame.FrameHeader.Opcode;
            if (RegisteredResponses[opcode] != null)
                return RegisteredResponses[opcode](frame);
            return null;
        }
    }
}
