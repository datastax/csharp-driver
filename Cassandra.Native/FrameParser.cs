using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Diagnostics;

namespace Cassandra.Native
{
    internal class FrameParser
    {
        delegate IResponse MyDel(ResponseFrame frame);
        static MyDel[] registeredResponses = new MyDel[sbyte.MaxValue + 1];

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
            registeredResponses[(byte)obj] = (MyDel)Delegate.CreateDelegate(typeof(MyDel), mth);
        }

        public IResponse Parse(ResponseFrame frame)
        {
            var opcode = frame.FrameHeader.Opcode;
            if (registeredResponses[opcode] != null)
                return registeredResponses[opcode](frame);
            return null;
        }
    }
}
