using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Diagnostics;

namespace Cassandra.Native
{
    internal class FrameParser
    {
        static Dictionary<byte, Type> registeredResponses = new Dictionary<byte, Type>();

        static void Register(Type response)
        {
            var obj = response.GetField("OpCode").GetValue(response);
            registeredResponses.Add((byte)obj,response);
        }

        public FrameParser()
        {
            Register(typeof(AuthenticateResponse));
            Register(typeof(ErrorResponse));
            Register(typeof(EventResponse));
            Register(typeof(ReadyResponse));
            Register(typeof(ResultResponse));
            Register(typeof(SupportedResponse));
        }

        public IResponse Parse(ResponseFrame frame)
        {
            var opcode = frame.FrameHeader.opcode;
            if (registeredResponses.ContainsKey(opcode))
            {
                return registeredResponses[opcode].GetConstructor(BindingFlags.NonPublic|BindingFlags.Instance,null, new Type[] { typeof(ResponseFrame) } ,null).Invoke(new object[] { frame }) as IResponse;
            }
            return null;
        }
    }
}
