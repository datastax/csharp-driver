using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class ResultResponse : IResponse
    {
        public enum ResultResponseKind { Void = 1, Rows = 2, SetKeyspace = 3, Prepared = 4 };

        public const byte OpCode = 0x08;
        public ResultResponseKind Kind;
        public IOutput Output;
        internal ResultResponse(ResponseFrame frame)
        {
            var rd = new BEBinaryReader(frame);
            Kind = (ResultResponseKind)rd.ReadInt32();
            switch (Kind)
            {
                case ResultResponseKind.Void:
                    Output = new OutputVoid();
                    break;
                case ResultResponseKind.Rows:
                    Output = new OutputRows(rd, frame.RawStream is BufferedProtoBuf);
                    break;
                case ResultResponseKind.SetKeyspace:
                    Output = new OutputSetKeyspace(rd.ReadString());
                    break;
                case ResultResponseKind.Prepared:
                    Output = new OutputPrepared(rd);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}
