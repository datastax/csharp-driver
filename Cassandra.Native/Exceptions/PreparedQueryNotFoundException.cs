using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    public class PreparedQueryNotFoundException : QueryValidationException
    {
        public byte[] UnknownID { get; private set; }
        public PreparedQueryNotFoundException(string Message, byte[] UnknownId) :
            base(Message) { this.UnknownID = UnknownId; }
    }

}
