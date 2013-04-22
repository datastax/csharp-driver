using System;

namespace Cassandra
{
    public class InvalidTypeException : DriverException
    {
        public object ReceivedType { get; private set; }
        public object[] ExpectedType { get; private set; }
        public String ParamName { get; private set; }

        public InvalidTypeException(String msg)
            :base(msg)
        {            
        }

        public InvalidTypeException(String msg, Exception cause)
            :base(msg, cause)
        {            
        }

        public InvalidTypeException(String paramName, object receivedType, object[] expectedType)
            : base(String.Format("Received object of type: {0}, expected: {1} {2}. Parameter name that caused exception: {3}",
            receivedType,  expectedType[0], expectedType.Length > 1 ? "or" + expectedType[1] : "", paramName))
        {
            this.ReceivedType = receivedType;
            this.ExpectedType = expectedType;
            this.ParamName = paramName; 
        }                        
    }
}
