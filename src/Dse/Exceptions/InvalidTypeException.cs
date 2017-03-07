//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    /// <summary>
    /// Exception that is thrown when the driver expected a type and other was provided
    /// </summary>
    public class InvalidTypeException : DriverException
    {
        public object ReceivedType { get; private set; }
        public object[] ExpectedType { get; private set; }
        public String ParamName { get; private set; }

        public InvalidTypeException(String msg)
            : base(msg)
        {
        }

        public InvalidTypeException(String msg, Exception cause)
            : base(msg, cause)
        {
        }

        public InvalidTypeException(String paramName, object receivedType, object[] expectedType)
            : base(String.Format("Received object of type: {0}, expected: {1} {2}. Parameter name that caused exception: {3}",
                                 receivedType, expectedType[0], expectedType.Length > 1 ? "or" + expectedType[1] : "", paramName))
        {
            ReceivedType = receivedType;
            ExpectedType = expectedType;
            ParamName = paramName;
        }
    }
}
