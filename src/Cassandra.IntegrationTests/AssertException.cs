using System;

namespace Cassandra.IntegrationTests
{
    public class AssertException : Exception
    {
        public string UserMessage { get; private set; }

        public AssertException(string message, string userMessage = "") : base(message)
        {
            UserMessage = userMessage;
        }
    }
}