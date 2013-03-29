using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Cassandra.Data.Linq.MSTest
{
    public class AssertException : Exception
    {
        public string UserMessage { get; private set; }

        public AssertException(string message, string userMessage = "")
            : base(message)
        {
            UserMessage = message;
        }
    }

    public static class Assert
    {
        public static void True(bool condition)
        {
            if (!condition)
            {
                Debugger.Break();
                throw new AssertException("false");
            }
        }

        public static void True(bool condition, string userMessage)
        {
            if (!condition)
            {
                Debugger.Break();
                throw new AssertException("false", userMessage);
            }
        }

        public static void False(bool condition)
        {
            if (condition)
            {
                Debugger.Break();
                throw new AssertException("true");
            }
        }

        public static void False(bool condition, string userMessage)
        {
            if (condition)
            {
                Debugger.Break();
                throw new AssertException("true", userMessage);
            }
        }

        public static void Equal<T>(T expected, T actual)
        {
            if (!((expected == null && actual == null) || (expected != null && expected.Equals(actual))))
            {
                Debugger.Break();
                throw new AssertException(string.Format("'{0}'!='{1}'", expected, actual));
            }
        }

        public static void Equal<T>(T expected, T actual, string userMessage)
        {
            if (!((expected == null && actual == null) || !(expected != null && expected.Equals(actual))))
            {
                Debugger.Break();
                throw new AssertException(string.Format("'{0}'!='{1}'", expected, actual), userMessage);
            }
        }

        public static bool ArrEqual(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;

            if (a1 == null || a2 == null)
                return false;

            if (a1.Length != a2.Length)
                return false;

            EqualityComparer<byte> comparer = EqualityComparer<byte>.Default;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!comparer.Equals(a1[i], a2[i])) return false;
            }
            return true;
        }
    }

}
