using System;
using System.Diagnostics;
using System.Collections.Generic;

namespace MyTest
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TestClass : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class TestMethodAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class WorksForMeAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class NeedSomeFixAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class PriorityAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class TestInitializeAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method)]
    public class TestCleanupAttribute : Attribute
    {
    }

    public class AssertException : Exception
    {
        public string UserMessage { get; private set; }

        public AssertException(string message, string userMessage = "") : base(message)
        {
            UserMessage = userMessage;
        }
    }

    public static class Assert
    {
        public static void Fail(string msg = "FAILED")
        {
            throw new AssertException(msg);
        }

        public static void True(bool condition)
        {
            if (!condition)
            {
#if DEBUG				
                Debugger.Break();
#endif				
                throw new AssertException("false");
            }
        }

        public static void True(bool condition, string userMessage)
        {
            if (!condition)
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException("false", userMessage);
            }
        }

        public static void False(bool condition)
        {
            if (condition)
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException("true");
            }
        }

        public static void False(bool condition, string userMessage)
        {
            if (condition)
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException("true", userMessage);
            }
        }

        public static void Equal<T>(T expected, T actual)
        {
            if (!((expected == null && actual == null) || (expected != null && expected.Equals(actual))))
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException(string.Format("'{0}'!='{1}'", expected, actual));
            }
        }

        public static void Equal<T>(T expected, T actual, string userMessage)
        {
            if (!((expected == null && actual == null) || (expected != null && expected.Equals(actual))))
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException(string.Format("'{0}'!='{1}'", expected, actual), userMessage);
            }
        }

        public static bool ArrEqual(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;

            if (a1 == null || a2 == null)
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException("At least one array is null.");
            }
                

            if (a1.Length != a2.Length)
            {
#if DEBUG				
                Debugger.Break();
#endif
				throw new AssertException(string.Format("Arrays length is not equal: '{0}'!='{1}'", a1.Length, a2.Length));
            }
            

            EqualityComparer<byte> comparer = EqualityComparer<byte>.Default;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!comparer.Equals(a1[i], a2[i]))
                {
#if DEBUG				
                Debugger.Break();
#endif
					throw new AssertException(string.Format("Arrays values are not equal: '{0}'!='{1}' at {2} position.", a1[i], a2[i], i));
                }
            }
            return true;
        }

    }
}
