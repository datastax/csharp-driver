using System;
using System.Diagnostics;
using System.Collections.Generic;

namespace MyTest
{
    public class TestClass : Attribute
    {
    }

    public class TestMethodAttribute : Attribute
    {
    }

    public class IgnoreAttribute : Attribute
    {
    }

    public class PriorityAttribute : Attribute
    {
    }

    public class TestInitializeAttribute : Attribute
    {
    }

    public class TestCleanupAttribute : Attribute
    {
    }

    //public interface ISettings
    //{
    //    string this[string name] { get; }
    //    Action<string> GetWriter();
    //}


    //public class SettingsFixture
    //{
    //    private ISettings _settings;

    //    public SettingsFixture()
    //    {
    //    }

    //    public void Initialize(ISettings settings)
    //    {
    //        this._settings = settings;
    //    }

    //    public ISettings Settings
    //    {
    //        get { return _settings; }
    //    }

    //    public void InfoMessage(string msg)
    //    {
    //        var we = _settings.GetWriter();
    //        if (we != null)
    //            we(msg);
    //    }
    //}

    public class AssertException : Exception
    {
        public string UserMessage { get; private set; }

        public AssertException(string message, string userMessage = "") : base(message)
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
