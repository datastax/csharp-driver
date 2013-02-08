using System;
using System.Diagnostics;

namespace Dev
{
    public class FactAttribute : Attribute
    {
    }

    public class IgnoreAttribute : Attribute
    {
    }

    public class PriorityAttribute : Attribute
    {
    }

    public interface ISettings
    {
        string this[string name] { get; }
        Action<string> GetWriter();
    }

    public class SettingsFixture
    {
        private ISettings _settings;

        public SettingsFixture()
        {
        }

        public void Initialize(ISettings settings)
        {
            this._settings = settings;
        }

        public ISettings Settings
        {
            get { return _settings; }
        }

        public void InfoMessage(string msg)
        {
            var we = _settings.GetWriter();
            if (we != null)
                we(msg);
        }
    }

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
    }
}
