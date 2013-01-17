using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using System.Diagnostics;

namespace Dev
{
    public class IgnoreAttribute : Attribute
    {
    }
    public class PrioriotyAttribute : Attribute
    {
    }
    public interface ISettings
    {
        string this[string name] { get; }
        Action<string> GetWriter();
    }

    public class SettingsFixture
    {
        ISettings settings;

        public SettingsFixture()
        {
        }
        public void Initialize(ISettings settings)
        {
            this.settings = settings;
        }
        public ISettings Settings
        {
            get
            {
                return settings;
            }
        }

        public void InfoMessage(string msg)
        {
            var we = settings.GetWriter();
            if (we != null)
                we(msg);
        }
    }

    public static class Assert
    {
        public static void True(bool condition)
        {
            if (!condition)
                Debugger.Break();
            Xunit.Assert.True(condition);
        }
        public static void True(bool condition, string userMessage)
        {
            if (!condition)
                Debugger.Break();
            Xunit.Assert.True(condition, userMessage);
        }

        public static void Equal<T>(T expected, T actual)
        {
            if (!expected.Equals(actual))
                Debugger.Break();
            Xunit.Assert.Equal<T>(expected, actual);
        }
    }
    
}
