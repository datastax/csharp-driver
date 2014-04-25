using NUnit.Framework;
using System;

namespace Cassandra.IntegrationTests
{
    [AttributeUsage(AttributeTargets.Method)]
    class TestIgnoreAttribute : IgnoreAttribute
    {
    }
}
