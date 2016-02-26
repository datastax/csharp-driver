using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture, Category("unit")]
    public abstract class BaseUnitTest
    {
        protected static Task<T> TaskOf<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
            return tcs.Task;
        }
    }
}
