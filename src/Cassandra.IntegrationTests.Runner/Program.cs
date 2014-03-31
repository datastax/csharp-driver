//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Threading;
using Cassandra.IntegrationTests.Runner.Properties;
using CommandLine;

namespace Cassandra.IntegrationTests.Runner
{
    internal class Program
    {
        // The assembly that contains the integration tests
        private static readonly Assembly IntegrationTestsAssembly = typeof (MyTestOptions).Assembly;

        private static MethodInfo FindMethodWithAttribute(Type tpy, Type attr)
        {
            foreach (MethodInfo m in tpy.GetMethods())
            {
                if (m.GetCustomAttributes(attr, true).Length > 0)
                    return m;
            }
            return null;
        }

        private static void Test(ref object testObj, Type type, MethodInfo mth, StreamWriter output, ref int Passed, ref int Failed)
        {
            try
            {
                if (mth != null && testObj == null)
                {
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.BackgroundColor = ConsoleColor.White;
                    string s = type.FullName + ":Init";
                    Console.WriteLine(new string(' ', 79));
                    Console.WriteLine(s);
                    output.WriteLine(new string('-', 79));
                    output.WriteLine(s);
                    Console.ResetColor();

                    testObj = type.GetConstructor(new Type[] {}).Invoke(new object[] {});
                    MethodInfo ist = FindMethodWithAttribute(type, typeof (TestInitializeAttribute));
                    if (ist != null)
                        ist.Invoke(testObj, new object[] {});
                }
                if (mth == null)
                {
                    if (testObj != null)
                    {
                        Console.ForegroundColor = ConsoleColor.Black;
                        Console.BackgroundColor = ConsoleColor.White;
                        string s = type.FullName + ":Cleanup";
                        Console.WriteLine(new string(' ', 79));
                        Console.WriteLine(s);
                        output.WriteLine(new string('-', 79));
                        output.WriteLine(s);
                        Console.ResetColor();
                        MethodInfo ist = FindMethodWithAttribute(type, typeof (TestCleanupAttribute));
                        if (ist != null)
                            ist.Invoke(testObj, new object[] {});
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                    }
                    return;
                }
                {
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.BackgroundColor = ConsoleColor.White;
                    string s = type.FullName + "." + mth.Name + "() Start...";
                    Console.WriteLine(new string(' ', 79));
                    Console.WriteLine(s);
                    output.WriteLine(new string('-', 79));
                    output.WriteLine(s);
                    Console.ResetColor();
                    mth.Invoke(testObj, new object[] {});
                    Passed++;
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.BackgroundColor = ConsoleColor.Green;
                    s = type.FullName + "." + mth.Name + "() Passed";
                    Console.WriteLine(s);
                    Console.WriteLine(new string(' ', 79));
                    output.WriteLine(s);
                    output.WriteLine(new string('-', 79));
                    output.Flush();
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                if (ex.InnerException is AssertException)
                {
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.BackgroundColor = ConsoleColor.Red;
                    string s = type.FullName + "." + mth.Name + "() Failed!";
                    Console.WriteLine(s);
                    output.WriteLine(s);
                    s = ex.InnerException.Message;
                    Console.WriteLine(s);
                    output.WriteLine(s);
                    s = (ex.InnerException as AssertException).UserMessage;
                    Console.WriteLine(s);
                    Console.WriteLine(new string(' ', 79));
                    output.WriteLine(s);
                    output.WriteLine(new string('-', 79));
                    output.Flush();
                }
                else
                {
                    Console.BackgroundColor = ConsoleColor.Magenta;
                    Console.ForegroundColor = ConsoleColor.Black;
                    Console.WriteLine("Exception");
                    Console.WriteLine(ex.InnerException.Source);
                    Console.WriteLine(ex.InnerException.Message);
                    output.WriteLine("Exception");
                    output.WriteLine(ex.InnerException.Source);
                    output.WriteLine(ex.InnerException.Message);
                    if (ex.InnerException.InnerException != null)
                        printInnerException(ex.InnerException.InnerException, output);
                    output.Flush();
                }
                Console.WriteLine(ex.InnerException.StackTrace);
                output.WriteLine(ex.InnerException.StackTrace);
                Console.ResetColor();
                Failed++;
                output.Flush();
            }
            Console.WriteLine();
            output.WriteLine();
        }

        private static int Main(string[] args)
        {
            if (!Parser.Default.ParseArguments(args, MyTestOptions.Default))
            {
                MyTestOptions.Default.GetUsage();
                return 1;
            }

            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            string tstDir = Settings.Default.TestFolder.Replace("$TEST_ROOT", Directory.GetCurrentDirectory());
            Directory.CreateDirectory(tstDir);

            var output =
                new StreamWriter(tstDir + "/" + DateTime.Now.ToShortDateString().Replace("/", "_") + "+" +
                                 DateTime.Now.ToShortTimeString().Replace(":", "_") + ".log");

            int Passed = 0;
            int Failed = 0;

            bool priorityTestsRun = true;

            second_round: // Iterate again over the test packs, and run tests without priority attribute 
            foreach (Type type in IntegrationTestsAssembly.GetTypes())
            {
                if (type.Name.EndsWith("Tests") && type.IsPublic)
                {
                    object testObj = null;
                    foreach (MethodInfo mth in type.GetMethods())
                    {
                        if (mth.GetCustomAttributes(typeof (TestMethodAttribute), true).Length > 0)
                        {
                            if (mth.GetCustomAttributes(typeof (StressAttribute), true).Length > 0)
                                if ((MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.Fixing)
                                    && (MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.FullTest))
                                    continue;
                            if (mth.GetCustomAttributes(typeof (NeedSomeFixAttribute), true).Length > 0)
                                if ((MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.Fixing)
                                    && (MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.NoStress)
                                    && (MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.FullTest))
                                    continue;
                            if (mth.GetCustomAttributes(typeof (WorksForMeAttribute), true).Length > 0)
                                if (((MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.FullTest)
                                     && (MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.NoStress)
                                     && (MyTestOptions.Default.TestRunMode != MyTestOptions.TestRunModeEnum.ShouldBeOk))
                                    || (MyTestOptions.Default.TestRunMode == MyTestOptions.TestRunModeEnum.Fixing))
                                    continue;
                            if ((mth.GetCustomAttributes(typeof (PriorityAttribute), true).Length == 0) ==
                                priorityTestsRun)
                                continue;
                            Test(ref testObj, type, mth, output, ref Passed, ref Failed);
                        }
                    }
                    Test(ref testObj, type, null, output, ref Passed, ref Failed);
                }
            }

            if (priorityTestsRun)
            {
                priorityTestsRun = false;
                goto second_round;
            }

            Console.ForegroundColor = ConsoleColor.Black;
            if (Failed > 0)
                Console.BackgroundColor = ConsoleColor.Red;
            else
                Console.BackgroundColor = ConsoleColor.Green;

            string st = Failed > 0
                            ? string.Format("[{0} (of {1}) Failures]", Failed, Failed + Passed)
                            : string.Format("[All {0} Passed :)]", Passed);
            st += " Press Any Key To Close The Program";
            Console.WriteLine(new string(' ', 79));
            Console.WriteLine(st + new string(' ', 79 - st.Length));
            Console.WriteLine(new string(' ', 79));
            output.WriteLine(new string('-', 79));
            output.WriteLine(st);
            output.WriteLine(new string('-', 79));
            output.Close();
            if (Failed > 0)
                Console.ReadKey();

            return 0;
        }

        private static void printInnerException(Exception ex, StreamWriter output)
        {
            Console.WriteLine("(");
            Console.WriteLine("Exception");
            Console.WriteLine(ex.Source);
            Console.WriteLine(ex.Message);
            output.WriteLine("Exception");
            output.WriteLine(ex.Source);
            output.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            output.WriteLine(ex.StackTrace);
            if (ex.InnerException != null)
                printInnerException(ex.InnerException, output);
            Console.WriteLine(")");
        }
    }
}