//
//       Copyright (C) 2019 DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dse.Helpers
{
    internal static class AssemblyHelpers
    {
        public static Assembly GetAssembly(Type type)
        {
            return type.GetTypeInfo().Assembly;
        }

        public static Version GetVersionPrefix(Type type)
        {
            return AssemblyHelpers.GetVersionPrefix(AssemblyHelpers.GetAssembly(type));
        }

        public static Version GetVersionPrefix(Assembly assembly)
        {
            var assemblyVersion = AssemblyHelpers.GetAssemblyInformationalVersion(assembly);
            var indexOfVersionSuffix = assemblyVersion.IndexOf('-');
            var versionPrefix = indexOfVersionSuffix == -1 ? assemblyVersion : assemblyVersion.Substring(0, indexOfVersionSuffix);
            return Version.Parse(versionPrefix);
        }

        public static string GetAssemblyInformationalVersion(Assembly assembly)
        {
            // InformationalVersion must be used for this driver because GetName().Version returns the assembly version attribute which is wrong
            // (at the time this comment was written, the assembly version was 3.99.0.0 while the actual driver version is 3.8.0)
            // and file version doesn't include suffixes like "-alpha"
            return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
        }

        public static string GetAssemblyInformationalVersion(Type type)
        {
            return AssemblyHelpers.GetAssemblyInformationalVersion(AssemblyHelpers.GetAssembly(type));
        }

        public static string GetAssemblyTitle(Type type)
        {
            return AssemblyHelpers.GetAssembly(type).GetCustomAttribute<AssemblyTitleAttribute>().Title;
        }

        public static Assembly GetEntryAssembly()
        {
            var assembly = Assembly.GetEntryAssembly();

#if !NETSTANDARD1_5
            if (assembly == null)
            {
                assembly = AssemblyHelpers.GetEntryAssemblyByMainModule();
            }

            if (assembly == null)
            {
                assembly = AssemblyHelpers.GetEntryAssemblyByStacktrace();
            }
#endif

            return assembly;
        }

#if !NETSTANDARD1_5
        private static Assembly GetEntryAssemblyByStacktrace()
        {
            try
            {
                var methodFrames = new StackTrace().GetFrames().Select(t => t.GetMethod()).ToArray();
                MethodBase entryMethod = null;
                int firstInvokeMethod = 0;
                for (int i = 0; i < methodFrames.Length; i++)
                {
                    var method = methodFrames[i] as MethodInfo;
                    if (method == null)
                        continue;
                    if (method.IsStatic &&
                        method.Name == "Main" &&
                        (
                            method.ReturnType == typeof(void) ||
                            method.ReturnType == typeof(int) ||
                            method.ReturnType == typeof(Task) ||
                            method.ReturnType == typeof(Task<int>)
                        ))
                    {
                        entryMethod = method;
                    }
                    else if (firstInvokeMethod == 0 &&
                             method.IsStatic &&
                             method.Name == "InvokeMethod" &&
                             method.DeclaringType == typeof(RuntimeMethodHandle))
                    {
                        firstInvokeMethod = i;
                    }
                }

                if (entryMethod == null)
                    entryMethod = firstInvokeMethod != 0 ? methodFrames[firstInvokeMethod - 1] : methodFrames.Last();

                return entryMethod.Module.Assembly;
            }
            catch
            {
                return null;
            }
        }

        private static Assembly GetEntryAssemblyByMainModule()
        {
            try
            {
                ProcessModule mainModule = Process.GetCurrentProcess().MainModule;
                Assembly entryAssembly = AppDomain.CurrentDomain.GetAssemblies()
                                                  .SingleOrDefault(assembly => assembly.Location == mainModule.FileName);
                return entryAssembly;
            }
            catch
            {
                return null;
            }
        }
#endif
    }
}