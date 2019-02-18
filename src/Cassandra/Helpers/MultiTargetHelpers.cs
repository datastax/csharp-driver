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
using System.Linq;
using System.Reflection;

namespace Cassandra.Helpers
{
    internal static class MultiTargetHelpers
    {
        public static Assembly GetAssembly(Type type)
        {
#if NETSTANDARD1_5
            return type.GetTypeInfo().Assembly;
#else
            return type.Assembly;
#endif
        }

        public static Version GetAssemblyVersion(Type type)
        {
            return MultiTargetHelpers.GetAssemblyFileVersion(MultiTargetHelpers.GetAssembly(type));
        }

        public static Version GetAssemblyFileVersion(Assembly assembly)
        {
            return Version.Parse(MultiTargetHelpers.GetAssemblyFileVersionString(assembly));
        }

        public static string GetAssemblyFileVersionString(Assembly assembly)
        {
            // InformationalVersion must be used because GetName().Version returns the assembly version attribute which is wrong
            // (at the time this comment was written, the assembly version was 3.99.0.0 while the actual driver version is 3.8.0)
            return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
        }

        public static string GetAssemblyFileVersionString(Type type)
        {
            return MultiTargetHelpers.GetAssemblyFileVersionString(MultiTargetHelpers.GetAssembly(type));
        }
    }
}