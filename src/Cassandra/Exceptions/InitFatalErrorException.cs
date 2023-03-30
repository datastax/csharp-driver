// 
//       Copyright (C) DataStax Inc.
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

using System;

namespace Cassandra
{
    public class InitFatalErrorException : Exception
    {
        private const string ExceptionMessage = 
            "An error occured during the initialization of the cluster instance. Further initialization attempts " +
            "for this cluster instance will never succeed and will return this exception instead. The InnerException property holds " +
            "a reference to the exception that originally caused the initialization error.";

        public InitFatalErrorException(Exception innerException) : base(ExceptionMessage, innerException)
        {
        }
    }
}