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

namespace Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then
{
    public abstract class BaseThen : IThen
    {
        public int DelayInMs { get; private set; } = 0;

        public bool IgnoreOnPrepare { get; private set; } = false;

        public void SetIgnoreOnPrepare(bool ignoreOnPrepare)
        {
            this.IgnoreOnPrepare = ignoreOnPrepare;
        }

        public void SetDelayInMs(int delayInMs)
        {
            DelayInMs = delayInMs;
        }

        public abstract object Render();
    }
}