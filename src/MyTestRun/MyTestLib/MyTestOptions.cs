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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CommandLine;
using CommandLine.Text;

namespace MyTest
{
    public class MyTestOptions
    {
        public static MyTestOptions Default = new MyTestOptions();

        [Option('c', "cassandra-version",
          HelpText = "CCM Cassandra Version.", DefaultValue = "1.2.4")]
        public string CassandraVersion { get; set; }

        [Option('i', "ip-prefix",
          HelpText = "CCM Ip prefix", DefaultValue = "127.0.0.")]
        public string IpPrefix { get; set; }

        [Option('h', "ssh-host",
          HelpText = "CCM SSH host", DefaultValue = "127.0.0.1")]
        public string SSHHost { get; set; }

        [Option('t', "ssh-port",
          HelpText = "CCM SSH port", DefaultValue = 22)]
        public int SSHPort { get; set; }

        [Option('u', "ssh-user", Required = true,
          HelpText = "CCM SSH user")]
        public string SSHUser { get; set; }

        [Option('p', "ssh-password", Required = true,
          HelpText = "CCM SSH password")]
        public string SSHPassword { get; set; }

        public enum TestRunModeEnum {NoStress, FullTest, Fixing, Checking, ShouldBeOk };
        public enum TestGroupEnum { All, Unitary, Integration };

        [Option('m', "mode",
          HelpText = "Test run mode", DefaultValue = TestRunModeEnum.Fixing)]
        public TestRunModeEnum TestRunMode { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }

        //test configuration
        [Option("compression",
          HelpText = "Use Compression", DefaultValue = false)]
        public bool UseCompression { get; set; }

        [Option("nobuffering",
          HelpText = "No Buffering", DefaultValue = false)]
        public bool NoUseBuffering { get; set; }

        [Option("logger",
          HelpText = "Use Logger", DefaultValue = false)]
        public bool UseLogger { get; set; }
    }
}
