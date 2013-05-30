using System;
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

        public enum TestRunModeEnum { FullTest, Fixing, Checking, ShouldBeOk };

        [Option('m', "mode",
          HelpText = "Test run mode", DefaultValue = TestRunModeEnum.Fixing)]
        public TestRunModeEnum TestRunMode { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}
