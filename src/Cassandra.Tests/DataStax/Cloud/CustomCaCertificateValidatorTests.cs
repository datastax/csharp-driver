//
//      Copyright (C) DataStax Inc.
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
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

using Cassandra.DataStax.Cloud;

using NUnit.Framework;

namespace Cassandra.Tests.DataStax.Cloud
{
    [TestFixture]
    public class CustomCaCertificateValidatorTests
    {
        /// <summary>
        /// Simulate classic Astra
        /// </summary>
        [Test]
        public void TestCertificateWithoutWildcardAndWithRootCaInChain()
        {
            var rawData =
                "MIIDQzCCAiugAwIBAgIJAMH+1AGruRtDMA0GCSqGSIb3DQEBCwUAMFMxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQ" +
                "TEUMBIGA1UEBwwLU2FudGEgQ2xhcmExETAPBgNVBAoMCERhdGFTdGF4MQ4wDAYDVQQLDAVDbG91ZDAeFw0yMTAzMTI" +
                "xODAwMTRaFw0yMjAzMTIxODAwMTRaMG8xEjAQBgNVBAMTCWxvY2FsaG9zdDEOMAwGA1UECxMFQ2xvdWQxETAPBgNVBA" +
                "oTCERhdGFTdGF4MRQwEgYDVQQHEwtTYW50YSBDbGFyYTETMBEGA1UECBMKQ2FsaWZvcm5pYTELMAkGA1UEBhMCVVMwgg" +
                "EiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCgOdmkZ4TzKVNMdTQnpF/HoXdYP5iv7IoR6/+1zmeKzjd343xL/A" +
                "ZSPylDWj9kR5kvvDZNdkZ5l0WiZDf3rjJCUDA1yk7FkaUNNcUQFWun3vt+Bzz6bSmxlk+LH5L7DRkESXCqQxFKc8wFRV" +
                "fuMiCTiUUAv9yBBofs6XSXz7WbFV+EjZ/R8gW3lKpo9p8C4Y1xDld+fRV/gbg6LTUDcDExqxFxyDkQbpNYfzMgMSMsg1" +
                "6XrpdANXJZZVBrpKyVIRJ5UhcV11yyUEgYbLROgrGMTBBaTEiAaHaLL1H2jeVumKUcbAaaGFpZHdRq5IjOkDOjvSQmDv3" +
                "qBE1U0ZclSoBZAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAC6BLXvBj5YZf3QAKYok4zSSBghX8vGjlIvy7eksZtnuSBU3" +
                "QGe7IBOlv+iKtNVGxaNF94S+Xofl4N3RFXS7UwTZnC05yd3BBqGuR7ilKHO9bk+3SIwgu4pGtn1Z7WZ9l8U+jxDAfQVRW" +
                "U2pTRsiqnsembzBgdcCnFg/Ik6pc4EdwQfDQ+9aIVh1FtUDh852uo2grhRqxIv9BMADomVRhfHhgxTwoRRG0+Iww/A65a" +
                "cPD4cbK1Tq/wRz5MLKVWCvE1+SbBxPlPg8GyqaciuQKxwNwtr64JCkTxxYJMGiarzAu3UNM3/Ao7OmNrk2Ri6D/njKgiS3" +
                "aARVoyDw2k3u5dE=";
            var rawDataCa =
                "MIIDeTCCAmGgAwIBAgIJAMH+1AGruRtDMA0GCSqGSIb3DQEBCwUAMFMxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTEUMB" +
                "IGA1UEBwwLU2FudGEgQ2xhcmExETAPBgNVBAoMCERhdGFTdGF4MQ4wDAYDVQQLDAVDbG91ZDAeFw0yMTAzMTIxMzU0MjBa" +
                "Fw0zMTAzMTAxMzU0MjBaMFMxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTEUMBIGA1UEBwwLU2FudGEgQ2xhcmExETAPBg" +
                "NVBAoMCERhdGFTdGF4MQ4wDAYDVQQLDAVDbG91ZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALQwFjA9zueE" +
                "ZjlZHbMclffPjYjNcVjUs7kUw4Ah1Vj85ftdEqrayYZKLygc7zhsG4Vw5L/dBMYObXZBy3xriHiTAkdrDjdoxoxwbOU3fA" +
                "h/MdT3cp4e1uLt67lL5ulWDQXMxUAZf6aXjss4MfmTKouiG9rjtMUf7ZzJ5Se58/Hd0qC1PC2iVQMXtd6nJE7cBi4PTEJV" +
                "Vs3Prne6QU4R9IdzBcetYg+TlyC8DyzBViqegou7ILVlyykyyNGCxZlCsX6hDrt6zw2FqDlM5Jb6EM3uqFuyrKXyeevgy4" +
                "UV/7OlGPqBuuKs/0BAb0ZK/Cw75RiLs0kuhXRvWQIl459TqRcCAwEAAaNQME4wHQYDVR0OBBYEFMxMy6p9MPlYFC+qOUIF" +
                "garz/QAaMB8GA1UdIwQYMBaAFMxMy6p9MPlYFC+qOUIFgarz/QAaMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQELBQADgg" +
                "EBADibv/dhKE1nDc2d3GM0sbbAdQc7Jh9lCoh6dFLTkuYztsOzMYBMZJyYuf+W/EFkHtxB7vovaZYQ4+ovCrtPvKi9sa6a" +
                "hao3AwFvlX7vNlFR2CWN5ZRtfxpKqNq3H1FwCPu5YbH3kZiymwcY/0xRrEZC7nqieiZ5e7ZfsaPi+H/NooI0Ki0zT9negL" +
                "XCK/HYD/KDlRsH4Zj2WotBuZXTTYPylcs/EYPD/s6bEODj8KrzRj3OQCnT6KX6le3BqdvOyirSflj/Zq2/5dA3CjOctihj" +
                "ZE0+DipL13EubL9fxSr27/cQuBQWRdrCxNUKe7Ne3u7ppf28BU5cBFzLsQNR7+8=";

            var ca = new X509Certificate2(Convert.FromBase64String(rawDataCa));
            var cert = new X509Certificate2(Convert.FromBase64String(rawData));
            var chain = new X509Chain();
            chain.ChainPolicy.ExtraStore.Add(ca);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.Build(cert);
            Assert.True(new CustomCaCertificateValidator(ca, "localhost").Validate(cert, chain, SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch));
        }

        /// <summary>
        /// Simulate serverless Astra
        /// </summary>
        [Test]
        public void TestCertificateWithWildcardAndNoRootCaInChain()
        {
            var rawData =
                "MIIDrTCCApWgAwIBAgICBnowDQYJKoZIhvcNAQELBQAwezELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRQwEgYDVQQHE" +
                "wtTYW50YSBDbGFyYTERMA8GA1UEChMIRGF0YVN0YXgxDjAMBgNVBAsTBUNsb3VkMSYwJAYDVQQDEx1jYS5kYi5hc3RyYS" +
                "1wcm9kLmRhdGFzdGF4LmNvbTAeFw0yMTAyMjQyMDE2NDVaFw0zMTAyMjQyMDE2NDVaMHUxCzAJBgNVBAYTAlVTMQswCQY" +
                "DVQQIEwJDQTEUMBIGA1UEBxMLU2FudGEgQ2xhcmExETAPBgNVBAoTCERhdGFTdGF4MQ4wDAYDVQQLEwVDbG91ZDEgMB4G" +
                "A1UEAwwXKi5kYi5hc3RyYS5kYXRhc3RheC5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDdpcWmhr0jz" +
                "rjYQTAu1pxDkK8Yj+uit2NeT1MP1wiLv5YjV8q3gzWWCadlLxjyS44YBVKTR2DwjbxWgD51QqGrlmJ2//OQEqh3zAxy4i" +
                "un7iwVAo0Tg0+ko4MLneRUsqd/sNHyCDvGAX4/uU73zcWWg+l8sjsFaCUgCrUwBaQqCAoDoGRFTfk8Ls9OxCaFDrTWz56" +
                "x6vFi3qIZWE4CFQOUJCOSXvZyeQW13Qn3qFZ58L4m4Ks0PSZRl+rL7GC8QOF7HxdkC76P5DBuURn30ito27CquD7kwHro" +
                "HUOaWjqVNA4erNex29Z0EZ9Hs2EZ2mT06KUY0WsT5QlaBDgr4pa/AgMBAAGjQTA/MA4GA1UdDwEB/wQEAwIHgDAdBgNVH" +
                "SUEFjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwDgYDVR0OBAcEBQECAwQGMA0GCSqGSIb3DQEBCwUAA4IBAQB7btrTNUqiOfL" +
                "znOpMwOhCtkwd3je31alIvkoHyV+vAXIf2mZOVcBcusq22udQxT9gyWritiBXjzg9biQULtwyc68SX7n+6rPhV8P6dDsx0" +
                "tm5NKgsivwQkJ9l7leWoHqmmToFnKyetG/qA1FPHSDrVE3ZY6GeKRLEr3071r8cBffkrzc7EdspcAqqc4xZekY86O7/ta/" +
                "nnYGNwAST0OdGc0RtUODsne4AWJC3oYnBRid+n6DlQVAaN3DBDfNZYjTEBc3v6GptfBlf3J1G3sEsvSCnLu+1AcJqD31VNS" +
                "z4viBKVVPBuDGOwxj7l2q6kK+EpySZc7/iyaYKoHWOzuAo";
            var rawDataCa =
                "MIIDtDCCApygAwIBAgICBnUwDQYJKoZIhvcNAQELBQAwezELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRQwEgYDVQQHEwt" +
                "TYW50YSBDbGFyYTERMA8GA1UEChMIRGF0YVN0YXgxDjAMBgNVBAsTBUNsb3VkMSYwJAYDVQQDEx1jYS5kYi5hc3RyYS1wcm" +
                "9kLmRhdGFzdGF4LmNvbTAeFw0yMTAxMjAyMDE2MDJaFw0zMTAxMjAyMDE2MDJaMHsxCzAJBgNVBAYTAlVTMQswCQYDVQQIE" +
                "wJDQTEUMBIGA1UEBxMLU2FudGEgQ2xhcmExETAPBgNVBAoTCERhdGFTdGF4MQ4wDAYDVQQLEwVDbG91ZDEmMCQGA1UEAxMd" +
                "Y2EuZGIuYXN0cmEtcHJvZC5kYXRhc3RheC5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDKOnpwcM7HAaW" +
                "OvnfBkcEYL7wwBtpteNc4acXM1mTrog289KxgsUQEgdOT2EfRiS3qhrPbZn7+eXYTA7nT9UX3xnnPwigTAmEQdQvf/AXa7r" +
                "+GhpBoqo/CP0JXEdtrtA8KVDqWGxgnnpTNWbFmBEjCmG299/bFgWGmgvALjhwWHAUyT3sHhR10+UrtYG6CA0Az18rw+y0tf" +
                "cIhGKHvjOtctfgnjDp5fiuH6vJEeHsOK8vgOJK6xBTbaWjfAPqIArpCWFugErUzTnmYm77mR3MjjfPFM+wrCMpXSONnm722" +
                "vpCyjz1bw9bVdIY0GEtvdJzKHYQY/A+stNVmMZ1M1NzRAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIChDAdBgNVHSUEFjAUBgg" +
                "rBgEFBQcDAgYIKwYBBQUHAwEwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAVJw6dVTJUHG6yWUZPwWdk12" +
                "RauFxH9++UB16fv0wLNy78BNYcV/VbNq/Qhymz9V/ZSMtmrJw2/lV4i7tgVMVFkaNPEeFtHOL0cWJmV6yuFlWWGjaZ3oHCAL" +
                "9Awg4x6WQmtMoredcpRSAOGn9hX+IMenRF4OEI8ltG17zVGaMThZ7/OHzFIvgX5ynql9sRBXG0AjNsBG2QiP+0Xia9BGvvj" +
                "kqAfxwp44CWacVsbbWFrc+reYmOfmoy8b1Flm/gXVP2DbjMYwHX5RKvPt2SQ7L2iXgaa1a4+g9ZJx/U3RFHKeFbwpRLFH70" +
                "3FA1W20139MimrSfinHiQSy+WEDs7P41w==";

            var ca = new X509Certificate2(Convert.FromBase64String(rawDataCa));
            var cert = new X509Certificate2(Convert.FromBase64String(rawData));
            var chain = new X509Chain();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.Build(cert);
            Assert.AreEqual("*.db.astra.datastax.com", cert.GetNameInfo(X509NameType.SimpleName, false));
            Assert.True(new CustomCaCertificateValidator(ca, "3bdf7865-b9af-43d3-b76c-9ed0b57b2c2f-us-east-1.db.astra.datastax.com").Validate(cert, chain, SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch));
        }
    }
}