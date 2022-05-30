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
            //rootcacsharp.crt
            var rootcab64 =
                @"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVZQ0NRQ01JNHlFM0J5WnpUQU5CZ2txaGtpRzl3MEJBUXNGQURCQU1Rc3dDUVlEVlFRR0V3SlYKVXpFUk1BOEdBMVVFQ2d3SVJHRjBZVk4wWVhneERqQU1CZ05WQkFzTUJVTnNiM1ZrTVE0d0RBWURWUVFEREFWRApiRzkxWkRBZ0Z3MHlNakExTXpBeE1qTTJNRGhhR0E4ek1ESXhNRGt6TURFeU16WXdPRm93UURFTE1Ba0dBMVVFCkJoTUNWVk14RVRBUEJnTlZCQW9NQ0VSaGRHRlRkR0Y0TVE0d0RBWURWUVFMREFWRGJHOTFaREVPTUF3R0ExVUUKQXd3RlEyeHZkV1F3Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRRGFZTjVBcm5VOQo5b2Q5cXdPMzVaRlBwdFpCc0psc29CQWl5b0V1WEpCbFFwdnlveHY2ckFXYkJaMkxpWEFvZkZWdzVjWThxTlNxCmUzRytjWmJzUm0xbk9Yc0lTRnVYekhGSWFJWWEzZi9OblIzc25SRG1uZUwwS3lhUVI5VnFvMCt5V0RUMmFlZWsKSDNrNFdrUlJrMEZtOUhIUWlKTWFTdU05WC9nQnhyUTdiMkxBOFRjM2FjTForbmYxdGpSa01ZN0hDNnJ6TW9rRwp0QmhCN3lxL1dtMzFOVk1ucVk2UnZhOWpBV1lIcW1YWXZkOG9uMmRsTDlzVzEybFRUNHd4Qkp1VTV6Mzd5bDhXClZOY0RkY3lhdWJzQUtDU3NwZmord2pVL0ZEL00zZUdYNEdZQVlDdjdQbStNL2NMNG1wMDRtR3dEakhVOE1RS28KUi91QmRRbllRN2lYQWdNQkFBRXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBTFhvL01UUUVYY29vQ3dTSVczaAovemhaSFNrUko1SmYyd2pjTTlPTmxEaVI1K01NU2pYVFVTMHF6amlhZFNvSzlZeGdWN1ByVHpjMWR0cVNzTjdXCllQNHM0VG9zbWRzRVRTdmVwRXFRSklLRHZyVFJaTDlPV2hLb1BVRTd3NzJVVnVYcERaWHIraTNNQ3p3U05zZGwKNzFXbHpVcXJqcGJCSkZnU21xZmZkNmJuVXExWVhKU3orcy9KSnV4SmlrNGVqTGlIbTh3MWhIckUvYWRvYUtMbQo1elYxaHpPT3Y0d0VnSTQ3VnNvMUxYenQzQmROeXVVMnZPR1R4Wmwwa0E1bUVSRUwxR3J0TEova251SkF6aWZ5ClFKMnVlU2JMTVZadXBsNy84MW5Ib3k3eVNaKzNYM1VUUkdQNzNNYWM5cTdvK0ZJeTV2M0laL3NJM0FEaWM4NXUKWGVvPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==";

            //localhostcsharp.crt
            var certb64 =
                @"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURBakNDQWVvQ0NRQ2ZCNGxlaUM0ZGl6QU5CZ2txaGtpRzl3MEJBUXNGQURCQU1Rc3dDUVlEVlFRR0V3SlYKVXpFUk1BOEdBMVVFQ2d3SVJHRjBZVk4wWVhneERqQU1CZ05WQkFzTUJVTnNiM1ZrTVE0d0RBWURWUVFEREFWRApiRzkxWkRBZ0Z3MHlNakExTXpBeE1qUXpOVE5hR0E4ek1ESXhNRGt6TURFeU5ETTFNMW93UkRFTE1Ba0dBMVVFCkJoTUNWVk14RVRBUEJnTlZCQW9UQ0VSaGRHRlRkR0Y0TVE0d0RBWURWUVFMRXdWRGJHOTFaREVTTUJBR0ExVUUKQXhNSmJHOWpZV3hvYjNOME1JSUJJakFOQmdrcWhraUc5dzBCQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBb2Z3TwpSNW5CQW9PaWhsengrTSt3enhCdzg2OHU5L3FxZlVyRlIycXlySFBYZkNjSm14ZUFXVE5UMXllbWcyd2pZSUZmClZoU2VmOXZNSWlQbWh1eXd2R1RkVFlCcFdsZTNKRlY3ejdvb1JYb1d4SGwwNFJTNVY3Q0p6Sk1vNEdNRWZYamgKL2VyNVk2NU9ibnY3Z2hiN0IzNEh5NDVzZFMyWWpPRklZVFFzZXgwMDhIMnpDSmVoT3J0OWFZWlFOVEp1Zk9BMwovNnJRaG9hZVhjKy9DdWdKUWlkOEQyRlJvUENmM1NsQlkzeHFZbmhJNGJmQWJvUTJqYmJINitzYjlYRDFsdTVCCiswTWl5NGcwL00yQTA0NWZpYnJjQVAvRG9YWnpSdkd5SWlmYU55RHltQjZERWhtUFNNUkNZRWdpWlFlMmpkRVQKSUVEMkk5NnJBZjhpb3B2ZG13SURBUUFCTUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFBM2V3MVhlaG9WMHpBZAo2SkJuS1FzMkdkVjl5WFVFRVhEQTQ3M0xJZ05JWWFrVEpWbVZTVGdQUlNEbW1LRzkyNlUwUUgzREtwbitsTDNxCnQ4V1IyM3gwRzJSeXFtaGJ5MTFlUXFkWUZrRzNIOGxVMVVIZTM3LzI3Q29EeE80cnpNUFNJd0JWRVN3aTVsbHoKc1FHWXY2K1dnZUFXUHpRYzlCOVV5UFYyenFSbitCQTNOdDBPSnJOdG1UcFJsRE5XU2NoZ1VhV1BCdXhRVk1kNgo2OEM5dkczNk9hYnJFZDBmakdJUWVaYlJjcHFPQVVEbEpMaUY3SElEU0NkNFo0MUxOM3pNdmMxTnhMN1orUDdhCm5jU2RTcjlkR0tIOXRvRm4ydk5Nd0JOZWgvbm5Xc3BRd3djSm85Yk9pZjNmeXlGOWZYWGF0TjVxM2dJcUhVd08KVXlFK1l1UjEKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=";

            var ca = new X509Certificate2(Convert.FromBase64String(rootcab64));
            var cert = new X509Certificate2(Convert.FromBase64String(certb64));
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