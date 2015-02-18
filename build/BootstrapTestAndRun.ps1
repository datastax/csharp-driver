
. ./BootStrapTest.ps1
. ./EnvGlobals.ps1

# This command *should* work for running tests, but for some reason ccm does not start when i use it
#cmd /c ClickToBuild.bat
echo "nunit path: $env:NUNIT_PATH"
cmd /c ClickToBuild.bat
cmd /c $env:NUNIT_PATH\nunit-console.exe ..\src\Cassandra.IntegrationTests\bin\Release\Cassandra.IntegrationTests.dll /labels /nologo /framework:net-4.0 /xml:testresults\TestResults.xml /trace=Info
#cmd /c $env:NUNIT_PATH\nunit-console.exe ..\src\Cassandra.IntegrationTests\bin\Release\Cassandra.IntegrationTests.dll /labels /nologo /framework:net-4.0 /include:smoke /xml:testresults\TestResults.xml /out=testresults\TestResults.txt
