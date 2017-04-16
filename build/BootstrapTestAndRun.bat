
#. ./BootStrapTest.ps1
#. ./EnvGlobals.ps1
#
# This command *should* work for running tests, but for some reason ccm does not start when i use it
#cmd /c ClickToBuild.bat
# (including full paths)
Powershell.exe -executionpolicy remotesigned -File BootstrapTest.ps1
ECHO " Current value of NUNUIT_PATH: "
ECHO %NUNIT_PATH%
#ClickToBuild.bat /t:test
cmd /c $env:NUNIT_PATH\nunit-console.exe ..\src\Cassandra.IntegrationTests\bin\Release\Cassandra.IntegrationTests.dll /labels /nologo /framework:net-4.0 /include:smoke /xml:testresults\TestResults.xml /out=testresults\TestResults.txt
#cmd /c C:\CSharpDownloads\NUnit-2.6.4\NUnit-2.6.4\bin\nunit-console.exe ..\src\Cassandra.IntegrationTests\bin\Release\Cassandra.IntegrationTests.dll /labels /nologo /framework:net-4.0 /include:smoke /xml:testresults\TestResults.xml /out=testresults\TestResults.txt
