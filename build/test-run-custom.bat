git pull
IF EXIST "C:\Program Files (x86)\Microsoft Visual Studio 11.0\VC\vcvarsall.bat" (
	CALL "C:\Program Files (x86)\Microsoft Visual Studio 11.0\VC\vcvarsall.bat"
) ELSE (
	CALL "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat"
)
msbuild ..\src\Cassandra.sln /t:Clean /p:Configuration=Release
msbuild ..\src\Cassandra.sln /p:Configuration=Release
"..\src\Cassandra.IntegrationTests.Runner\bin\Release\Cassandra.IntegrationTests.Runner.exe" -u user -p password -c 2.0.6 -h 192.168.59.1 -i 192.168.59. -m NoStress
