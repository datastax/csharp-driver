git pull
call "C:\Program Files (x86)\Microsoft Visual Studio 11.0\VC\vcvarsall.bat"
msbuild Cassandra.MyTest.sln /t:Clean /p:Configuration=Release
msbuild Cassandra.MyTest.sln /p:Configuration=Release
"MyTestRun/bin/Release/MyTestRun.exe" -u user -p password -c 1.2.12 -h 192.168.13.1 -i 192.168.13. -m NoStress
