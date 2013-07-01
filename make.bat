git pull
call "C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC\vcvarsall.bat"
msbuild Cassandra.MyTest.sln /t:Clean /p:Configuration=Release
msbuild Cassandra.MyTest.sln /p:Configuration=Release
mkdir -p "Nuget\lib"
cp MyTestRun/bin/Release/Cassandra.dll Nuget/lib/Cassandra.dll
cp MyTestRun/bin/Release/Cassandra.Data.dll Nuget/lib/Cassandra.Data.dll
cp MyTestRun/bin/Release/Cassandra.Data.Linq.dll Nuget/lib/Cassandra.Data.Linq.dll
"tools\nuget" pack "nuget\CassandraCSharpDriver.nuspec"
rmdir /S /Q "Nuget\lib"
