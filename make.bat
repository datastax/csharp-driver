git pull
call "C:\Program Files (x86)\Microsoft Visual Studio 11.0\VC\vcvarsall.bat"
msbuild Cassandra.MyTest.sln /t:Clean /p:Configuration=Release
msbuild Cassandra.MyTest.sln /p:Configuration=Release
mkdir "Nuget\lib"
cp Cassandra/bin/Release/Cassandra.dll Nuget/lib/Cassandra.dll
cp Cassandra.Data/bin/Release/Cassandra.Data.dll Nuget/lib/Cassandra.Data.dll
cp Cassandra.Data.Linq/bin/Release/Cassandra.Data.Linq.dll Nuget/lib/Cassandra.Data.Linq.dll
cp Cassandra.Data.DSE/bin/Release/Cassandra.Data.DSE.dll Nuget/lib/Cassandra.Data.DSE.dll
cp Cassandra/bin/Release/Cassandra.xml Nuget/lib/Cassandra.xml
cp Cassandra.Data/bin/Release/Cassandra.Data.xml Nuget/lib/Cassandra.Data.xml
cp Cassandra.Data.Linq/bin/Release/Cassandra.Data.Linq.xml Nuget/lib/Cassandra.Data.Linq.xml
cp Cassandra.Data.DSE/bin/Release/Cassandra.Data.DSE.xml Nuget/lib/Cassandra.Data.DSE.xml
"tools\nuget" pack "nuget\CassandraCSharpDriver.nuspec"
rmdir /S /Q "Nuget\lib"
msbuild Documentation.sln /p:Configuration=Release
