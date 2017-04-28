<# dotMemoryUnit requires the pull path of nunit3-console.
Appveyor does include the nunit3-console.exe at PATH system variable, but do not provide the Nunit3 path as a separated variable #>
$nunitrunner = "nunit3-console.exe";
$paths = $env:PATH -split ";"
For ($i=0; $i -le $paths.Length; $i++) {
	If (Test-Path "$($paths[$i])\nunit3-console.exe") {
		$nunitrunner = "$($paths[$i])\nunit3-console.exe"
	}
}
Write-Host "Nunit Runner path" + $nunitrunner
pushd src/Cassandra.IntegrationTests
Write-Host "Starting dotmemory unit tests..." 
dotMemoryUnit -targetExecutable="$($nunitrunner)" --"bin\Release\Cassandra.IntegrationTests.dll" --where "cat=memory" --trace=Verbose --labels:all --result="..\..\TestResult.xml"
popd
