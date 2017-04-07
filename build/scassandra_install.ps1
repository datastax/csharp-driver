$dep_dir="$($env:USERPROFILE)\deps"

Write-Host "Install scassandra..."

If (!(Test-Path $dep_dir)) {
  Write-Host "Creating $($dep_dir)"
  New-Item -Path $dep_dir -ItemType Directory -Force
}

# Install SCassandra
$scassandra_path = "$($dep_dir)\scassandra-server-1.0.10"
If (!(Test-Path $scassandra_path)) {
  Write-Host "Downloading cassandra 1.0.10"
  $scassandra_url = "https://github.com/scassandra/scassandra-server/archive/1.0.10.zip"
  $scassandra_path_zip = "$($dep_dir)\scassandra-server.zip"
  (new-object System.Net.WebClient).DownloadFile($scassandra_url, $scassandra_path_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($scassandra_path_zip, $dep_dir)
}

$scassandra_build_path = "$($scassandra_path)\server\build\libs"
If (!(Test-Path $scassandra_build_path)) {
  Write-Host "Building scassandra..."
  pushd $scassandra_path
  Start-Process cmd -ArgumentList "/c gradlew server:fatJar" -Wait -NoNewWindow
  popd
}

$scassandra_standalone_regex = 'scassandra-server.*standalone\.jar'
$scassandra_jar_path = Get-ChildItem -Path $scassandra_build_path | Where-Object -FilterScript {$_.Name -match $scassandra_standalone_regex}

$env:SCASSANDRA_JAR="$($scassandra_build_path)\$($scassandra_jar_path)"
Write-Host $env:SCASSANDRA_JAR 
[Environment]::SetEnvironmentVariable("SCASSANDRA_JAR", "$($env:SCASSANDRA_JAR)", "User")
