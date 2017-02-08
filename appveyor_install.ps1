$env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0"
$env:PYTHON="C:\Python27-x64"
$env:PATH="$($env:PYTHON);$($env:PYTHON)\Scripts;$($env:JAVA_HOME)\bin;$($env:PATH)"
$dep_dir="C:\Users\appveyor\deps"

$computerSystem = Get-CimInstance CIM_ComputerSystem
$computerOS = Get-CimInstance CIM_OperatingSystem
$computerCPU = Get-CimInstance CIM_Processor

Write-Host "System Information for: " $computerSystem.Name
"CPU: " + $computerCPU.Name
"RAM: " + "{0:N2}" -f ($computerSystem.TotalPhysicalMemory/1GB) + "GB"

Write-Host "Install..."

If (!(Test-Path $dep_dir)) {
  Write-Host "Creating $($dep_dir)"
  New-Item -Path $dep_dir -ItemType Directory -Force
}

# Install Ant
$ant_base = "$($dep_dir)\ant"
$ant_path = "$($ant_base)\apache-ant-1.9.7"
If (!(Test-Path $ant_path)) {
  Write-Host "Installing Ant"
  $ant_url = "https://www.dropbox.com/s/lgx95x1jr6s787l/apache-ant-1.9.7-bin.zip?dl=1"
  $ant_zip = "C:\Users\appveyor\apache-ant-1.9.7-bin.zip"
  (new-object System.Net.WebClient).DownloadFile($ant_url, $ant_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($ant_zip, $ant_base)
}
$env:PATH="$($ant_path)\bin;$($env:PATH)"

Write-Host "Installing java Cryptographic Extensions, needed for SSL..."
# Install Java Cryptographic Extensions, needed for SSL.
$target = "$($env:JAVA_HOME)\jre\lib\security"
# If this file doesn't exist we know JCE hasn't been installed.
$jce_indicator = "$target\README.txt"
$zip = "C:\Users\appveyor\jce_policy-8.zip"

If (!(Test-Path $jce_indicator)) {
  # Download zip to staging area if it doesn't exist, we do this because
  # we extract it to the directory based on the platform and we want to cache
  # this file so it can apply to all platforms.
  if(!(Test-Path $zip)) {
    $url = "https://www.dropbox.com/s/al1e6e92cjdv7m7/jce_policy-8.zip?dl=1"
    Write-Host "Downloading file..."
    (new-object System.Net.WebClient).DownloadFile($url, $zip)
    Write-Host "Download completed."
  }

  Add-Type -AssemblyName System.IO.Compression.FileSystem
  Write-Host "Extracting zip file..."
  [System.IO.Compression.ZipFile]::ExtractToDirectory($zip, $target)
  Write-Host "Extraction completed."

  $jcePolicyDir = "$target\UnlimitedJCEPolicyJDK8"
  Move-Item $jcePolicyDir\* $target\ -force
  Remove-Item $jcePolicyDir
}

# Install Python Dependencies for CCM.
Write-Host "Installing CCM and its dependencies"
Start-Process python -ArgumentList "-m pip install psutil pyYaml six" -Wait -NoNewWindow

$env:CCM_PATH="$($dep_dir)\ccm"

# Clone ccm from git and use master.
If (Test-Path $env:CCM_PATH) {
  Remove-Item $env:CCM_PATH -force
}

If (!(Test-Path $env:CCM_PATH)) {
  Write-Host "Cloning git ccm... $($env:CCM_PATH)"
  Start-Process git -ArgumentList "clone https://github.com/pcmanus/ccm.git $($env:CCM_PATH)" -Wait -NoNewWindow
  Write-Host "git ccm cloned"
  pushd $env:CCM_PATH
  Start-Process python -ArgumentList "setup.py install" -Wait -NoNewWindow
  popd
}

Write-Host "Set execution Policy"
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process

#removing any existing ccm cluster
Write-Host "Removing any existing ccm clusters"

$params = "/c ccm list"
& "cmd.exe" $params | Tee-Object -Variable scriptOutput | Out-Null

If ($scriptOutput)
{
  $list = $scriptOutput.Split(" ")
  Write-Host "[ccm] list $($list)"

  Foreach ($cluster in $list)
  {
    If (-Not $cluster.equals(""))
    {
      $name = $cluster.Replace("*", "")
      & "cmd.exe" "/c ccm remove $($name)" | Tee-Object -Variable result | Out-Null
      Write-Host "[ccm] remove $($name) $($result)"
    }
  }

  & "cmd.exe" $params | Tee-Object -Variable scriptOutputEnd | Out-Null
  Write-Host "[ccm] list $($scriptOutputEnd)"
}

Write-Host "[Install] Check installed cassandra version $($env:cassandra_version)"
# Predownload cassandra version for CCM if it isn't already downloaded.
If (!(Test-Path C:\Users\appveyor\.ccm\repository\$env:cassandra_version)) {
  Write-Host "[Install] Install cassandra version $($env:cassandra_version)"
  Start-Process python -ArgumentList "$($env:CCM_PATH)\ccm.py create -v $($env:cassandra_version) -n 1 predownload" -Wait -NoNewWindow
  Start-Process python -ArgumentList "$($env:CCM_PATH)\ccm.py remove predownload" -Wait -NoNewWindow
} else {
  Write-Host "Cassandra $env:cassandra_version was already preloaded"
}
