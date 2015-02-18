<#
    Interactive script to guide tester through setting up a Windows Test Environment 
  to run Cassandra CSharp client integration tests.
  
  NOTE: Right now this needs to be run by a human.
  
  Environment Requirements/Assumptions: 
  - Windows with .NET/Powershell already installed
  - 64 bit
  
  Components that will be installed:
  NUnit, Python 2.7, PyYAML, (python) Six,
#>

# import dependencies
. ./TestUtils.ps1
. ./ComponentUtils.ps1
. ./EnvGlobals.ps1

$downloadDir = "$env:TEST_DOWNLOAD_DIR"

###################
# NUnit
###################
NUnitBootstrap

######################
# Python
######################

# skip this step if the correct version of python is already installed.
$pythonVersionNumbers = "2.7.9"
$requiredPythonVersion = "Python $pythonVersionNumbers"
$output = cmd /c python --version 2>&1 | foreach-object {$_.tostring()}
#Start-Process cmd.exe -ArgumentList "python --version" -WindowStyle $windowStyle -Wait -OutVariable buildOutput
if ("$output".equals($requiredPythonVersion)) {
  echo "Required Python version -- $requiredPythonVersion -- already installed."
} else {
  echo "Installing $requiredPythonVersion now ..."
  $pythonDownloadDir = "$downloadDir\PythonDownload"
  echo "Creating directory $pythonDownloadDir"
  New-Item -ItemType Directory -Force -Path $pythonDownloadDir
  $installerFileName = "python-2.7.9.amd64.msi"
  $downloadUrl = "https://www.python.org/ftp/python/2.7.9/$installerFileName"
  $installerFileFullPath = "$pythonDownloadDir\$installerFileName"
  DownloadFileUrlIfFileNotExists $downloadUrl $pythonDownloadDir
  echo "Using installer file: $installerFileFullPath"
  # example: msiexec -i C:\CSharpDownloads\PythonDownload\python-2.7.9.amd64.msi /passive
  msiexec -i "$installerFileFullPath" /promptrestart
  echo ""
  echo "*** ATTENTION! ***"
  echo "You MUST select the option to add python.exe to 'Path' for remaining dependencies to install correctly!"
  echo ""
  WaitForUserKey
  
  echo "If this is the first time you have installed $requiredPythonVersion on this computer, then you must reboot now the remainder of the set up process may fail. " 
  $question = "Restart Now (Y/N)?"
  $answer = Read-Host $question
  $answerStr = [string]$answer
  while("Y","N" -notcontains $answerStr)  {
    $answerStr = Read-Host $question
  }
  if ($answerStr.StartsWith("Y")) {
    echo "Restarting computer NOW!"
    Restart-Computer
    exit
  } else {
    echo "WARNING: NOT Rebooting now."
  }
}
echo "DONE."

################################
# PyYAML
################################

# NOTE `pip install pyyaml` results in warning messages, including this longer version to make sure it gets installed correctly
echo "Installing PyYAML now ..."
$componentDownloadDir = "$downloadDir\PyYAML"
echo "Creating directory $componentDownloadDir"
New-Item -ItemType Directory -Force -Path $componentDownloadDir
$installerFileName = "PyYAML-3.11.win-amd64-py2.7.exe"
$downloadUrl = "http://pyyaml.org/download/pyyaml/$installerFileName"
$installerFileFullPath = "$componentDownloadDir\$installerFileName"
DownloadFileUrlIfFileNotExists $downloadUrl $componentDownloadDir
echo "Using installer file: $installerFileFullPath"
cmd /c $installerFileFullPath
echo "DONE."

################################
# Six
################################

echo "Installing Six now ..."
pip install six
echo "DONE."

################################
# psutil
################################

echo "Installing psutil now ..."
$componentDownloadDir = "$downloadDir\psutil"
echo "Creating directory $componentDownloadDir"
New-Item -ItemType Directory -Force -Path $componentDownloadDir
$installerFileName = "psutil-2.2.0.win-amd64-py2.7.exe"
$downloadUrl = "https://pypi.python.org/packages/2.7/p/psutil/$installerFileName"
$installerFileFullPath = "$componentDownloadDir\$installerFileName"
DownloadFileUrlIfFileNotExists $downloadUrl $componentDownloadDir
echo "Using installer file: $installerFileFullPath"
cmd /c $installerFileFullPath
echo "DONE."

#################################
# Java Version Check
#################################

echo "Checking java installation now ..."
$output = cmd /c java -version 2>&1 | foreach-object {$_.tostring()}
echo "Java info: $output"
if (!"$output".Contains("1.7.") -or !"$output".Contains("64-Bit")) {
  $componentDownloadDir = "$downloadDir\jdk1-7"
  $installedDir = "$env:PROGRAMFILES" + "\Java\jdk1.7.0_75"
  $downloadUrl = "http://download.oracle.com/otn-pub/java/jdk/7u75-b13/jdk-7u75-windows-x64.exe"
  $installerFileName = [System.IO.Path]::GetFileName($downloadUrl)
  $installerFileFullPath = "$componentDownloadDir\$installerFileName"
  If (!(Test-Path -Path $installerFileFullPath)) {
    $webclient = New-Object System.Net.WebClient
    $webcookie = "oraclelicense=accept-securebackup-cookie"
    $webclient.Headers.Add([System.Net.HttpRequestHeader]::Cookie, $webcookie) 
    echo "downloading from: $downloadUrl to: $installerFileFullPath"
    If (!(Test-Path -Path $componentDownloadDir)) {
      New-Item -ItemType directory -Path "$componentDownloadDir"
    }
    $webclient.DownloadFile($downloadUrl, $installerFileFullPath)
  } Else {
    echo "File $installerFileFullPath already exists, no need to re-download."
  }
  echo "Using installer file: $installerFileFullPath"
  cmd /c $installerFileFullPath /s ADDLOCAL="ToolsFeature,SourceFeature"
  [Environment]::SetEnvironmentVariable("JAVA_HOME", "$installedDir" , [System.EnvironmentVariableTarget]::User)
  [Environment]::SetEnvironmentVariable("Path", $env:Path + ";" + "$env:JAVA_HOME" + "\bin" , [System.EnvironmentVariableTarget]::User)
  echo "DONE."
} else {
   echo "Verified java installation is version 1.7, for 64-Bit Windows"
}

###############################
# Ant
###############################
AntBootstrap

WaitForUserKey









