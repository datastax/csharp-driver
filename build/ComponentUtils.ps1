
. ./EnvGlobals.ps1
$downloadDir = "$env:TEST_DOWNLOAD_DIR"

function NUnitBootstrap() {
  $nUnitVersion = "2.6.4"
  $componentName = "NUnit-$nUnitVersion"
  $componentDownloadDir = "$downloadDir\$componentName"
  $componentDestinationDir = "$componentDownloadDir\$componentName\bin\"

    If (!(Test-Path -Path $componentDestinationDir)) {
    echo "Creating directory $componentDownloadDir"
    New-Item -ItemType Directory -Force -Path $componentDownloadDir

    $nUnitZipFileName = "$componentName.zip"
    $nUnitDownloadUrl = "http://github.com/nunit/nunitv2/releases/download/$nUnitVersion/$nUnitZipFileName"
    DownloadFileUrlIfFileNotExists $nUnitDownloadUrl $componentDownloadDir
    $nUnitZipFileFullPath = "$componentDownloadDir\$nUnitZipFileName"
    ExpandZipFile $nUnitZipFileFullPath $componentDownloadDir
    } Else {
        echo "File $componentDestinationDir already exists, no need to unzip files again."
    }

  # set the NUNIT_PATH environment variable
  [Environment]::SetEnvironmentVariable("NUNIT_PATH", "$componentDestinationDir" , [System.EnvironmentVariableTarget]::User)
  [Environment]::SetEnvironmentVariable("Path", $env:Path + ";" + "$env:NUNIT_PATH", [System.EnvironmentVariableTarget]::User)

}

function AntBootstrap() {
  $componentName = "apache-ant-1.9.4"
  $componentNameBinAppended = "$componentName-bin"
  $componentDownloadDir = "$downloadDir\Ant"
  $componentDestinationDir = "$componentDownloadDir\$componentName\bin\"
    If (!(Test-Path -Path $componentDestinationDir)) {
    echo "Creating directory $componentDownloadDir"
    New-Item -ItemType Directory -Force -Path $componentDownloadDir

    $zipFileName = "$componentNameBinAppended.zip"
    $downloadUrl = "http://apache.mesi.com.ar/ant/binaries/$componentNameBinAppended.zip"
    DownloadFileUrlIfFileNotExists $downloadUrl $componentDownloadDir
    $zipFileFullPath = "$componentDownloadDir\$zipFileName"
    ExpandZipFile $zipFileFullPath $componentDownloadDir
    } Else {
        echo "File $componentDestinationDir already exists, no need to unzip files again."
    }

  # append to the current Path environment variable
  [Environment]::SetEnvironmentVariable("Path", $env:Path + ";" + "$componentDestinationDir", [System.EnvironmentVariableTarget]::User)
  echo "Current Path value: $env:Path" 
}