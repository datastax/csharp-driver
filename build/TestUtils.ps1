<#
	Utility functions that can be imported and used anywhere
#>

function ExpandZipFile($sourceZipFile, $destinationDir) {
    echo "un-zipping from: $sourceZipFile to $destinationDir"
    $shell = new-object -com shell.application
    $source = $shell.NameSpace($sourceZipFile)
    $destination = $shell.NameSpace($destinationDir)
    # force overwrite if extracted file already exists
    $destination.copyhere($source.items(), 0x14)
}

function DownloadFileUrlIfFileNotExists($downloadUrl, $destinationDir) {
    $destinationFileName = $downloadUrl.split("/")[-1];
    $fullDestinationFilePath = "$destinationDir\$destinationFileName"
    If (!(Test-Path -Path $fullDestinationFilePath)) {
        $webclient = New-Object System.Net.WebClient
        echo "downloading from: $downloadUrl to: $fullDestinationFilePath"
        $webclient.DownloadFile($downloadUrl, $fullDestinationFilePath)
    } Else {
        echo "File $fullDestinationFilePath already exists, no need to re-download."
    }
}

function WaitForUserKey() {
	Write-Host "Press any key to continue ..."
	$x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}
