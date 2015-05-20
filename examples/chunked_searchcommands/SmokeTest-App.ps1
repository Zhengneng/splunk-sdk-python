[CmdletBinding()]
param(
    [parameter(Mandatory = $true, Position=1)]
    [string]
    $SearchCommands
)

Get-Item -ErrorAction SilentlyContinue "$env:SPLUNK_HOME\var\log\splunk\splunklib.log" | Remove-Item

Get-Content $SearchCommands | ForEach-Object {
    splunk search "$_" -app chunked_searchcommands -auth admin:changeme -maxout 0 -output csv
}
