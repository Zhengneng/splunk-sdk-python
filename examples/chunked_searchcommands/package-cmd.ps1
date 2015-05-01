[CmdletBinding()]
param()

python "${PSScriptRoot}/setup.py" package --build-number=" $(git log -1 --pretty=format:%ct)"
