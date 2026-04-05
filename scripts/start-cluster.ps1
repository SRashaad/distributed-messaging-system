# Opens one terminal per cluster node (default: 3 nodes on ports 5001–5003).
# Requires ZooKeeper already running (default: localhost:2181).
#
# Usage (from anywhere):
#   .\scripts\start-cluster.ps1
#   .\scripts\start-cluster.ps1 -NodeCount 5
#   .\scripts\start-cluster.ps1 -Zk "localhost:2181" -UseBinary ".\bin\server.exe"
#
param(
    [ValidateRange(3, 9)]
    [int]$NodeCount = 3,

    [string]$Zk = "localhost:2181",

    # If set, runs this executable instead of "go run" (build first: go build -o bin/server.exe ./cmd/server)
    [string]$UseBinary = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

function Get-PeersForPort {
    param([int]$Port)
    $all = @(5001..(5000 + $NodeCount))
    $others = $all | Where-Object { $_ -ne $Port }
    return ($others | ForEach-Object { "localhost:$_" }) -join ","
}

for ($i = 1; $i -le $NodeCount; $i++) {
    $port = 5000 + $i
    $id = "node$i"
    $peers = Get-PeersForPort -Port $port

    if ($UseBinary -ne "") {
        $exePath = if ([System.IO.Path]::IsPathRooted($UseBinary)) { $UseBinary } else { Join-Path $RepoRoot $UseBinary }
        $inner = "Set-Location -LiteralPath '$RepoRoot'; & '$exePath' -id $id -port $port -peers '$peers' -zk '$Zk'"
    }
    else {
        $inner = "Set-Location -LiteralPath '$RepoRoot'; go run ./cmd/server -id $id -port $port -peers '$peers' -zk '$Zk'"
    }

    Start-Process powershell.exe -WorkingDirectory $RepoRoot -ArgumentList @("-NoExit", "-Command", $inner) | Out-Null
    Write-Host "Started $id on port $port (new window)"
}

Write-Host ""
Write-Host "Launched $NodeCount node window(s). Close each with Ctrl+C when done."
