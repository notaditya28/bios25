# Starts, stops and kills Hydra Stream brokers as local processes.
#
#   .\scripts\hydra.ps1 start [-Region IN]     build, then start every broker (or one region's)
#   .\scripts\hydra.ps1 stop                   stop every broker this script started
#   .\scripts\hydra.ps1 kill -Id in-1          hard-kill one broker (simulated crash)
#   .\scripts\hydra.ps1 restart -Id in-1       start one broker again
#   .\scripts\hydra.ps1 status                 show which brokers are running
#   .\scripts\hydra.ps1 demo                   build, start all 9 brokers + load + dashboard (one command)
#   .\scripts\hydra.ps1 chaos                  build and run every chaos scenario, print the results table
#
# Logs go to data\logs\<id>.log, pids to data\pids\<id>.pid.
param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("start", "stop", "kill", "restart", "status", "demo", "chaos")]
    [string]$Action,
    [string]$Region = "",
    [string]$Id = "",
    [string]$Config = "configs/cluster.local.json"
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
if (-not (Get-Command go -ErrorAction SilentlyContinue) -and (Test-Path "C:\Program Files\Go\bin\go.exe")) {
    $env:Path = "C:\Program Files\Go\bin;$env:Path"   # terminals opened before Go was installed
}
$cluster = Get-Content $Config -Raw | ConvertFrom-Json
New-Item -ItemType Directory -Force data\logs, data\pids | Out-Null

function Get-Brokers {
    foreach ($r in $cluster.regions) {
        if ($Region -and $r.name -ne $Region) { continue }
        foreach ($b in $r.brokers) {
            if ($Id -and $b.id -ne $Id) { continue }
            [pscustomobject]@{ Id = $b.id; Region = $r.name; Addr = $b.addr }
        }
    }
}

function Get-BrokerProcess($id) {
    $pidFile = "data\pids\$id.pid"
    if (-not (Test-Path $pidFile)) { return $null }
    $p = Get-Process -Id ([int](Get-Content $pidFile)) -ErrorAction SilentlyContinue
    if ($p -and $p.ProcessName -eq "hydra-broker") { return $p }
    return $null
}

function Start-Broker($b) {
    if (Get-BrokerProcess $b.Id) { Write-Host "$($b.Id) already running"; return }
    $p = Start-Process -FilePath .\bin\hydra-broker.exe -ArgumentList "-config", $Config, "-id", $b.Id, "-policies", "configs/policies.signed.json" `
        -RedirectStandardError "data\logs\$($b.Id).log" -RedirectStandardOutput "data\logs\$($b.Id).out" `
        -WindowStyle Hidden -PassThru
    Set-Content "data\pids\$($b.Id).pid" $p.Id
    Write-Host "started $($b.Id) ($($b.Region)) on $($b.Addr) pid $($p.Id)"
}

switch ($Action) {
    # Windows PowerShell treats a program's stderr (our log lines) as errors, so these run via
    # Start-Process with stderr sent to a log file; stdout (results, URL) stays on the console.
    "demo" {
        go build -o bin/ ./cmd/...
        if ($LASTEXITCODE -ne 0) { exit 1 }
        Write-Host "starting 9 brokers + load + dashboard (logs: hydra-dash.log). Ctrl+C stops everything."
        $p = Start-Process -FilePath .\bin\hydra-dash.exe -ArgumentList "-config", $Config `
            -NoNewWindow -PassThru -Wait -RedirectStandardError hydra-dash.log
        exit $p.ExitCode
    }
    "chaos" {
        go build -o bin/ ./cmd/...
        if ($LASTEXITCODE -ne 0) { exit 1 }
        Write-Host "running every chaos scenario (about 2-3 minutes; progress log: hydra-chaos.log)..."
        $p = Start-Process -FilePath .\bin\hydra-chaos.exe -ArgumentList "-config", $Config `
            -NoNewWindow -PassThru -Wait -RedirectStandardError hydra-chaos.log
        exit $p.ExitCode
    }
    "start" {
        go build -o bin/ ./cmd/...
        if ($LASTEXITCODE -ne 0) { exit 1 }
        .\bin\hydra-policy.exe ensure -config $Config   # admin key + signed policies (once)
        if ($LASTEXITCODE -ne 0) { exit 1 }
        Get-Brokers | ForEach-Object { Start-Broker $_ }
    }
    "restart" {
        if (-not $Id) { throw "restart needs -Id" }
        Get-Brokers | ForEach-Object { Start-Broker $_ }
    }
    "kill" {
        if (-not $Id -and -not $Region) { throw "kill needs -Id or -Region" }
        Get-Brokers | ForEach-Object {
            $p = Get-BrokerProcess $_.Id
            if ($p) { Stop-Process -Id $p.Id -Force; Write-Host "killed $($_.Id)" }
        }
    }
    "stop" {
        Get-Brokers | ForEach-Object {
            $p = Get-BrokerProcess $_.Id
            if ($p) { Stop-Process -Id $p.Id -Force; Write-Host "stopped $($_.Id)" }
        }
    }
    "status" {
        Get-Brokers | ForEach-Object {
            $state = if (Get-BrokerProcess $_.Id) { "up" } else { "down" }
            Write-Host ("{0,-6} {1,-3} {2,-16} {3}" -f $_.Id, $_.Region, $_.Addr, $state)
        }
    }
}
