param(
    [Parameter(Mandatory=$true)][string]$Edge,
    [string]$Credentials = 'bin/cf-h3-direction-20261016/credentials.tmp',
    [string]$Output = 'test-results/cf-h3-direction-20261016',
    [string]$Executable = 'bin/cf-h3-direction-20261016/probe-v3.exe',
    [string]$Suffix = ''
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$sslocal = Join-Path $root 'test-results/windows-sslocal-socks5-20260701/sslocal.exe'
$cases = @(
    @{Id='tcp-down-only-r1'; Mode='tcp-h3'; Duration='45s'; Up='0s'},
    @{Id='tcp-up5-stop32-r1'; Mode='tcp-h3'; Duration='55s'; Up='5s'},
    @{Id='tcp-up10-stop32-r1'; Mode='tcp-h3'; Duration='55s'; Up='10s'},
    @{Id='tcp-up14-stop32-r1'; Mode='tcp-h3'; Duration='55s'; Up='14s'},
    @{Id='tcp-up16-r1'; Mode='tcp-h3'; Duration='40s'; Up='16s'},
    @{Id='tcp-up20-r1'; Mode='tcp-h3'; Duration='40s'; Up='20s'},
    @{Id='tcp-quiet-r1'; Mode='tcp-h3'; Duration='40s'; Up='0s'; Down='0s'},
    @{Id='tcp-down-guard-r1'; Mode='tcp-h3'; Duration='40s'; Up='0s'; Guard=$true},
    @{Id='tcp-ws-down-r1'; Mode='tcp-ws'; Duration='40s'; Up='0s'},
    @{Id='tcp-both-r1'; Mode='tcp-h3'; Duration='40s'; Up='5s'; Stop='90s'},
    @{Id='tcp-fin2-r1'; Mode='tcp-h3'; Duration='40s'; Up='0s'; Finish='2s'},
    @{Id='tcp-down-only-r2'; Mode='tcp-h3'; Duration='45s'; Up='0s'},
    @{Id='tcp-up10-stop32-r2'; Mode='tcp-h3'; Duration='55s'; Up='10s'},
    @{Id='tcp-up14-stop32-r2'; Mode='tcp-h3'; Duration='55s'; Up='14s'}
)
$cases | ForEach-Object { $_.Id += $Suffix }
$active = @()
try {
    for ($i=0; $i -lt $cases.Count -or $active.Count -gt 0;) {
        while ($i -lt $cases.Count -and $active.Count -lt 4) {
            $case=$cases[$i]
            $dir = Join-Path $root "$Output/$($case.Id)"
            if (Test-Path -LiteralPath "$dir/exit.txt") { $i++; continue }
            New-Item -ItemType Directory -Force "$dir/qlog" | Out-Null
            $args=@('-mode',$case.Mode,'-id',$case.Id,'-url','https://relay.example.com:2096/','-edge',$Edge,
                '-credentials',(Join-Path $root $Credentials),'-sslocal',$sslocal,'-out',$dir,
                '-plugin-port',"$(11500+2*$i)",'-socks-port',"$(11501+2*$i)",
                '-duration',$case.Duration,'-up-interval',$case.Up,
                '-stop-up-after',$(if($case.Stop){$case.Stop}else{'32s'}),
                '-down-interval',$(if($case.Down){$case.Down}else{'100ms'}))
            if($case.Guard){$args+='-guard'}
            if($case.Finish){$args+=@('-finish-after',$case.Finish)}
            $case | ConvertTo-Json | Set-Content "$dir/parameters.json"
            $p=Start-Process -FilePath (Join-Path $root $Executable) -ArgumentList $args -WindowStyle Hidden -PassThru `
                -Environment @{TCP_PROXY_QLOG_DIR="$dir/qlog"} -RedirectStandardOutput "$dir/client.jsonl" -RedirectStandardError "$dir/client.err.txt"
            $active+=@{Process=$p;Id=$case.Id;Dir=$dir}
            "START $($case.Id) pid=$($p.Id)"
            $i++
        }
        Start-Sleep -Seconds 1
        $remaining=@()
        foreach($item in $active){
            $item.Process.Refresh()
            if($item.Process.HasExited){
                $item.Process.WaitForExit()
                [IO.File]::WriteAllText("$($item.Dir)/exit.txt","$($item.Process.ExitCode)`n")
                "END $($item.Id) exit=$($item.Process.ExitCode)"
            }else{$remaining+=$item}
        }
        $active=$remaining
    }
} finally {
    # The probe owns a bounded context and orderly plugin shutdown. Do not kill
    # a running H3 trace just to make this script exit faster.
    foreach($item in $active){"LEFT_RUNNING $($item.Id) pid=$($item.Process.Id)"}
}
