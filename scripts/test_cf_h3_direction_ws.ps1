param([Parameter(Mandatory=$true)][string]$Edge)
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$dir=Join-Path $root 'test-results/cf-h3-direction-20261016/v2-ws-down'
New-Item -ItemType Directory -Force $dir | Out-Null
$src=Get-Content (Join-Path $root 'bin/cf-h3-direction-20261016/credentials-v2.tmp') -Raw | ConvertFrom-Json
$src.server=$Edge
$src.server_port=2053
$src | Add-Member -NotePropertyName local_address -NotePropertyValue '127.0.0.1' -Force
$src | Add-Member -NotePropertyName local_port -NotePropertyValue 11781 -Force
$src.plugin=Join-Path $root 'bin/tcp-qlog-investigation-20261016-2005/v2ray-plugin_windows_amd64.exe'
$src.plugin_opts=($src.plugin_opts -split ';' | Where-Object { $_ -ne 'server' }) -join ';'
$path=Join-Path $root 'bin/cf-h3-direction-20261016/v2-client-config.tmp'
[IO.File]::WriteAllText($path,($src|ConvertTo-Json))
$p=$null
try{
 $p=Start-Process -FilePath (Join-Path $root 'test-results/windows-sslocal-socks5-20260701/sslocal.exe') -ArgumentList @('-c',$path) -WindowStyle Hidden -PassThru -RedirectStandardOutput "$dir/sslocal.stdout.txt" -RedirectStandardError "$dir/sslocal.stderr.txt"
 Start-Sleep -Seconds 2
 & (Join-Path $root 'bin/cf-h3-direction-20261016/probe-v4.exe') -mode socks -id v2-ws-down -edge $Edge -url 'https://relay.example.com:2053/' -socks-port 11781 -target 127.0.0.1:28096 -duration 40s -up-interval 0s -out $dir > "$dir/client.jsonl" 2> "$dir/client.err.txt"
 Set-Content "$dir/exit.txt" $LASTEXITCODE
}finally{
 if($p){
  $children=@(Get-CimInstance Win32_Process -Filter "ParentProcessId=$($p.Id)" | Where-Object Name -eq 'v2ray-plugin_windows_amd64.exe')
  foreach($child in $children){Stop-Process -Id $child.ProcessId -ErrorAction SilentlyContinue}
  Stop-Process -Id $p.Id -ErrorAction SilentlyContinue
 }
 Remove-Item -LiteralPath $path -ErrorAction SilentlyContinue
}
