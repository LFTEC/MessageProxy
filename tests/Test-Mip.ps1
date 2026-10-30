param([int]$Port = 18089)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$env:DOTNET_CLI_HOME = Join-Path $root '.dotnet-local'
$work = Join-Path $PSScriptRoot 'generated'
New-Item -ItemType Directory -Force $work | Out-Null
$source = [IO.File]::ReadAllText((Join-Path $root 'MessageProxy/Program.cs'))
$start = $source.IndexOf('app.MapPost("/mip"')
$end = $source.IndexOf('app.Run();', $start)
$modelsStart = $source.IndexOf('public class MIPInputData')
$modelsEnd = $source.IndexOf('internal class RabbitMQConnectionOptions', $modelsStart)
# Run the production endpoint and models verbatim, without unrelated RabbitMQ startup.
$hostSource = "using Google.OrTools.LinearSolver;`nvar builder = WebApplication.CreateBuilder(args);`nvar app = builder.Build();`n" + $source.Substring($start, $end-$start) + "`napp.Run();`n" + $source.Substring($modelsStart,$modelsEnd-$modelsStart)
[IO.File]::WriteAllText((Join-Path $work 'Program.cs'), $hostSource)
@"
<Project Sdk="Microsoft.NET.Sdk.Web">
<PropertyGroup><TargetFramework>net10.0</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable><NoWarn>CS1998;CS0436</NoWarn></PropertyGroup>
<ItemGroup><ProjectReference Include="../../MessageProxy/MessageProxy.csproj" /></ItemGroup>
</Project>
"@ | Set-Content (Join-Path $work 'MipHarness.csproj')
& dotnet build (Join-Path $root 'MessageProxy/MessageProxy.csproj') --no-restore
if ($LASTEXITCODE) { throw 'Production build failed' }
$assets = Get-Content (Join-Path $root 'MessageProxy/obj/project.assets.json') -Raw | ConvertFrom-Json
$cache = @($assets.packageFolders.PSObject.Properties.Name)[0]
& dotnet build (Join-Path $work 'MipHarness.csproj') --source $cache "-p:RestorePackagesPath=$cache" -p:NuGetAudit=false
if ($LASTEXITCODE) { throw 'Harness build failed' }
$stdout = Join-Path $work 'host.stdout.log'
$stderr = Join-Path $work 'host.stderr.log'
$process = Start-Process dotnet -ArgumentList @((Join-Path $work 'bin/Debug/net10.0/MipHarness.dll'), '--urls', "http://127.0.0.1:$Port") -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr
$results = [Collections.Generic.List[object]]::new()
function Demand($key,$qty,$single=$false,$mat='M') { @{resKey=$key;matNo=$mat;quantity=$qty;requireSingleVendor=$single} }
function Vendor($key,$qty,$mat='M') { @{vendorKey=$key;stocks=@(@{matNo=$mat;quantity=$qty;offset=0;priority=0})} }
function DualVendor($key,$m,$n) { @{vendorKey=$key;stocks=@(@{matNo='M';quantity=$m;offset=0;priority=0},@{matNo='N';quantity=$n;offset=0;priority=0})} }
function AllocationQty($r,$res,$vendor) { [decimal]($r.allocations | Where-Object { $_.resKey -eq $res -and (!$vendor -or $_.vendorKey -eq $vendor) } | Measure-Object allocatedQuantity -Sum).Sum }

function Check($condition,$message) { if (!$condition) { throw $message } }
function RunCase($name,$demands,$vendors,$verify) {
    $body = @{reservations=@($demands);vendors=@($vendors);params=@{paramQty=2000;paramVendor=1900;paramOffset=10}} | ConvertTo-Json -Depth 12
    $response = $null
    try {
        $response = Invoke-RestMethod -NoProxy "http://127.0.0.1:$Port/mip" -Method Post -ContentType application/json -Body $body
        & $verify $response
        $results.Add([pscustomobject]@{name=$name;status='PASS';error='';request=($body|ConvertFrom-Json);response=$response})
    } catch { $results.Add([pscustomobject]@{name=$name;status='FAIL';error=$_.Exception.Message;request=($body|ConvertFrom-Json);response=$response}) }
}
try {
    $ready=$false
    for($i=0;$i -lt 60;$i++) {
        if($process.HasExited){throw "Host exited: $(Get-Content $stderr -Raw)"}
        try { Invoke-RestMethod -NoProxy "http://127.0.0.1:$Port/mip" -Method Post -ContentType application/json -Body '{}' | Out-Null; $ready=$true; break } catch { $lastStartupError = $_.Exception.Message; Start-Sleep -Milliseconds 250 }
    }
    if(!$ready){throw "Host startup timed out: $($_.Exception.Message) $lastStartupError"}
    RunCase 'Single vendor fulfills entire demand' @((Demand R1 100 $true)) @((Vendor A 120),(Vendor B 60)) {param($r) Check ($r.allocations.Count -eq 1 -and $r.allocations[0].vendorKey -eq 'A' -and $r.totalQuantityAllocated -eq 100) 'Expected A to supply 100'}
    RunCase 'Combined stock cannot fulfill special demand' @((Demand R1 100 $true)) @((Vendor A 60),(Vendor B 60)) {param($r) Check ($r.allocations.Count -eq 0) 'Special demand must not split'}
    RunCase 'Ordinary demand can split' @((Demand R1 100)) @((Vendor A 60),(Vendor B 60)) {param($r) Check ($r.allocations.Count -eq 2 -and $r.totalQuantityAllocated -eq 100) 'Expected split allocation totaling 100'}
    RunCase 'Missing material cannot supply demand' @((Demand R1 100 $true)) @((Vendor A 120 X)) {param($r) Check ($r.allocations.Count -eq 0) 'Expected no allocations'}
    RunCase 'Exact stock fulfills special demand' @((Demand R1 100 $true)) @((Vendor A 100)) {param($r) Check ($r.totalQuantityAllocated -eq 100) 'Expected full allocation'}
    RunCase 'Ordinary demand reuses selected supplier' @((Demand R1 100 $true),(Demand R2 20)) @((Vendor A 120),(Vendor B 20)) {param($r) Check ($r.totalQuantityAllocated -eq 120 -and $r.totalVendorsUsed -eq 1) 'Expected both demands at A'}
    RunCase 'Competing special demands respect stock' @((Demand R1 100 $true),(Demand R2 100 $true)) @((Vendor A 150)) {param($r) Check ($r.allocations.Count -eq 1 -and $r.totalQuantityAllocated -eq 100) 'Expected one whole demand only'}
    RunCase 'Zero quantity creates no allocation' @((Demand R1 0 $true)) @((Vendor A 100)) {param($r) Check ($r.allocations.Count -eq 0) 'Expected no allocation'}
    $dualDemands = @((Demand R1 100 $true M),(Demand R2 80 $false N))
    RunCase 'M/N both suppliers have sufficient stock' $dualDemands @((DualVendor A 120 100),(DualVendor B 120 100)) {param($r)
        Check ($r.totalVendorsUsed -eq 1 -and $r.totalQuantityAllocated -eq 180 -and $r.allocations.Count -eq 2) 'Expected M=100 and N=80 at the same supplier'
    }
    RunCase 'M/N only A can fulfill M and both can fulfill N' $dualDemands @((DualVendor A 100 80),(DualVendor B 60 80)) {param($r)
        Check ((AllocationQty $r R1 A) -eq 100 -and (AllocationQty $r R2 A) -eq 80 -and $r.totalVendorsUsed -eq 1) 'Expected both demands at A'
    }
    RunCase 'M/N choose supplier with enough stock for both' $dualDemands @((DualVendor A 100 30),(DualVendor B 100 80)) {param($r)
        Check ((AllocationQty $r R1 B) -eq 100 -and (AllocationQty $r R2 B) -eq 80 -and $r.totalVendorsUsed -eq 1) 'Expected both demands at B'
    }
    RunCase 'M/N selected supplier lacks N but combined N is sufficient' $dualDemands @((DualVendor A 100 30),(DualVendor B 60 60)) {param($r)
        Check ((AllocationQty $r R1 A) -eq 100 -and (AllocationQty $r R2) -eq 80 -and (AllocationQty $r R2 A) -ge 20 -and (AllocationQty $r R2 A) -le 30 -and $r.totalVendorsUsed -eq 2) 'Expected M at A and N split within inventory limits'
    }
    RunCase 'M/N total N stock insufficient' $dualDemands @((DualVendor A 100 30),(DualVendor B 60 20)) {param($r)
        Check ((AllocationQty $r R1 A) -eq 100 -and (AllocationQty $r R2 A) -eq 30 -and (AllocationQty $r R2 B) -eq 20 -and $r.totalQuantityAllocated -eq 150) 'Expected M=100, N=50, N shortage=30'
    }
    RunCase 'M/N no supplier can fulfill M even with sufficient combined M' $dualDemands @((DualVendor A 60 80),(DualVendor B 60 80)) {param($r)
        Check ((AllocationQty $r R1) -eq 0 -and (AllocationQty $r R2) -eq 80 -and $r.totalVendorsUsed -eq 1) 'Expected M unallocated and N fully allocated'
    }
    RunCase 'M/N neither material has sufficient stock' $dualDemands @((DualVendor A 60 30),(DualVendor B 60 20)) {param($r)
        Check ((AllocationQty $r R1) -eq 0 -and (AllocationQty $r R2 A) -eq 30 -and (AllocationQty $r R2 B) -eq 20) 'Expected M=0 and N=50'
    }
    $results | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $PSScriptRoot 'mip-results.json') -Encoding utf8
    $report = @('# MIP test results', '', "Executed: $(Get-Date -Format o)", '', 'Production endpoint and models executed verbatim in an isolated HTTP host; RabbitMQ startup excluded.', '', '| Test | Result | Details |', '| --- | --- | --- |')
    foreach($r in $results){$report += "| $($r.name) | $($r.status) | $($r.error) |"}
    $report += ''; $report += "Passed: $(@($results|Where-Object status -eq PASS).Count) / $($results.Count)"
    foreach($r in $results) {
        $report += ''; $report += "## $($r.name)"
        $report += ''; $report += "Result: $($r.status). Total allocated: $($r.response.totalQuantityAllocated). Vendors: $($r.response.totalVendorsUsed)."
        $report += ''; $report += '| Demand | Material | Requested | Single vendor |'; $report += '| --- | --- | --- | --- |'
        foreach($d in $r.request.reservations){$report += "| $($d.resKey) | $($d.matNo) | $($d.quantity) | $($d.requireSingleVendor) |"}
        $report += ''; $report += '| Vendor | Material | Stock |'; $report += '| --- | --- | --- |'
        foreach($v in $r.request.vendors){foreach($stock in $v.stocks){$report += "| $($v.vendorKey) | $($stock.matNo) | $($stock.quantity) |"}}
        $report += ''; $report += '| Demand | Material | Vendor | Allocated |'; $report += '| --- | --- | --- | --- |'
        foreach($a in $r.response.allocations){$report += "| $($a.resKey) | $($a.matNo) | $($a.vendorKey) | $($a.allocatedQuantity) |"}
    }
    $report | Set-Content (Join-Path $PSScriptRoot 'mip-results.md') -Encoding utf8
    $results | Select-Object name,status,error | Format-Table -AutoSize
    if(@($results|Where-Object status -eq FAIL).Count){throw 'MIP tests failed; see report'}
} finally { if(!$process.HasExited){Stop-Process -Id $process.Id} }
