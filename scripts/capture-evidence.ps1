param([string]$Namespace = 'fraud-scale', [string]$OutputDirectory = 'docs/evidence')
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
# Store actual command output; never generate successful-looking sample logs.
$checks = @{
    'pods.txt' = @('get', 'pods', '-o', 'wide')
    'workloads.txt' = @('get', 'deployment,statefulset,hpa,pvc')
    'processor.txt' = @('logs', '-l', 'app=processor', '-c', 'processor', '--tail=20', '--prefix=true')
    'brokers.txt' = @('exec', 'kafka-0', '--', 'rpk', 'cluster', 'info', '-X', 'brokers=kafka:9092')
    'partitions.txt' = @('exec', 'kafka-0', '--', 'rpk', 'topic', 'describe', 'transactions', '-X', 'brokers=kafka:9092')
}
foreach ($name in $checks.Keys) {
    $arguments = @('-n', $Namespace) + $checks[$name]
    $result = & kubectl @arguments 2>&1
    if ($LASTEXITCODE -ne 0) { throw "kubectl failed for $name`: $result" }
    $result | Set-Content -Encoding UTF8 -LiteralPath (Join-Path $OutputDirectory $name)
}
$apiCode = "import json,urllib.request; print(json.dumps({p:json.load(urllib.request.urlopen('http://serving:8000/'+p)) for p in ['summary','stats','velocity']},indent=2))"
$apiOutput = & kubectl -n $Namespace exec deployment/serving -- python -c $apiCode 2>&1
if ($LASTEXITCODE -ne 0) { throw "API evidence failed: $apiOutput" }
$apiOutput | Set-Content -Encoding UTF8 -LiteralPath (Join-Path $OutputDirectory 'api.json')
Write-Output 'Captured real outputs. Add screenshots from the running UI and kubectl, then embed them in README.'
