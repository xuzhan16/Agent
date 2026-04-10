param(
    [switch]$ForceReimport,
    [string]$EnvFile = '.env.neo4j'
)

$ErrorActionPreference = 'Stop'

function Write-Step {
    param([string]$Message)
    Write-Host "[neo4j-setup] $Message"
}

function Load-EnvFile {
    param([string]$Path)
    $result = @{}
    foreach ($line in Get-Content -Path $Path -Encoding UTF8) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) {
            continue
        }
        $parts = $trimmed.Split('=', 2)
        if ($parts.Count -ne 2) {
            continue
        }
        $result[$parts[0].Trim()] = $parts[1].Trim()
    }
    return $result
}

function New-RandomPassword {
    $token = [Guid]::NewGuid().ToString('N').Substring(0, 12)
    return "Neo4j!$token"
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent (Split-Path -Parent $scriptDir)
$composeFile = Join-Path $repoRoot 'docker-compose.neo4j.yml'
$envPath = Join-Path $repoRoot $EnvFile
$envExamplePath = Join-Path $repoRoot '.env.neo4j.example'
$csvDir = Join-Path $repoRoot 'outputs\neo4j'
$dataDir = Join-Path $repoRoot '.infra\neo4j\data'
$logsDir = Join-Path $repoRoot '.infra\neo4j\logs'
$pluginsDir = Join-Path $repoRoot '.infra\neo4j\plugins'
$confDir = Join-Path $repoRoot '.infra\neo4j\conf'

$requiredFiles = @(
    'jobs.csv',
    'skills.csv',
    'degrees.csv',
    'majors.csv',
    'industries.csv',
    'rel_requires_skill.csv',
    'rel_requires_degree.csv',
    'rel_prefers_major.csv',
    'rel_belongs_industry.csv',
    'rel_promote_to.csv',
    'rel_transfer_to.csv'
)

foreach ($dir in @($dataDir, $logsDir, $pluginsDir, $confDir)) {
    New-Item -ItemType Directory -Force $dir | Out-Null
}

foreach ($file in $requiredFiles) {
    $fullPath = Join-Path $csvDir $file
    if (-not (Test-Path $fullPath)) {
        throw "Missing Neo4j CSV file: $fullPath"
    }
}

if (-not (Test-Path $envPath)) {
    if (-not (Test-Path $envExamplePath)) {
        throw "Missing env template: $envExamplePath"
    }
    Copy-Item $envExamplePath $envPath
    $envText = [System.IO.File]::ReadAllText($envPath)
    $generatedPassword = New-RandomPassword
    $envText = $envText.Replace('ChangeThisPassword_123!', $generatedPassword)
    [System.IO.File]::WriteAllText($envPath, $envText, [System.Text.UTF8Encoding]::new($false))
    Write-Step "Created $EnvFile with generated password: $generatedPassword"
}

$envMap = Load-EnvFile -Path $envPath
$image = if ($envMap.ContainsKey('NEO4J_IMAGE')) { $envMap['NEO4J_IMAGE'] } else { 'neo4j:5' }
$containerName = if ($envMap.ContainsKey('NEO4J_CONTAINER_NAME')) { $envMap['NEO4J_CONTAINER_NAME'] } else { 'agent-neo4j' }
$username = if ($envMap.ContainsKey('NEO4J_USERNAME')) { $envMap['NEO4J_USERNAME'] } else { 'neo4j' }
$password = if ($envMap.ContainsKey('NEO4J_PASSWORD')) { $envMap['NEO4J_PASSWORD'] } else { throw 'NEO4J_PASSWORD missing in env file.' }
$httpPort = if ($envMap.ContainsKey('NEO4J_HTTP_PORT')) { $envMap['NEO4J_HTTP_PORT'] } else { '7474' }
$boltPort = if ($envMap.ContainsKey('NEO4J_BOLT_PORT')) { $envMap['NEO4J_BOLT_PORT'] } else { '7687' }

$hasExistingData = @(Get-ChildItem -Path $dataDir -Force -ErrorAction SilentlyContinue).Count -gt 0

Write-Step 'Stopping existing Neo4j container if present'
& docker compose --env-file $envPath -f $composeFile down | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw 'docker compose down failed.'
}

if ($ForceReimport -and $hasExistingData) {
    Write-Step 'Force reimport enabled, clearing existing local Neo4j data'
    Get-ChildItem -Path $dataDir -Force | Remove-Item -Recurse -Force
    $hasExistingData = $false
}

if (-not $hasExistingData) {
    Write-Step 'Importing CSV files into a fresh Neo4j database'
    $dockerArgs = @(
        'run', '--rm',
        '--mount', "type=bind,source=$dataDir,target=/data",
        '--mount', "type=bind,source=$csvDir,target=/import",
        $image,
        'neo4j-admin', 'database', 'import', 'full', 'neo4j',
        '--nodes=/import/jobs.csv',
        '--nodes=/import/skills.csv',
        '--nodes=/import/degrees.csv',
        '--nodes=/import/majors.csv',
        '--nodes=/import/industries.csv',
        '--relationships=/import/rel_requires_skill.csv',
        '--relationships=/import/rel_requires_degree.csv',
        '--relationships=/import/rel_prefers_major.csv',
        '--relationships=/import/rel_belongs_industry.csv',
        '--relationships=/import/rel_promote_to.csv',
        '--relationships=/import/rel_transfer_to.csv'
    )
    & docker @dockerArgs | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'neo4j-admin import failed.'
    }
}
else {
    Write-Step 'Existing local Neo4j data detected, skip reimport. Use -ForceReimport to rebuild from CSV.'
}

Write-Step 'Starting Neo4j container'
& docker compose --env-file $envPath -f $composeFile up -d | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw 'docker compose up failed.'
}

Write-Step 'Waiting for Neo4j to become ready'
$deadline = (Get-Date).AddMinutes(2)
$ready = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker exec $containerName cypher-shell -u $username -p $password 'RETURN 1 AS ok;' 1>$null 2>$null
    }
    catch {
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
}
if (-not $ready) {
    throw 'Neo4j did not become ready within 2 minutes.'
}

Write-Step 'Running a quick graph verification query'
& docker exec $containerName cypher-shell -u $username -p $password 'MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC;' | Out-Host

Write-Step "Neo4j Browser: http://localhost:$httpPort"
Write-Step "Bolt URI: bolt://localhost:$boltPort"
Write-Step "Username: $username"
Write-Step "Password: $password"