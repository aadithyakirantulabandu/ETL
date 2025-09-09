## generator.ps1
# Requires: Java + Synthea JAR
param(
  [string]$JarPath = "C:\Users\aadit\Downloads\synthea-with-dependencies.jar",
  [int]$BatchSize  = 200,
  [int]$SleepSec   = 20,
  [int]$Seed       = 7000,
  [string]$State   = "Pennsylvania",
  [string]$City    = "Philadelphia"
)

# Strict/quiet
$ErrorActionPreference = "Stop"
$ProgressPreference    = "SilentlyContinue"

# Resolve project paths
$ProjectRoot = (Get-Location).Path
$StreamRoot  = Join-Path $ProjectRoot "synthea_stream"
$IncomingDir = Join-Path $ProjectRoot "incoming"

# Preconditions
if (!(Test-Path $JarPath)) { Write-Error "Synthea JAR not found at: $JarPath"; exit 1 }
New-Item -ItemType Directory -Force -Path $StreamRoot  | Out-Null
New-Item -ItemType Directory -Force -Path $IncomingDir | Out-Null

# Activate venv once
$VenvAct = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"
if (!(Test-Path $VenvAct)) { Write-Error "vEnv activation not found at: $VenvAct"; exit 1 }
. $VenvAct

Write-Host "✅ JAR: $JarPath"
Write-Host "✅ Stream: $StreamRoot"
Write-Host "✅ Incoming: $IncomingDir"
Write-Host "✅ Starting loop… (Ctrl+C to stop)"

while ($true) {
  try {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $RunDir = Join-Path $StreamRoot "run_$stamp"
    Write-Host "`n[Synthea] Generating -> $RunDir"

    # Use baseDirectory (works across Synthea builds) and disable FHIR to avoid collisions
    java -jar $JarPath -s $Seed -p $BatchSize -a 18-85 $State $City `
      --exporter.csv.export true `
      --exporter.fhir.export false `
      --exporter.baseDirectory "$RunDir"

    $Seed++

    # Find CSV folder (support both layouts)
    $CsvDir = $null
    $Candidates = @(
      (Join-Path $RunDir "output\csv"),
      (Join-Path $RunDir "csv")
    )
    foreach ($c in $Candidates) { if (Test-Path $c) { $CsvDir = $c; break } }

    if (-not $CsvDir) {
      $listing = (Get-ChildItem $RunDir -Force | Select-Object Name,Length,Mode | Format-Table | Out-String)
      throw "CSV directory not found. Tried:`n  $($Candidates -join "`n  ")`nRunDir contents:`n$listing"
    }

    Write-Host "[Injector] Input: $CsvDir  →  Incoming: $IncomingDir"
    python -m app.noise_injector "$CsvDir" "$IncomingDir"

    Write-Host "[Mapper] Building standardized events_* from patients/observations"
    python -m app.mapper_synthea_events "$CsvDir" "$IncomingDir"

    Write-Host "[Sleep] Waiting $SleepSec sec…"
    Start-Sleep -Seconds $SleepSec
  }
  catch {
    Write-Host "❌ Error: $($_.Exception.Message)"
    # brief pause then continue loop
    Start-Sleep -Seconds 5
  }
}
