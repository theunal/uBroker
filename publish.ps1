<#
.SYNOPSIS
    uBroker NuGet paketleme ve yayınlama scripti.

.DESCRIPTION
    Tüm uBroker source projelerini Release modda derler, paketler ve NuGet.org'a yükler.

.PARAMETER Version
    Paket versiyonu (örn: 1.1.0). Zorunlu.

.PARAMETER ApiKey
    NuGet.org API key. Verilmezse sadece pack yapılır, push yapılmaz.

.PARAMETER Source
    NuGet source URL. Varsayılan: https://api.nuget.org/v3/index.json

.PARAMETER PackOnly
    Sadece pack yap, push yapma.

.PARAMETER SkipBuild
    Derlemeyi atla (zaten Release olarak derlendiyse).

.EXAMPLE
    .\publish.ps1 -Version 1.1.0 -PackOnly
    .\publish.ps1 -Version 1.1.0 -ApiKey "nuget-key buraya"
    .\publish.ps1 -Version 1.1.0 -ApiKey "nuget-key buraya" -Source "https://nuget.org/api/v2/package"
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$Version,

    [string]$ApiKey,

    [string]$Source = "https://api.nuget.org/v3/index.json",

    [switch]$PackOnly,

    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

# ── Source projeler (test/benchmark/sample değil) ──

$SourceProjects = @(
    "src\uBroker.Core\uBroker.Core.csproj",
    "src\uBroker.RabbitMQ\uBroker.RabbitMQ.csproj",
    "src\uBroker.Kafka\uBroker.Kafka.csproj",
    "src\uBroker.Azure\uBroker.Azure.csproj",
    "src\uBroker.Aws\uBroker.Aws.csproj",
    "src\uBroker.Diagnostics\uBroker.Diagnostics.csproj",
    "src\uBroker.DependencyInjection\uBroker.DependencyInjection.csproj"
)

$OutputDir = "artifacts\nupkg"
$SolutionFile = "uBroker.slnx"

# ── Helper functions ──

function Write-Step($msg) {
    Write-Host "`n=== $msg ===" -ForegroundColor Cyan
}

function Write-Ok($msg) {
    Write-Host "  [OK] $msg" -ForegroundColor Green
}

function Write-Fail($msg) {
    Write-Host "  [FAIL] $msg" -ForegroundColor Red
}

# ── Pre-flight checks ──

Write-Host "uBroker NuGet Publish" -ForegroundColor Yellow
Write-Host "Version: $Version" -ForegroundColor Yellow

if (-not $PackOnly -and -not $ApiKey) {
    Write-Host "`nWARNING: No ApiKey provided. Only packing (no push)." -ForegroundColor Yellow
    $PackOnly = $true
}

# Validate version format
if ($Version -notmatch '^\d+\.\d+\.\d+(-[a-zA-Z0-9]+)?(\.[a-zA-Z0-9]+)?$') {
    Write-Fail "Invalid version format: $Version (expected: 1.0.0 or 1.0.0-beta.1)"
    exit 1
}

# Check dotnet SDK
$dotnetVersion = & dotnet --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Fail "dotnet SDK not found"
    exit 1
}
Write-Ok "dotnet SDK: $dotnetVersion"

# ── Step 1: Clean ──

Write-Step "1/5 Cleaning artifacts"

if (Test-Path $OutputDir) {
    Remove-Item -Recurse -Force $OutputDir
}
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
Write-Ok "Cleaned $OutputDir"

# ── Step 2: Build ──

if (-not $SkipBuild) {
    Write-Step "2/5 Building solution (Release)"

    dotnet build $SolutionFile -c Release --verbosity quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Fail "Build failed"
        exit 1
    }
    Write-Ok "Build succeeded"
} else {
    Write-Step "2/5 Build skipped (--SkipBuild)"
}

# ── Step 3: Pack ──

Write-Step "3/5 Packing NuGet packages"

foreach ($proj in $SourceProjects) {
    $projPath = Join-Path $PSScriptRoot $proj
    if (-not (Test-Path $projPath)) {
        Write-Fail "Project not found: $proj"
        exit 1
    }

    $projName = [System.IO.Path]::GetFileNameWithoutExtension($proj)

    Write-Host "  Packing $projName..." -NoNewline
    dotnet pack $projPath -c Release -o $OutputDir -p:PackageVersion=$Version --verbosity quiet --no-build
    if ($LASTEXITCODE -ne 0) {
        Write-Host " FAILED" -ForegroundColor Red
        exit 1
    }
    Write-Host " OK" -ForegroundColor Green
}

$packages = Get-ChildItem -Path $OutputDir -Filter "*.nupkg"
Write-Ok "Packed $($packages.Count) packages:"
foreach ($pkg in $packages) {
    Write-Host "    $($pkg.Name) ($([math]::Round($pkg.Length / 1KB, 1)) KB)"
}

# ── Step 4: Verify ──

Write-Step "4/5 Verifying packages"

$expectedPackages = @(
    "uBroker.Core.$Version.nupkg",
    "uBroker.RabbitMQ.$Version.nupkg",
    "uBroker.Kafka.$Version.nupkg",
    "uBroker.Azure.$Version.nupkg",
    "uBroker.Aws.$Version.nupkg",
    "uBroker.Diagnostics.$Version.nupkg",
    "uBroker.DependencyInjection.$Version.nupkg"
)

$allFound = $true
foreach ($expected in $expectedPackages) {
    $found = $packages | Where-Object { $_.Name -eq $expected }
    if ($found) {
        Write-Ok $expected
    } else {
        Write-Fail "Missing: $expected"
        $allFound = $false
    }
}

if (-not $allFound) {
    Write-Fail "Package verification failed"
    exit 1
}

# ── Step 5: Push ──

if ($PackOnly) {
    Write-Step "5/5 Push skipped (--PackOnly or no ApiKey)"
    Write-Host "`nPackages saved to: $OutputDir" -ForegroundColor Yellow
    Write-Host "`nTo publish manually:" -ForegroundColor Yellow
    Write-Host "  dotnet nuget push `"$OutputDir\*.nupkg`" --api-key YOUR_KEY --source $Source" -ForegroundColor Gray
} else {
    Write-Step "5/5 Pushing to NuGet.org"

    foreach ($pkg in $packages) {
        Write-Host "  Pushing $($pkg.Name)..." -NoNewline
        dotnet nuget push $pkg.FullName --api-key $ApiKey --source $Source --skip-duplicate
        if ($LASTEXITCODE -ne 0) {
            Write-Host " FAILED" -ForegroundColor Red
            Write-Fail "Push failed for $($pkg.Name)"
            exit 1
        }
        Write-Host " OK" -ForegroundColor Green
    }

    Write-Host "`nAll packages published!" -ForegroundColor Green
    Write-Host "https://www.nuget.org/packages?q=ubroker" -ForegroundColor Cyan
}

# ── Summary ──

Write-Host "`n" -NoNewline
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " uBroker v$Version - Publish Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Packages: $($packages.Count)"
Write-Host " Output:   $OutputDir"
Write-Host " Version:  $Version"
