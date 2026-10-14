$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    dotnet build -c Release
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    dotnet run --project tests/EventTimeWatermark.Tests -c Release --no-build
    if ($LASTEXITCODE -ne 0) { throw 'Assertions failed' }
    $cli = 'src/EventTimeWatermark.Cli/bin/Release/net10.0/EventTimeWatermark.Cli.dll'
    foreach ($scenario in @('ordered', 'boundary', 'skew', 'idle-resume', 'correction', 'mixed')) {
        foreach ($policy in @('discard', 'side-output', 'correct')) {
            dotnet $cli compare --scenario $scenario --late-policy $policy --output "artifacts/verify/$scenario-$policy" | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "CLI failed: $scenario $policy" }
        }
    }
    dotnet $cli sweep --output artifacts/verify/sweep | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Sweep failed' }
    if (@(Import-Csv artifacts/verify/sweep/sweep.csv).Count -ne 12) { throw 'Unexpected sweep row count' }
    $invalidCases = @(
        @('compare', '--idle-timeout', '0s'),
        @('compare', '--watermark-delay', '-1s'),
        @('compare', '--scenario', 'unknown'),
        @('compare', '--seed', 'oops'),
        @('compare', '--unknown', 'x'),
        @('compare', '--scenario'),
        @('compare', '--watermark-delay', '0.0001s')
    )
    foreach ($invalid in $invalidCases) {
        # Expected diagnostics are preserved in an artifact, not hidden by the test.
        & dotnet $cli @invalid 2> artifacts/verify/last-expected-error.txt | Out-Null
        if ($LASTEXITCODE -eq 0) { throw "Invalid arguments accepted: $invalid" }
    }
    Write-Output 'CLI smoke checks passed: 18 scenario/policy runs, 12 sweep rows, 7 invalid argument cases.'
}
finally { Pop-Location }
