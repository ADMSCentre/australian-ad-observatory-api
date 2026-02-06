# PowerShell script to build and deploy AAO Enrichment Ingestion service

$version = Get-Content -Path "version" -Raw
$version = $version.Trim()
$registry = "registry.rc.nectar.org.au/aio2024-private"
$imageName = "aao-enrichment-ingestion"

Write-Host "Building and deploying $imageName version $version"

# Build the image
docker build . -t "${imageName}:${version}"

# Tag the images
docker tag "${imageName}:${version}" "${registry}/${imageName}:${version}"
docker tag "${imageName}:${version}" "${registry}/${imageName}:latest"

# Push the images
docker push "${registry}/${imageName}:${version}"
docker push "${registry}/${imageName}:latest"

Write-Host "Successfully pushed ${imageName} version ${version}"