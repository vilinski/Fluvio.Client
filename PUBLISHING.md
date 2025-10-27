# Publishing Fluvio.Client to NuGet

This guide explains how to package and publish the Fluvio.Client library to NuGet.

## Prerequisites

1. **.NET 8.0 SDK** or later
2. **NuGet account** - Sign up at [nuget.org](https://www.nuget.org/)
3. **API Key** - Generate from your NuGet account settings

## Building the Package

### 1. Update Version Numbers

Update the version in the project files:

**src/Fluvio.Client/Fluvio.Client.csproj:**
```xml
<Version>0.1.0</Version>
```

**src/Fluvio.Client.Abstractions/Fluvio.Client.Abstractions.csproj:**
```xml
<Version>0.1.0</Version>
```

### 2. Update Package Metadata

Edit the `.csproj` files to update:
- `Authors` - Your name
- `Company` - Your company name
- `RepositoryUrl` - Your GitHub repository URL
- `PackageProjectUrl` - Your project website

### 3. Build in Release Mode

```bash
dotnet build -c Release
```

### 4. Create NuGet Packages

```bash
# Package the abstractions library
dotnet pack src/Fluvio.Client.Abstractions/Fluvio.Client.Abstractions.csproj -c Release -o ./packages

# Package the main library
dotnet pack src/Fluvio.Client/Fluvio.Client.csproj -c Release -o ./packages
```

This will create `.nupkg` files in the `./packages` directory.

## Testing the Package Locally

Before publishing, test the package locally:

```bash
# Create a test project
mkdir test-consumer
cd test-consumer
dotnet new console

# Add your local package
dotnet add package Fluvio.Client --source /path/to/Fluvio.Client3/packages

# Test your code
# ... (write test code)

# Run
dotnet run
```

## Publishing to NuGet

### 1. Get Your API Key

1. Go to [nuget.org](https://www.nuget.org/)
2. Sign in to your account
3. Go to **API Keys** in your account settings
4. Create a new API key with "Push" permissions
5. Copy the key (you won't be able to see it again)

### 2. Publish the Packages

```bash
# Set your API key (do this once)
export NUGET_API_KEY="your-api-key-here"

# Publish the abstractions package
dotnet nuget push packages/Fluvio.Client.Abstractions.0.1.0.nupkg \
    --api-key $NUGET_API_KEY \
    --source https://api.nuget.org/v3/index.json

# Publish the main package
dotnet nuget push packages/Fluvio.Client.0.1.0.nupkg \
    --api-key $NUGET_API_KEY \
    --source https://api.nuget.org/v3/index.json
```

### 3. Verify Publication

After publishing, it may take a few minutes for the package to appear:

1. Go to [nuget.org](https://www.nuget.org/)
2. Search for "Fluvio.Client"
3. Verify the package details and version

## Publishing Pre-release Versions

For beta or preview versions, use semantic versioning with pre-release tags:

```xml
<Version>0.1.0-beta</Version>
```

or

```xml
<Version>0.1.0-preview.1</Version>
```

Then publish as normal. Users can install pre-release versions with:

```bash
dotnet add package Fluvio.Client --version 0.1.0-beta
```

## Automated Publishing with GitHub Actions

You can automate publishing with GitHub Actions:

**.github/workflows/publish.yml:**
```yaml
name: Publish NuGet Package

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Setup .NET
      uses: actions/setup-dotnet@v3
      with:
        dotnet-version: '8.0.x'

    - name: Restore dependencies
      run: dotnet restore

    - name: Build
      run: dotnet build -c Release --no-restore

    - name: Test
      run: dotnet test -c Release --no-build --verbosity normal

    - name: Pack
      run: |
        dotnet pack src/Fluvio.Client.Abstractions/Fluvio.Client.Abstractions.csproj -c Release -o ./packages
        dotnet pack src/Fluvio.Client/Fluvio.Client.csproj -c Release -o ./packages

    - name: Publish to NuGet
      run: |
        dotnet nuget push packages/Fluvio.Client.Abstractions.*.nupkg \
          --api-key ${{ secrets.NUGET_API_KEY }} \
          --source https://api.nuget.org/v3/index.json \
          --skip-duplicate
        dotnet nuget push packages/Fluvio.Client.*.nupkg \
          --api-key ${{ secrets.NUGET_API_KEY }} \
          --source https://api.nuget.org/v3/index.json \
          --skip-duplicate
```

Add your NuGet API key as a secret in GitHub:
1. Go to your repository settings
2. Navigate to **Secrets and variables** → **Actions**
3. Add a new secret named `NUGET_API_KEY`

## Version Management

Follow [Semantic Versioning](https://semver.org/):

- **MAJOR** version (X.0.0) - Breaking changes
- **MINOR** version (0.X.0) - New features, backwards compatible
- **PATCH** version (0.0.X) - Bug fixes, backwards compatible

## Package Icon

To add a package icon:

1. Add an icon file (128x128 PNG) to your project
2. Update the `.csproj` file:

```xml
<PropertyGroup>
  <PackageIcon>icon.png</PackageIcon>
</PropertyGroup>

<ItemGroup>
  <None Include="icon.png" Pack="true" PackagePath="\" />
</ItemGroup>
```

## Changelog

Maintain a CHANGELOG.md file to track changes between versions:

```markdown
# Changelog

## [0.1.0] - 2025-01-27

### Added
- Initial release
- Producer API for sending messages
- Consumer API for streaming messages
- Admin API for topic management
- TLS support
- Binary protocol implementation
```

## Unlisting Packages

If you need to unlist a published package:

1. Go to [nuget.org](https://www.nuget.org/)
2. Sign in to your account
3. Go to **Manage Packages**
4. Select the package and version
5. Click **Unlist**

Note: Unlisted packages can still be installed if the exact version is specified.

## Best Practices

1. **Test thoroughly** before publishing
2. **Version carefully** - you can't delete or modify published versions
3. **Document breaking changes** in release notes
4. **Support multiple .NET versions** when possible (e.g., netstandard2.0, net6.0, net8.0)
5. **Include XML documentation** for IntelliSense
6. **Add package dependencies** carefully
7. **Keep dependencies up to date**

## Troubleshooting

### Package Not Appearing

Packages can take a few minutes to appear after publishing. Check:
- The package validation status on nuget.org
- Your search filters (pre-release, etc.)

### Push Failed

Common issues:
- Invalid API key
- Package ID already exists (claimed by another user)
- Version already published
- Package size too large (>250 MB)

### Symbol Packages

To include debug symbols:

```bash
dotnet pack -c Release --include-symbols --include-source
```

## Resources

- [NuGet Documentation](https://docs.microsoft.com/en-us/nuget/)
- [Creating NuGet Packages](https://docs.microsoft.com/en-us/nuget/create-packages/creating-a-package)
- [Publishing NuGet Packages](https://docs.microsoft.com/en-us/nuget/nuget-org/publish-a-package)
- [Semantic Versioning](https://semver.org/)
