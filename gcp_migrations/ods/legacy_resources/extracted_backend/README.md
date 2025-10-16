## Firstly, install dotnet for C# projects
``` bash
$ sudo snap install dotnet --classic
$ dotnet --version
$ dotnet new console -o dataops-poc/gcp_migrations/ods/legacy_resources/extracted_backend
```

``` bash
### Remeber to add .gitignore for C#

# =========================
# .NET / C# / Visual Studio Code
# =========================

## Build folders
bin/
obj/
[Bb]uild/
[Ll]ogs/
[Tt]est[Rr]esults/

## VS Code folders
.vscode/
.vs/
.idea/

## User-specific files
*.user
*.suo
*.userosscache
*.sln.docstates

## .NET Core
project.lock.json
project.fragment.lock.json
artifacts/

## NuGet packages
*.nupkg
*.snupkg
*.nuspec
**/packages/*
!**/packages/build/
!**/packages/repositories.config
.nuget/

## Logs and temp files
*.log
*.tlog
*.tmp
*.temp
*.pidb

## Generated files
*.dll
*.exe
*.pdb
*.cache
*.config
*.dbmdl
*.bak
*.FileListAbsolute.txt

## Rider / JetBrains
.idea/
*.sln.iml

## OS files
.DS_Store
Thumbs.db
ehthumbs.db
Icon?
desktop.ini

## Test coverage
*.coverage
*.coveragexml
TestResult.xml
*.trx
coverage.cobertura.xml

## Others
_ReSharper*/
*.resharper
*.DotSettings.user
```