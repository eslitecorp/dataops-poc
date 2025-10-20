# syntax=docker/dockerfile:1.7

########################
# 1) 基底執行階段（瘦身 + 非 root + 健康檢查）
########################
#FROM mcr.microsoft.com/dotnet/aspnet:8.0-bookworm-slim AS base
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_EnableDiagnostics=0 \
    COMPlus_ReadyToRun=0
WORKDIR /app
EXPOSE 8080

# （可選）若你的服務有 /health 或 /alive
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/health || exit 1

# 建立非 root 帳號
ARG APP_UID=10001
RUN useradd -m -u ${APP_UID} appuser
USER appuser

########################
# 2) 建置階段（最佳化快取）
########################
#FROM mcr.microsoft.com/dotnet/sdk:8.0-bookworm AS build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# 先複製專案檔，最大化 restore 快取命中率
# 若有多專案或中央套件管理，請一併 COPY Directory.Packages.props、NuGet.Config、Solution.sln
COPY src/MyApp/*.csproj src/MyApp/
#COPY NuGet.Config ./
RUN dotnet restore src/MyApp/MyApp.csproj --locked-mode

# 再複製其餘原始碼
COPY . .
WORKDIR /src/src/MyApp

# 可重現建置（CI-friendly）
RUN dotnet build MyApp.csproj -c Release -o /app/build \
    -p:ContinuousIntegrationBuild=true

########################
# 3) 發佈階段（ReadyToRun 等優化）
########################
FROM build AS publish
RUN dotnet publish MyApp.csproj -c Release -o /app/publish \
    -p:PublishReadyToRun=true \
    -p:PublishSingleFile=false \
    -p:UseAppHost=false

########################
# 4) 最終映像
########################
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish ./
ENTRYPOINT ["dotnet", "MyApp.dll"]
