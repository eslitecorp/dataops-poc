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