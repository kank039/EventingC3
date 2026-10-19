# Build from the repository root:  docker build -f src/EventingC3.Etl.Worker/Dockerfile -t eventingc3-etl .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props ./
COPY deploy/clickhouse/ deploy/clickhouse/
COPY src/EventingC3.Etl.Core/EventingC3.Etl.Core.csproj src/EventingC3.Etl.Core/
COPY src/EventingC3.Etl.Infrastructure/EventingC3.Etl.Infrastructure.csproj src/EventingC3.Etl.Infrastructure/
COPY src/EventingC3.Etl.Worker/EventingC3.Etl.Worker.csproj src/EventingC3.Etl.Worker/
RUN dotnet restore src/EventingC3.Etl.Worker/EventingC3.Etl.Worker.csproj -r linux-x64
COPY src/ src/
RUN dotnet publish src/EventingC3.Etl.Worker/EventingC3.Etl.Worker.csproj -c Release -r linux-x64 --self-contained false --no-restore -o /app

# Full (non-chiseled) runtime: librdkafka's native library needs the distro's libc/zlib/ssl.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://0.0.0.0:8080 \
    DOTNET_gcServer=1 \
    DOTNET_GCHeapAffinitizeMask=0 \
    DOTNET_TieredPGO=1
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "EventingC3.Etl.Worker.dll"]
