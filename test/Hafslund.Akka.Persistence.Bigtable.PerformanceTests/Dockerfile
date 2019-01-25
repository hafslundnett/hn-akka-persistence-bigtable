FROM microsoft/dotnet:2.0.0-sdk

WORKDIR /app
COPY . .

RUN dotnet build test/Hafslund.Akka.Persistence.Bigtable.PerformanceTests

# Run tests
ENTRYPOINT cd test/Hafslund.Akka.Persistence.Bigtable.PerformanceTests \
    && dotnet nbench