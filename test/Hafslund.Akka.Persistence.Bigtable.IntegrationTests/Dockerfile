FROM microsoft/dotnet:2.0-sdk

WORKDIR /app
COPY . .

RUN dotnet build test/Hafslund.Akka.Persistence.Bigtable.IntegrationTests/Hafslund.Akka.Persistence.Bigtable.IntegrationTests.csproj

# Run tests
ENTRYPOINT dotnet test test/Hafslund.Akka.Persistence.Bigtable.IntegrationTests/Hafslund.Akka.Persistence.Bigtable.IntegrationTests.csproj