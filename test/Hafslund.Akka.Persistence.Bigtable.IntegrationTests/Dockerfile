FROM microsoft/dotnet:2.0-sdk

WORKDIR /app
COPY . .

RUN dotnet build test/Hafslund.Akka.Persistence.Bigtable.IntegrationTests/Hafslund.Akka.Persistence.Bigtable.IntegrationTests.csproj

# Run tests
ENV SA_KEY=""
ENV INTEGRATION_TEST_TIME_FACTOR="1"
ENV INTEGRATION_TEST_JOURNAL_TABLE=""
ENV INTEGRATION_TEST_SNAPSHOT_STORE_TABLE=""
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/_key.json
ENTRYPOINT echo $SA_KEY | base64 -d > _key.json \
    && dotnet test test/Hafslund.Akka.Persistence.Bigtable.IntegrationTests/Hafslund.Akka.Persistence.Bigtable.IntegrationTests.csproj