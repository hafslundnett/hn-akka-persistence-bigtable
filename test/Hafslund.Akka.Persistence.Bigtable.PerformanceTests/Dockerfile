FROM microsoft/dotnet:2.0.0-sdk

WORKDIR /app
COPY . .

WORKDIR /app
COPY . .

RUN dotnet build test/Hafslund.Akka.Persistence.Bigtable.PerformanceTests

# Run tests
ENV SA_KEY=""
ENV PERFORMANCE_TEST_JOURNAL_TABLE=""
ENV PERFORMANCE_TEST_SNAPSHOT_STORE_TABLE=""
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/_key.json
ENTRYPOINT echo $SA_KEY | base64 -d > _key.json \
    && cd test/Hafslund.Akka.Persistence.Bigtable.PerformanceTests \
    && dotnet nbench