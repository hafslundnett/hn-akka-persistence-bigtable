FROM google/cloud-sdk:slim

# Install .NET CLI dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libc6 \
        libcurl3 \
        libgcc1 \
        libgssapi-krb5-2 \
        libicu52 \
        liblttng-ust0 \
        libssl1.0.0 \
        libstdc++6 \
        libunwind8 \
        libuuid1 \
        zlib1g \
        ca-certificates \
        curl \
        gettext \
        nuget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# .NET Core
ENV DOTNET_SDK_VERSION 2.1.400

RUN curl -SL --output dotnet.tar.gz https://dotnetcli.blob.core.windows.net/dotnet/Sdk/$DOTNET_SDK_VERSION/dotnet-sdk-$DOTNET_SDK_VERSION-linux-x64.tar.gz \
    && dotnet_sha512='9c850fe9d1ac335376fb3ed1f07472d281a9c4ac8ed02cf752ce36c3488d6746c09d6e2101184f3c8de357aae86b567196f4d071b6c55505a250ce6db53ab532' \
    && sha512sum dotnet.tar.gz \
    && echo "$dotnet_sha512 dotnet.tar.gz" | sha512sum -c - \
    && mkdir -p /usr/share/dotnet \
    && tar -zxf dotnet.tar.gz -C /usr/share/dotnet \
    && rm dotnet.tar.gz \
    && ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

# Trigger the population of the local package cache
ENV NUGET_XMLDOC_MODE skip
RUN mkdir warmup \
    && cd warmup \
    && dotnet new \
    && cd .. \
    && rm -rf warmup \
    && rm -rf /tmp/NuGetScratch

WORKDIR /app
COPY . .

RUN dotnet build

# Run tests
ENV SA_KEY=""
ENV INTEGRATION_TEST_TIME_FACTOR="1"
ENV INTEGRATION_TEST_JOURNAL_TABLE=""
ENV INTEGRATION_TEST_SNAPSHOT_STORE_TABLE=""
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/_key.json
ENTRYPOINT echo $SA_KEY | base64 -d > _key.json \
    && gcloud auth activate-service-account --key-file=_key.json \
    && dotnet test test/Hafslund.Akka.Persistence.Bigtable.IntegrationTests/Hafslund.Akka.Persistence.Bigtable.IntegrationTests.csproj