# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.245.0/containers/python-3/.devcontainer/base.Dockerfile

ARG VARIANT=ubuntu-24.04
FROM mcr.microsoft.com/vscode/devcontainers/base:${VARIANT}

# nco provides the ncks/ncrcat tools the partitioner shells out to (as in CI
# and the production image); build-essential for any wheels built from source.
RUN apt-get update \
    && apt-get -y install --no-install-recommends nco build-essential \
    && rm -rf /var/lib/apt/lists/*

# Keep the inner (docker-in-docker) daemon off 172.17.0.0/16. Its default
# docker0 would otherwise collide with the outer bridge, making the outer
# gateway/DNS (172.17.0.1) look local and breaking name resolution.
RUN mkdir -p /etc/docker && printf '%s\n' \
    '{' \
    '  "bip": "10.200.0.1/24",' \
    '  "default-address-pools": [{ "base": "10.201.0.0/16", "size": 24 }]' \
    '}' > /etc/docker/daemon.json
