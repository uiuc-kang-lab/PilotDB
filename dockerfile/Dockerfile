from ubuntu:22.04

COPY --from=pilotdb . /home

# install mssql tools
RUN apt-get update \
    && apt-get install -y curl \
    && curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev

# install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && . $HOME/.local/bin/env

# install pilotdb
RUN cd /home && \
    $HOME/.local/bin/uv sync && \
    $HOME/.local/bin/uv pip install -e .
    
ENTRYPOINT ["bash", "/home/dockerfile/start_pilotdb_server.sh"]