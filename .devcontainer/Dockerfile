FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.11
# RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install system dependencies
RUN apt-get update && apt-get install -y ripgrep fd-find

# Install Node.js for markdown linting
# RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
#     apt-get install -y nodejs && \
#     npm install -g markdownlint-cli

# We will install dependencies after workspace is mounted
# The workspace will be mounted at /workspaces/binance-data-services
# and the dependencies will be installed from there

# Add .local/bin to PATH for the vscode user to remove WARNINGs
# This prevents warnings like: "The script X is installed in '/home/vscode/.local/bin' which is not on PATH"
# The PATH is also set in devcontainer.json using remoteEnv and in the postCreateCommand
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc