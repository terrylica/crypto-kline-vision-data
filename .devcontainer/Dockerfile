FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.10
# RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install and upgrade pip
RUN pip3 install --upgrade pip # Upgrade pip to the latest version for better package management and security

# Install Node.js tools
RUN npm install -g npm@latest # Install the latest version of npm for access to the newest features and bug fixes
RUN npm install -g markdownlint-cli2

# Install system dependencies
RUN apt-get update && apt-get install -y ripgrep fd-find

# We will install dependencies after workspace is mounted
# The workspace will be mounted at /workspaces/raw-data-services
# and the dependencies will be installed from there

# Add .local/bin to PATH for the vscode user to remove WARNINGs
# This prevents warnings like: "The script X is installed in '/home/vscode/.local/bin' which is not on PATH"
# The PATH is also set in devcontainer.json using remoteEnv and in the postCreateCommand
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc