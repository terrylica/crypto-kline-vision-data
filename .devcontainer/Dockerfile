FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.11
# RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install system dependencies
RUN apt-get update && apt-get install -y ripgrep fd-find

# Install Node.js for markdown linting
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g markdownlint-cli

# Install Python dependencies
RUN pip3 install apprise httpx fsspec aiohttp requests pendulum pandas tenacity colorlog psutil arrow pyarrow>=19.0.1

# Install Python dependencies for development
RUN pip3 install twine typer pytest pytest-asyncio pytest-cov pytest-xdist pytest-profiling pylint autoflake vulture

# Add .local/bin to PATH for the vscode user to remove WARNINGs
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc