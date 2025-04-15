FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.10
# RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install system dependencies
RUN apt-get update && apt-get install -y ripgrep fd-find

# Install Python dependencies
RUN pip3 install curl_cffi httpx pendulum pandas tenacity colorlog psutil arrow pyarrow>=19.0.1

# Install Python dependencies for development
RUN pip3 install twine typer pytest pytest-asyncio pytest-cov pytest-xdist pytest-profiling pylint autoflake vulture

# Add .local/bin to PATH for the vscode user to remove WARNINGs
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc