FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.13
# RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install system dependencies
RUN apt-get update && apt-get install -y ripgrep

# Install Python dependencies
RUN pip3 install pandas curl_cffi tenacity colorlog psutil arrow pyarrow>=19.0.1 filelock

# Install Python dependencies for development
RUN pip3 install twine pytest pytest-asyncio pytest-cov pytest-xdist pytest-profiling pylint autoflake

# Add .local/bin to PATH for the vscode user to remove WARNINGs
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc