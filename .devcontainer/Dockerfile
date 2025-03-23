FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.10
RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install Python dependencies
RUN pip3 install pandas==2.2.1 numpy==1.24.4 numba==0.60.0 \
    matplotlib plotly dash dash-bootstrap-components talipp \
    joblib tenacity scipy colorlog aiohttp \
    psutil arrow pyarrow>=19.0.1 httpx

# Install Python dependencies for development
RUN pip3 install twine pytest pytest-asyncio pytest-cov pytest-xdist mypy pylint flake8 black isort

# Add .local/bin to PATH for the vscode user to remove WARNINGs
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc