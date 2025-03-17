FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.10
RUN conda install -y -c conda-forge "ta-lib=0.5.1"

# Install Python dependencies
RUN pip3 install pandas==2.2.1 numpy==1.24.4 numba==0.60.0 \
    matplotlib==3.9.2 plotly==5.23.0 dash==2.17.1 dash-bootstrap-components==1.6.0 talipp==2.4.0 \
    joblib==1.4.2 tenacity==9.0.0 scipy==1.14.0 rich==13.7.1 aiohttp==3.11.13 \
    psutil==5.9.8 arrow==1.3.0 pyarrow==14.0.2 httpx==0.27.0

# Install Python dependencies for development
RUN pip3 install twine pytest>=8.0.0 pytest-asyncio>=0.23.5

# Add .local/bin to PATH for the vscode user to remove WARNINGs
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc

# Configure Git for better DevContainer experience
RUN apt-get update && apt-get install -y --no-install-recommends \
    git-lfs \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Fix SSH config for DevContainer
RUN mkdir -p /home/vscode/.ssh && \
    echo "Host github.com\n\tStrictHostKeyChecking no\n" >> /home/vscode/.ssh/config && \
    chown -R vscode:vscode /home/vscode/.ssh && \
    chmod 700 /home/vscode/.ssh && \
    chmod 600 /home/vscode/.ssh/config
