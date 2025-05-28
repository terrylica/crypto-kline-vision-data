FROM mcr.microsoft.com/devcontainers/miniconda:1.1-3

# Configure conda
RUN conda config --set channel_priority strict

RUN conda install -y python=3.10
# RUN conda install -y -c conda-forge "ta-lib=0.5.1" # regularly check back with https://anaconda.org/conda-forge/ta-lib for updates on the latest version that conda can install

# Install and upgrade pip
RUN pip3 install --upgrade pip

# All Python dependencies are now managed through pyproject.toml
# and installed via the postCreateCommand in devcontainer.json
# which uses pip install -e '.[dev]'

# Add .local/bin to PATH for the vscode user to remove WARNINGs
RUN echo "export PATH=\$PATH:/home/vscode/.local/bin" >> /home/vscode/.bashrc
