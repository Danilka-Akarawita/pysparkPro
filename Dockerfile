FROM debian:12-slim

# Install base dependencies
RUN apt-get update && apt-get install -y \
    wget \
    bzip2 \
    ca-certificates \
    openjdk-17-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Miniconda
ENV CONDA_DIR=/opt/conda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p $CONDA_DIR && \
    rm miniconda.sh

# Set environment variables
ENV PATH=$CONDA_DIR/bin:$PATH

# Create environment from YAML
COPY environment.yml .
RUN conda env create -f environment.yml

# Make RUN commands use the new environment
SHELL ["conda", "run", "-n", "ag_news", "/bin/bash", "-c"]

# Install Python 3.11 explicitly
RUN conda install -c conda-forge python=3.11

# Copy code
WORKDIR /app
COPY code /app/code



# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8080/health || exit 1

# Set proper entrypoint
ENTRYPOINT ["conda", "run", "-n", "ag_news"]
