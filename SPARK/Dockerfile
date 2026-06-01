FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y vim-tiny && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*