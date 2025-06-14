# Start from the official Rust image
FROM rust:1.85

# Install essential build tools, clang/llvm, and SQLite dependencies
RUN apt-get update && \
    apt-get install -y \
    clang libclang-dev llvm \
    wget unzip build-essential tcl-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Install protoc from pre-compiled binaries (more reliable than package manager)
RUN PB_REL="https://github.com/protocolbuffers/protobuf/releases" && \
    PB_VERSION="28.3" && \
    wget $PB_REL/download/v$PB_VERSION/protoc-$PB_VERSION-linux-x86_64.zip && \
    unzip protoc-$PB_VERSION-linux-x86_64.zip -d /usr/local && \
    rm protoc-$PB_VERSION-linux-x86_64.zip && \
    protoc --version

# Min version supported is 3044000 (3.44.0)
ARG SQLITE_YEAR=2025
ARG SQLITE_FILENAME_VERSION=3490200
ARG SQLITE_TARBALL_FILENAME=sqlite-autoconf-${SQLITE_FILENAME_VERSION}.tar.gz

# Download SQLite source
RUN cd /tmp && \
    wget "https://www.sqlite.org/${SQLITE_YEAR}/${SQLITE_TARBALL_FILENAME}" && \
    tar xvfz "${SQLITE_TARBALL_FILENAME}" && \
    mv "sqlite-autoconf-${SQLITE_FILENAME_VERSION}" sqlite-src && \
    rm -rf /tmp/${SQLITE_TARBALL_FILENAME}

# Set the working directory in the container
WORKDIR /code

# Copy our project
COPY . .

# Build the static library with grpsqlite (debug build for faster compilation)
RUN cargo build -p sqlite_vfs --features static

# Create a custom SQLite binary with grpsqlite built in
RUN cd /tmp && \
    # Copy sqlite source files we need
    cp sqlite-src/sqlite3.c sqlite-src/sqlite3.h sqlite-src/shell.c . && \
    # Copy our simple C wrapper
    cp /code/sqlite_vfs/sqlite_with_grpsqlite.c . && \
    # Compile everything together
    gcc -o sqlite3_with_grpsqlite \
        -I/tmp \
        -DSQLITE_ENABLE_COLUMN_METADATA=1 \
        -DSQLITE_ENABLE_LOAD_EXTENSION=1 \
        -DSQLITE_ENABLE_FTS5=1 \
        -DSQLITE_ENABLE_BATCH_ATOMIC_WRITE=1 \
        -DSQLITE_ENABLE_DBSTAT_VTAB=1 \
        -DSQLITE_ENABLE_NULL_TRIM=1 \
        -DSQLITE_ENABLE_RTREE=1 \
        -DHAVE_READLINE=0 \
        -D_GNU_SOURCE \
        -O2 \
        sqlite3.c shell.c sqlite_with_grpsqlite.c \
        /code/target/debug/libsqlite_vfs.a \
        -lpthread -ldl -lm && \
    # Install the custom sqlite binary
    cp sqlite3_with_grpsqlite /usr/local/bin/ && \
    rm -rf /tmp/*

CMD ["/bin/bash"]
