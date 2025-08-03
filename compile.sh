#!/bin/bash
set -ex

cargo build -p sqlite_vfs --features static

cd /tmp

# Copy sqlite source files we need
cp sqlite-src/sqlite3.c sqlite-src/sqlite3.h sqlite-src/shell.c .

# Copy our simple C wrapper
cp /code/sqlite_vfs/sqlite_with_grpsqlite.c .

# First create the shared library
echo "Building shared library..."
gcc -shared -o libsqlite3_with_grpsqlite.so \
    -Wl,-soname,libsqlite3_with_grpsqlite.so.1 \
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
    -fPIC \
    sqlite3.c  \
    -Wl,--whole-archive /code/target/debug/libsqlite_vfs.a -Wl,--no-whole-archive \
    -lpthread -ldl -lm

echo "Shared library built successfully"

# # Then create the executable using the shared library
# echo "Building executable..."
# gcc -o sqlite3_with_grpsqlite \
#     -I/tmp \
#     -DSQLITE_ENABLE_COLUMN_METADATA=1 \
#     -DSQLITE_ENABLE_LOAD_EXTENSION=1 \
#     -DSQLITE_ENABLE_FTS5=1 \
#     -DSQLITE_ENABLE_BATCH_ATOMIC_WRITE=1 \
#     -DSQLITE_ENABLE_DBSTAT_VTAB=1 \
#     -DSQLITE_ENABLE_NULL_TRIM=1 \
#     -DSQLITE_ENABLE_RTREE=1 \
#     -DHAVE_READLINE=0 \
#     -D_GNU_SOURCE \
#     -O2 \
#     shell.c \
#     -L/tmp -lsqlite3_with_grpsqlite \
#     -lpthread -ldl -lm

# echo "Executable built successfully"

# Install both the shared library and executable
echo "Installing files..."
cp libsqlite3_with_grpsqlite.so /usr/local/lib/
# cp sqlite3_with_grpsqlite /usr/local/bin/
cp sqlite3.h /usr/local/include/

# Also install as system SQLite for pkg-config
echo "Installing as system SQLite..."
cp libsqlite3_with_grpsqlite.so /usr/local/lib/libsqlite3.so.0
ln -sf /usr/local/lib/libsqlite3.so.0 /usr/local/lib/libsqlite3.so

# Create pkg-config file
echo "Creating pkg-config file..."
mkdir -p /usr/local/lib/pkgconfig
cat > /usr/local/lib/pkgconfig/sqlite3.pc << 'EOF'
prefix=/usr/local
exec_prefix=${prefix}
libdir=${exec_prefix}/lib
includedir=${prefix}/include

Name: SQLite
Description: SQL database engine with grpsqlite VFS
Version: 3.49.2
Libs: -L${libdir} -lsqlite3
Cflags: -I${includedir}
EOF

# Update library cache
echo "Updating library cache..."
ldconfig

