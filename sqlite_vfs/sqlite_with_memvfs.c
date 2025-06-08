// Simple C constructor to initialize the Rust grpcvfs before SQLite starts
extern int initialize_grpcvfs(void);

__attribute__((constructor)) static void init()
{
    initialize_grpcvfs();
}
