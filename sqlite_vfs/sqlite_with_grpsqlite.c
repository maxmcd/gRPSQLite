// Simple C constructor to initialize the Rust grpsqlite before SQLite starts
extern int initialize_grpsqlite(void);

__attribute__((constructor)) static void init()
{
    initialize_grpsqlite();
}
