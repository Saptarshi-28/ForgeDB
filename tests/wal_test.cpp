#include "storage/WAL.h"

int main() {
    forgedb::storage::WAL wal("forge.wal");

    wal.append("SET name Saptarshi");
    wal.append("SET city Delhi");
    wal.append("DELETE name");

    return 0;
}