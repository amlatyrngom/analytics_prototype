#pragma once
#include <SQLiteCpp/Database.h>
#include <SQLiteCpp/Transaction.h>

namespace smartid {

struct InfoStore {
  // If this becomes bottleneck, split into a map of dbs for each subsystem.
  // Or use something faster than sqlite.
  std::unique_ptr<SQLite::Database> db;
};

}