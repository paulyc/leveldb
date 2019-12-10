// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstdio>
#include <include/leveldb/db.h>

#include "leveldb/dumpfile.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {
namespace {

class StdoutPrinter : public WritableFile {
 public:
  Status Append(const Slice& data) override {
    fwrite(data.data(), 1, data.size(), stdout);
    return Status::OK();
  }
  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }
};

bool HandleDumpCommand(Env* env, char** files, int num) {
  StdoutPrinter printer;
  bool ok = true;
  for (int i = 0; i < num; i++) {
    Status s = DumpFile(env, files[i], &printer);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      ok = false;
    }
  }
  return ok;
}

bool HandleCompactCommand(Env* env, const std::string &dbname) {
  leveldb::Status stat;
  do {
    leveldb::DB *db = nullptr;
    stat = leveldb::DB::Open(leveldb::Options{}, dbname, &db);
    if (!stat.ok()) break;
    db->CompactRange(nullptr, nullptr);
    delete db;
  } while (false);
  if (!stat.ok()) {
      fprintf(stderr, "%s\n", stat.ToString().c_str());
  }
  return stat.ok();
}

}  // namespace
}  // namespace leveldb

static void Usage() {
  fprintf(stderr,
          "Usage: leveldbutil command...\n"
          "   dump files...         -- dump contents of specified files\n"
          "   compact dbdirname     -- compact database in specified directory name\n");
}

int main(int argc, char** argv) {
  leveldb::Env* env = leveldb::Env::Default();
  bool ok = true;
  if (argc < 2) {
    Usage();
    ok = false;
  } else {
    std::string command = argv[1];
    if (command == "dump") {
      ok = leveldb::HandleDumpCommand(env, argv + 2, argc - 2);
    } else if (command == "compact" && argc == 3) {
      const std::string dbname = argv[2];
      ok = leveldb::HandleCompactCommand(env, dbname);
    } else {
      Usage();
      ok = false;
    }
  }
  return (ok ? 0 : 1);
}
