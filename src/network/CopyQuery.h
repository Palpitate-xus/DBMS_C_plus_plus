#pragma once

#include <memory>
#include <string>

struct Session;

namespace dbms {

class PreparedQueryCursor;

// The server owns the COPY transaction and the cursor borrows it. The
// application's query planner supplies a fresh typed cursor for each COPY.
using CopyQueryFactory = std::shared_ptr<PreparedQueryCursor> (*)(
    Session&, const std::string&);

bool startServer(int port, bool allowPlaintext, CopyQueryFactory openCopyQuery);

} // namespace dbms
