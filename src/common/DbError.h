#pragma once

#include <stdexcept>
#include <string>
#include <utility>

namespace dbms {

// SQLSTATE is execution metadata, not something a protocol adapter should
// infer from translated or user-controlled message text. what() retains the
// legacy CLI diagnostic while structured consumers use the separate fields.
class DbError : public std::runtime_error {
public:
    DbError(std::string sqlState, std::string message)
        : std::runtime_error(message + " (SQLSTATE " + sqlState + ")"),
          sqlState_(std::move(sqlState)), message_(std::move(message)) {}

    const std::string& sqlState() const noexcept { return sqlState_; }
    const std::string& message() const noexcept { return message_; }

private:
    std::string sqlState_;
    std::string message_;
};

}  // namespace dbms
