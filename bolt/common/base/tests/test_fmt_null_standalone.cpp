// Standalone test to reproduce the fmt "string pointer is null" bug.
// Compile: g++ -std=c++17 -o test_fmt_null test_fmt_null_standalone.cpp -lfmt -lgtest -lgtest_main
// Run: ./test_fmt_null

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <string>

// Simulates the errorMessage() template from bolt/common/base/Exceptions.h
// This is the function that BOLT_FAIL calls internally with format args.
template <typename... Args>
std::string errorMessage(fmt::string_view fmt_str, const Args&... args) {
  return fmt::vformat(fmt_str, fmt::make_format_args(args...));
}

// Reproduces the core bug: fmt::format with a null const char* throws
// "string pointer is null" instead of formatting gracefully.
TEST(FmtNullPointerTest, fmtFormatWithNullConstCharPtr) {
  const char* nullPtr = nullptr;

  try {
    auto result = fmt::format("error: {}", nullPtr);
    FAIL() << "Expected fmt::format to throw, but got: " << result;
  } catch (const fmt::format_error& e) {
    EXPECT_NE(
        std::string(e.what()).find("string pointer is null"), std::string::npos)
        << "Unexpected error: " << e.what();
    // Test passes: confirmed fmt throws on null const char*
  }
}

// Reproduces the BOLT_FAIL scenario: errorMessage() (which uses fmt::vformat)
// receives a null const char* from e.what() as a format argument.
TEST(FmtNullPointerTest, errorMessageWithNullConstCharPtr) {
  const char* nullPtr = nullptr;

  try {
    auto msg = errorMessage("convert to integer error: {}, extra: {}", nullPtr, "info");
    FAIL() << "Expected errorMessage to throw, but got: " << msg;
  } catch (const fmt::format_error& e) {
    EXPECT_NE(
        std::string(e.what()).find("string pointer is null"), std::string::npos)
        << "Unexpected error: " << e.what();
    // Test passes: confirmed the BOLT_FAIL code path would throw fmt::format_error
    // instead of the intended BoltRuntimeError
  }
}

// Simulates the exact SplitReader.cpp:492-494 scenario:
// A std::exception is caught, then e.what() is passed to a format call.
TEST(FmtNullPointerTest, catchAndRethrowWithNullWhat) {
  // Custom exception that returns nullptr from what() - simulates a corrupted
  // exception state under memory pressure.
  struct NullWhatException : public std::exception {
    const char* what() const noexcept override {
      return nullptr;
    }
  };

  try {
    try {
      throw NullWhatException();
    } catch (const std::exception& e) {
      // This is the pattern from SplitReader.cpp:492-494:
      //   BOLT_FAIL("convert to integer error: {}, ...", e.what(), ...);
      auto msg = errorMessage("convert to integer error: {}", e.what());
      FAIL() << "Expected throw, but got: " << msg;
    }
  } catch (const fmt::format_error& e) {
    EXPECT_NE(
        std::string(e.what()).find("string pointer is null"), std::string::npos)
        << "Unexpected error: " << e.what();
    // Test passes: the original exception info is lost, user sees
    // "string pointer is null" - this is the bug.
  }
}
