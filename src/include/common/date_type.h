#pragma once
#include <string>
#include <cstring>
#include <cstdint>
#include <ostream>

/**
 * Taken from NoisePage.
 */

namespace smartid {
static constexpr int64_t K_MICRO_SECONDS_PER_SECOND = 1000 * 1000;
static constexpr int64_t K_MICRO_SECONDS_PER_MINUTE = 60UL * K_MICRO_SECONDS_PER_SECOND;
static constexpr int64_t K_MICRO_SECONDS_PER_HOUR = 60UL * K_MICRO_SECONDS_PER_MINUTE;
static constexpr int64_t K_MICRO_SECONDS_PER_DAY = 24UL * K_MICRO_SECONDS_PER_HOUR;

class Date {
    public:
    /**
     * The internal representation of a SQL date.
     */
    using NativeType = int32_t;

    /**
     * Empty constructor.
     */
    Date() = default;

    /**
     * @return True if this is a valid date instance; false otherwise.
     */
    bool IsValid() const;

    /**
     * @return A string representation of this date in the form "YYYY-MM-MM".
     */
    std::string ToString() const;

    /**
     * @return The year of this date.
     */
    int32_t ExtractYear() const;

    /**
     * @return The month of this date.
     */
    int32_t ExtractMonth() const;

    /**
     * @return The day of this date.
     */
    int32_t ExtractDay() const;

    /**
     * Convert this date object into its year, month, and day parts.
     * @param[out] year The year corresponding to this date.
     * @param[out] month The month corresponding to this date.
     * @param[out] day The day corresponding to this date.
     */
    void ExtractComponents(int32_t *year, int32_t *month, int32_t *day);

    /**
     * @return True if this date equals @em that date; false otherwise.
     */
    bool operator==(const Date &that) const { return value_ == that.value_; }

    /**
     * @return True if this date is not equal to @em that date; false otherwise.
     */
    bool operator!=(const Date &that) const { return value_ != that.value_; }

    /**
     * @return True if this data occurs before @em that date; false otherwise.
     */
    bool operator<(const Date &that) const { return value_ < that.value_; }

    /**
     * @return True if this data occurs before or is the same as @em that date; false otherwise.
     */
    bool operator<=(const Date &that) const { return value_ <= that.value_; }

    /**
     * @return True if this date occurs after @em that date; false otherwise.
     */
    bool operator>(const Date &that) const { return value_ > that.value_; }

    /**
     * @return True if this date occurs after or is equal to @em that date; false otherwise.
     */
    bool operator>=(const Date &that) const { return value_ >= that.value_; }

    /**
     * TODO(Amadou): This is a stupid to get histograms to work with dates. Think of a better way.
     * @return Difference of two JDN.
     */
    Date::NativeType operator-(const Date& that) const {
      return value_ - that.value_;
    }

    /**
     * TODO(Amadou): Same todo as above.
     * @return Increment JDN.
     */
    Date operator+(const Date::NativeType& x) const {
      return Date::FromNative(value_ + x);
    }

  /**
   * TODO(Amadou): Same todo as above.
   * @return Decrement JDN.
   */
  Date operator-(const Date::NativeType& x) const {
    return Date::FromNative(value_ - x);
  }


  /** @return The native representation of the date (julian microseconds). */
    Date::NativeType ToNative() const;

    /**
     * Construct a Date with the specified native representation.
     * @param val The native representation of a date.
     * @return The constructed Date.
     */
    static Date FromNative(Date::NativeType val);

    /**
     * Convert a C-style string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert
     * the first date-like object it sees, skipping any leading whitespace.
     * @param str The string to convert.
     * @param len The length of the string.
     * @return The constructed date. May be invalid.
     */
    static Date FromString(const char *str, std::size_t len);

    /**
     * Convert a string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert the
     * first date-like object it sees, skipping any leading whitespace.
     * @param str The string to convert.
     * @return The constructed Date. May be invalid.
     */
    static Date FromString(std::string_view str) { return FromString(str.data(), str.size()); }

    /**
     * Create a Date instance from a specified year, month, and day.
     * @param year The year of the date.
     * @param month The month of the date.
     * @param day The day of the date.
     * @return The constructed date. May be invalid.
     */
    static Date FromYMD(int32_t year, int32_t month, int32_t day);

    /**
     * Is the date corresponding to the given year, month, and day a valid date?
     * @param year The year of the date.
     * @param month The month of the date.
     * @param day The day of the date.
     * @return True if valid date.
     */
    static bool IsValidDate(int32_t year, int32_t month, int32_t day);

    private:
    // Private constructor to force static factories.
    explicit Date(NativeType value) : value_(value) {}

    private:
    // Date value
    NativeType value_;
};
}

