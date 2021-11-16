#include "common/date_type.h"
#include <iostream>
#include <sstream>

/**
 * Taken from NoisePage.
 */


namespace smartid {

namespace {

constexpr int64_t K_MONTHS_PER_YEAR = 12;
constexpr int32_t K_DAYS_PER_MONTH[2][12] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                             {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
constexpr int64_t K_HOURS_PER_DAY = 24;
constexpr int64_t K_MINUTES_PER_HOUR = 60;
constexpr int64_t K_SECONDS_PER_MINUTE = 60;
constexpr int64_t K_MILLISECONDS_PER_SECOND = 1000;
constexpr int64_t K_MICROSECONDS_PER_MILLISECOND = 1000;

constexpr std::string_view K_TIMESTAMP_SUFFIX = "::timestamp";

// Like Postgres, TPL stores dates as Julian Date Numbers. Julian dates are
// commonly used in astronomical applications and in software since it's
// numerically accurate and computationally simple. BuildJulianDate() and
// SplitJulianDate() correctly convert between Julian day and Gregorian
// calendar for all non-negative Julian days (i.e., from 4714-11-24 BC to
// 5874898-06-03 AD). Though the JDN number is unsigned, it's physically
// stored as a signed 32-bit integer, and comparison functions also use
// signed integer logic.
//
// Many of the conversion functions are adapted from implementations in
// Postgres. Specifically, we use the algorithms date2j() and j2date()
// in src/backend/utils/adt/datetime.c.

constexpr int64_t K_JULIAN_MIN_YEAR = -4713;
constexpr int64_t K_JULIAN_MIN_MONTH = 11;
constexpr int64_t K_JULIAN_MAX_YEAR = 5874898;
constexpr int64_t K_JULIAN_MAX_MONTH = 6;

// Is the provided year a leap year?
bool IsLeapYear(int32_t year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

// Does the provided date fall into the Julian date range?
bool IsValidJulianDate(int32_t y, int32_t m, int32_t d) {
  return (y > K_JULIAN_MIN_YEAR || (y == K_JULIAN_MIN_YEAR && m >= K_JULIAN_MIN_MONTH)) &&
      (y < K_JULIAN_MAX_YEAR || (y == K_JULIAN_MAX_YEAR && m < K_JULIAN_MAX_MONTH));
}

// Is the provided date a valid calendar date?
bool IsValidCalendarDate(int32_t year, int32_t month, int32_t day) {
  // There isn't a year 0. We represent 1 BC as year zero, 2 BC as -1, etc.
  if (year == 0) return false;

  // Month.
  if (month < 1 || month > K_MONTHS_PER_YEAR) return false;

  // Day.
  if (day < 1 || day > K_DAYS_PER_MONTH[IsLeapYear(year)][month - 1]) return false;

  // Looks good.
  return true;
}

// Based on date2j().
uint32_t BuildJulianDate(uint32_t year, uint32_t month, uint32_t day) {
  if (month > 2) {
    month += 1;
    year += 4800;
  } else {
    month += 13;
    year += 4799;
  }

  int32_t century = year / 100;
  int32_t julian = year * 365 - 32167;
  julian += year / 4 - century + century / 4;
  julian += 7834 * month / 256 + day;

  return julian;
}

// Based on j2date().
void SplitJulianDate(int32_t jd, int32_t *year, int32_t *month, int32_t *day) {
  uint32_t julian = jd;
  julian += 32044;
  uint32_t quad = julian / 146097;
  uint32_t extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;
  int32_t y = julian * 4 / 1461;
  julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
  y += quad * 4;
  *year = y - 4800;
  quad = julian * 2141 / 65536;
  *day = julian - 7834 * quad / 256;
  *month = (quad + 10) % K_MONTHS_PER_YEAR + 1;
}

// Split a Julian time (i.e., Julian date in microseconds) into a time and date
// component.
void StripTime(int64_t jd, int64_t *date, int64_t *time) {
  *date = jd / K_MICRO_SECONDS_PER_DAY;
  *time = jd - (*date * K_MICRO_SECONDS_PER_DAY);
}

// Given hour, minute, second, millisecond, and microsecond components, build a time in microseconds.
int64_t BuildTime(int32_t hour, int32_t min, int32_t sec, int32_t milli = 0, int32_t micro = 0) {
  return (((hour * K_MINUTES_PER_HOUR + min) * K_SECONDS_PER_MINUTE) * K_MICRO_SECONDS_PER_SECOND) +
      sec * K_MICRO_SECONDS_PER_SECOND + milli * K_MILLISECONDS_PER_SECOND + micro;
}

// Given a time in microseconds, split it into hour, minute, second, and
// fractional second components.
void SplitTime(int64_t jd, int32_t *hour, int32_t *min, int32_t *sec, int32_t *millisec, int32_t *microsec) {
  int64_t time = jd;

  *hour = time / K_MICRO_SECONDS_PER_HOUR;
  time -= (*hour) * K_MICRO_SECONDS_PER_HOUR;
  *min = time / K_MICRO_SECONDS_PER_MINUTE;
  time -= (*min) * K_MICRO_SECONDS_PER_MINUTE;
  *sec = time / K_MICRO_SECONDS_PER_SECOND;
  int32_t fsec = time - (*sec * K_MICRO_SECONDS_PER_SECOND);
  *millisec = fsec / 1000;
  *microsec = fsec % 1000;
}

// Check if a string value ends with string ending
bool EndsWith(const char *str, std::size_t len, const char *suffix, std::size_t suffix_len) {
  if (suffix_len > len) return false;
  return (std::strncmp(str + len - suffix_len, suffix, suffix_len) == 0);
}

}  // namespace

//===----------------------------------------------------------------------===//
//
// Date
//
//===----------------------------------------------------------------------===//

bool Date::IsValid() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return IsValidJulianDate(year, month, day);
}

std::string Date::ToString() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  std::stringstream ss;
  ss << year << "-";
  if (month < 10) {
    ss << "0";
  }
  ss << month << "-";
  if (day < 10) {
    ss << "0";
  }
  ss << day;
  return ss.str();
}

int32_t Date::ExtractYear() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return year;
}

int32_t Date::ExtractMonth() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return month;
}

int32_t Date::ExtractDay() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return day;
}

void Date::ExtractComponents(int32_t *year, int32_t *month, int32_t *day) { SplitJulianDate(value_, year, month, day); }

Date::NativeType Date::ToNative() const { return value_; }

Date Date::FromNative(Date::NativeType val) { return Date{val}; }

Date Date::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && static_cast<bool>(std::isspace(*ptr))) ptr++;
  while (ptr != limit && static_cast<bool>(std::isspace(*(limit - 1)))) limit--;

  uint32_t year = 0, month = 0, day = 0;

#define DATE_ERROR std::cerr << "DATE ERROR" << std::endl;

  // Year
  while (true) {
    if (ptr == limit) DATE_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      year = year * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      DATE_ERROR;
    }
  }

  // Month
  while (true) {
    if (ptr == limit) DATE_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      month = month * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      DATE_ERROR;
    }
  }

  // Day
  while (true) {
    if (ptr == limit) break;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      day = day * 10 + (c - '0');
    } else {
      DATE_ERROR;
    }
  }

  return Date::FromYMD(year, month, day);
}

Date Date::FromYMD(int32_t year, int32_t month, int32_t day) {
  // Check calendar date.
  if (!IsValidCalendarDate(year, month, day)) {
    DATE_ERROR
  }

  // Check if date would overflow Julian calendar.
  if (!IsValidJulianDate(year, month, day)) {
    DATE_ERROR
  }

  return Date(BuildJulianDate(year, month, day));
}

bool Date::IsValidDate(int32_t year, int32_t month, int32_t day) { return IsValidJulianDate(year, month, day); }

}
