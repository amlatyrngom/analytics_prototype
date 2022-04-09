#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch2/catch2.h"

#include "execution/execution_common.h"
#include "storage/table_loader.h"
#include "test_util/test_table_info.h"
#include "cuckoofilter/src/cuckoofilter.h"
using namespace smartid;



TEST_CASE("Simple Index Test") {
  auto date1 = Date::FromYMD(1992, 1, 2);
  auto date2 = Date::FromYMD(1998, 12, 1);
  auto date3 = Date::FromYMD(1992, 9, 1);

  auto mid = (date1.ToNative() + date2.ToNative()) / 2;
  auto mid_date = Date::FromNative(mid);

  std::cout << "Numbers: "
            << date1.ToNative() << ", "
            << date2.ToNative() << ", "
            << mid_date.ToNative() << ", "
            << date3.ToNative()
            << std::endl;

  std::cout << "Numbers: "
            << date1.ToString() << ", "
            << date2.ToString() << ", "
            << mid_date.ToString() << ", "
            << date3.ToString()
            << std::endl;
}