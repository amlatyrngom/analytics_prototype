#include <iostream>
#include "storage/table.h"
#include "storage/table_loader.h"
#include "common/catalog.h"

using namespace smartid;

// Print some TPCH tables to stdout for sanity check.
int main() {
  TableLoader::LoadTPCH("tpch_data");
  auto catalog = Catalog::Instance();
  auto region = catalog->GetTable("region");
  TableIterator r_ti(region);
  while (r_ti.Advance()) {
    auto col1 = r_ti.VectorAt(0);
    auto col2 = r_ti.VectorAt(1);
    auto col3 = r_ti.VectorAt(2);
    auto col1_data = col1->DataAs<int32_t>();
    auto col2_data = col2->DataAs<Varlen>();
    auto col3_data = col3->DataAs<Varlen>();
    for (uint64_t i = 0; i < col1->NumElems(); i++) {
      std::string attr2(col2_data[i].Data(), col2_data[i].Size());
      std::string attr3(col3_data[i].Data(), col3_data[i].Size());
      std::cout << col1_data[i] << ", " << attr2 << ", " << attr3 << std::endl;
    }
  }

  // Customers
  auto customer = catalog->GetTable("customer");
  TableIterator c_ti(customer);
  while (c_ti.Advance()) {
    auto num_elems = c_ti.VectorAt(0)->NumElems();
    auto custkey_col = c_ti.VectorAt(0)->DataAs<int32_t>();
    auto name_col = c_ti.VectorAt(1)->DataAs<Varlen>();
    auto address_col = c_ti.VectorAt(2)->DataAs<Varlen>();
    for (uint64_t i = 0; i < num_elems; i++) {
      std::string name_attr(name_col[i].Data(), name_col[i].Size());
      std::string addr_attr(address_col[i].Data(), address_col[i].Size());
      std::cout << custkey_col[i] << ", " << name_attr << ", " << addr_attr << std::endl;
    }
  }

  // Orders
  auto orders = catalog->GetTable("orders");
  TableIterator o_ti(orders);
  while (o_ti.Advance()) {
    auto num_elems = o_ti.VectorAt(0)->NumElems();
    auto orderkey_col = o_ti.VectorAt(0)->DataAs<int32_t>();
    auto custkey_col = o_ti.VectorAt(1)->DataAs<int32_t>();
    auto orderstatus_col = o_ti.VectorAt(2)->DataAs<char>();
    auto totalprice_col = o_ti.VectorAt(3)->DataAs<float>();
    auto orderdate_col = o_ti.VectorAt(4)->DataAs<Date>();
    auto orderpriority_col = o_ti.VectorAt(5)->DataAs<Varlen>();
    for (uint64_t i = 0; i < num_elems; i++) {
      std::string prio_attr(orderpriority_col[i].Data(), orderpriority_col[i].Size());
      std::cout << orderkey_col[i] << ", "
                << custkey_col[i] << ", "
                << orderstatus_col[i] << ", "
                << totalprice_col[i] << ", "
                << orderdate_col[i].ToString() << ", "
                << prio_attr << ", "
                << std::endl;
    }
  }

  return 0;
}
