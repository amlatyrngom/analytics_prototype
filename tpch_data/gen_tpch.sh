# Create tables
cd tpch-dbgen || exit
make
./dbgen -f -s $1
mv *.tbl ..
cd ..

echo "Formatting..."
# Remove last | in each line; it messes up the csv reader.
if [ "$(uname)" == "Darwin" ]; then
  sed -i '' 's/[|]$//' part.tbl
  sed -i '' 's/[|]$//' supplier.tbl
  sed -i '' 's/[|]$//' partsupp.tbl
  sed -i '' 's/[|]$//' customer.tbl
  sed -i '' 's/[|]$//' orders.tbl
  sed -i '' 's/[|]$//' lineitem.tbl
  sed -i '' 's/[|]$//' nation.tbl
  sed -i '' 's/[|]$//' region.tbl
else
  sed -i 's/[|]$//' part.tbl
  sed -i 's/[|]$//' supplier.tbl
  sed -i 's/[|]$//' partsupp.tbl
  sed -i 's/[|]$//' customer.tbl
  sed -i 's/[|]$//' orders.tbl
  sed -i 's/[|]$//' lineitem.tbl
  sed -i 's/[|]$//' nation.tbl
  sed -i 's/[|]$//' region.tbl
fi
echo "Done formatting"
