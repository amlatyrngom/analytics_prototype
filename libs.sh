mkdir -p installations
cd installations


sudo apt install clang-11
sudo apt install openssl
wget https://github.com/Kitware/CMake/releases/download/v3.21.0-rc1/cmake-3.21.0-rc1.tar.gz
tar -xvf cmake-3.21.0-rc1.tar.gz
cd cmake-3.21.0-rc1/
./bootstrap
make
make install

export CC=clang-11
export CXX=clang++-11
cmake .. -DCMAKE_BUILD_TYPE=Release

