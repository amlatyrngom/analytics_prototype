sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-key C99B11DEB97541F0
sudo apt-add-repository https://cli.github.com/packages
sudo apt update
sudo apt install gh

sudo apt install -y clang-11 openssl-dev zip unzip build-essential
wget https://github.com/Kitware/CMake/releases/download/v3.21.0-rc1/cmake-3.21.0-rc1.tar.gz
tar -xvf cmake-3.21.0-rc1.tar.gz
cd cmake-3.21.0-rc1/
./bootstrap
make
make install
cd ..

cd job_light_workload
wget https://muimages.sfo2.digitaloceanspaces.com/job_light_trimmed_data.zip
unzip job_light_trimmed_data.zip
cd ..

export CC=clang-11
export CXX=clang++-11

mkdir cmake-debug
cd cmake-debug
cmake .. -DCMAKE_BUILD_TYPE=Debug
cd ..

mkdir cmake-release
cd cmake-release
cmake .. -DCMAKE_BUILD_TYPE=Release
cd ..


