# PAJU-Q15
##  Data Generation (TPC-H)

This project uses the official **TPC-H `dbgen` tool** to generate benchmark data sets.  
The following steps describe how to generate a data set with **scale factor SF = 0.05**.

---

### Step 1: Prepare the Data Directory

Create a new directory for the target scale factor and copy the `dbgen` binary and required distribution files

```bash
mkdir -p ~/tpch-data005 \
&& cp ~/tpch-data1/dbgen ~/tpch-data005/ \
&& cp -r ~/tpch-data1/dists ~/tpch-data005/ \
&& chmod +x ~/tpch-data005/dbgen
```
## Run the PAJU project

### Step 2: Generate TPC-H Data

Run `dbgen` with the specified scale factor

```bash
cd ~/tpch-data005 \
&& ./dbgen -s 0.05 -f
```
### step 3: Package Project

```bash
mvn clean package -DskipTests
```
### step 4: Submit the Project to Flink

Start the Flink Cluster
```bash
./bin/start-cluster.sh
```
Submit the job to Flink Clster
```
./bin/flink run \
  -c org.example.aju.TpchAjuJobQ15 \
  /home/qiuyuan/project/aju-tpch-q15/target/aju-tpch-1.0-SNAPSHOT.jar
```
## Run the DBToaster Project
### step 1:Compile the Project
```bash
g++-10 -O3 -std=c++17 \
  main_stream.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/program_base.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/iprogram.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/runtime.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/streams.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/standard_adaptors.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/standard_functions.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/event.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/hpds/pstring.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/hpds/KDouble.cpp \
  ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/smhasher/MurmurHash2.cpp \
  -I ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++ \
  -I ~/project/dbtoaster-workspace/dbtoaster/lib/dbt_c++/HPDS \
  -lpthread -ltbb \
  -o q15_stream_exec
```
### step 2:Run the Project
```bash
./q15_stream_exec
```
