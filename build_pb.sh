#!/bin/bash
#
# To build the gRPC/protobuf (proto3) classes from the .proto. 
#
# NOTE:
# 1. A java grpc codegen plugin (not available on ARM-based 
#    computers) is required to create Java classes.
# 2. Python's plugin is available on ARM computers.
# 3. Do you need to generate new classes to add/remove 
#    attribute data?
#

project_base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# generated code is put here
generated_base=${project_base}/generated

# define protoc to use - otherwise it uses your env path
#PROTOC_HOME=/opt/homebrew/Cellar/protobuf/21.5

# REALLY IMPORTANT: what/where is your java codegen plugin
#java_codegen=/usr/local/grpc/java-plugin-1.40.0/install/java_plugin/protoc-gen-grpc-java

# intel processors only (not apple m1)
#java_codegen=/Users/gash/Downloads/protoc-gen-grpc-java-1.44.1-osx-aarch_64.exe

if [ -d ${generated_base} ]; then
  echo -e "\n* removing contents of ${generated_base}"
  rm -r ${generated_base}/*
else
  echo -e "\n* creating directory ${generated_base}"
  mkdir ${generated_base}
fi

# for all .proto files in resources
for p in `ls ${project_base}/resources/*.proto`; do
   base=$(basename $p |cut -f1 -d'.')
   echo -e "\n* compiling $base"
   protoc \
        --proto_path=${project_base}/resources \
        --java_out=${generated_base} \
        --plugin=protoc-gen-grpc-java=${java_codegen} \
        --grpc-java_out=${generated_base} $p
done

echo -e "\n* done, created:\n"
find ${generated_base} -name "*.java" | xargs basename
echo -e "\n"

 protoc --proto_path=/Users/vatsalbhanderi/CMPE-275/gc1-test-1/CMPE_275_GC1_Team_VZSH/grpc-test-1/resources --java_out=/Users/vatsalbhanderi/CMPE-275/gc1-test-1/CMPE_275_GC1_Team_VZSH/grpc-test-1/generated --plugin=protoc-gen-grpc-java --grpc-java_out=/Users/vatsalbhanderi/CMPE-275/gc1-test-1/CMPE_275_GC1_Team_VZSH/grpc-test-1/generated route.proto