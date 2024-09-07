# Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

hiactor_add_demo (LDBC-IC5
  SOURCES
    LDBC-IC2.cc
    actor/map.act.cc
    actor/map.act.h  
    actor/shuffle.act.cc
    actor/shuffle.act.h  
    actor/barrier.act.cc
    actor/barrier.act.h  
    actor/file_sink.act.h 
    actor/file_sink.act.cc
    actor/file_source.act.h 
    actor/file_source.act.cc
    actor/end_data_sink.act.cc
    actor/end_data_sink.act.h
    actor/end_file_sink.act.cc
    actor/end_file_sink.act.h
    actor/data_sink.act.h 
    actor/data_sink.act.cc
    actor/data_source.act.h 
    actor/data_source.act.cc
#    actor/send_back_result.act.cc
#    actor/send_back_result.act.h
    actor/flat_map.act.cc
    actor/flat_map.act.h
    actor/map_partition.act.cc
    actor/map_partition.act.h
    DataFlow/DataFlow.h
    DataFlow/DataFlow.cc
    Executor/Executor.h
    Executor/Executor.cc
    Executor/Project.h
    Executor/Project.cc
    Executor/Project_node.h
    Executor/Project_node.cc
    Executor/Project_edge.h
    Executor/Project_edge.cc
    Executor/NodeByIDScan.cc
    Executor/NodeByIDScan.h
    Executor/Expand.h
    Executor/Expand.cc
    Executor/Expand_vec.h
    Executor/Expand_vec.cc
    Executor/VarExpand.h
    Executor/VarExpand.cc
    Executor/Visit_Expand.h
    Executor/Visit_Expand.cc
    Executor/Expand_IC5.h
    Executor/Expand_IC5.cc
    Executor/ExpandInto.h
    Executor/ExpandInto.cc
    Executor/Add_distance.h
    Executor/Add_distance.cc
    Executor/Top.h
    Executor/Top.cc
    Executor/Filter.cc
    Executor/Filter.h
    Executor/Distinct.cc
    Executor/Distinct.h
    Executor/ReduceByKey.cc
    Executor/ReduceByKey.h
    Executor/Timing.cc
    Executor/Timing.h
    Executor/file_sink_exe.h
    Graph_source/Graph_source.h
    Graph_source/Graph_source_data.h
    Graph_source/Graph_source.cc
    )
