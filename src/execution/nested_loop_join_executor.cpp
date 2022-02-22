//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),
    left_child_executor_(std::move(left_executor)),
    right_child_executor_(std::move(right_executor)) {
    }

void NestedLoopJoinExecutor::Init() { 
     if (left_child_executor_.get()!= nullptr){
             left_child_executor_.get()->Init();
            try {
                Tuple tuple;
                RID rid;
                while (left_child_executor_.get()->Next(&tuple, &rid)) {
                    left_result_set.push_back(tuple);
                }
            } catch (Exception &e) {
            // TODO(student): handle exceptions
            }        
        }
         if (right_child_executor_.get()!= nullptr){
             right_child_executor_.get()->Init();
            try {
                Tuple tuple;
                RID rid;
                while (right_child_executor_.get()->Next(&tuple, &rid)) {
                    right_result_set.push_back(tuple);
                }
            } catch (Exception &e) {
            // TODO(student): handle exceptions
            }        
        }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
