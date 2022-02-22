//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {}

void SeqScanExecutor::Init() {
    auto *bpm = exec_ctx_->GetBufferPoolManager();
    auto table_id = plan_->GetTableOid();
    auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(table_id);
    auto *meta = table_info->table_.get();
    auto first_page_id = meta->GetFirstPageId();
    page_ = static_cast<TablePage *>(bpm->FetchPage(first_page_id));
    RID *rid = new RID();
    page_->GetFirstTupleRid(rid);
    cur_rid = rid;
}
//加上有index的情况，即有where子句或者其他判断时，检查where子句中的判断条件是否有对应的索引，然后根据索引找到
//对应tuple，而不是直接遍历。3.
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
     if (page_->GetTuple(*cur_rid,tuple,exec_ctx_->GetTransaction(),exec_ctx_->GetLockManager())){
         if(!page_->GetNextTupleRid(*cur_rid,rid)) {
             auto next_page_id = page_->GetNextPageId();
             auto *bpm = exec_ctx_->GetBufferPoolManager();
             bpm->UnpinPage(page_->GetPageId(), false);
             page_ = static_cast<TablePage *>(bpm->FetchPage(next_page_id));
             rid->Set(next_page_id,0);
         }
         cur_rid = rid;
         auto predicate = plan_->GetPredicate();
         if (predicate == nullptr)  return true;
         if ( predicate->Evaluate(tuple,plan_->OutputSchema()).ToString() == "false"){

             if(Next(tuple, rid)) return true;
             else return false;
         }
         return true;
     }
    
    return false; 
}

}  // namespace bustub
