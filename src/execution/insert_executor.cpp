//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "execution/execution_engine.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan) {
        if (child_executor.get()!= nullptr){
             child_executor.get()->Init();
            try {
                Tuple tuple;
                RID rid;
                while (child_executor.get()->Next(&tuple, &rid)) {
                    result_set.push_back(tuple);
                }
            } catch (Exception &e) {
            // TODO(student): handle exceptions
            }        
        }
 }

void InsertExecutor::Init() {
    auto *bpm = exec_ctx_->GetBufferPoolManager();
    auto table_id = plan_->TableOid();
    table_info = GetExecutorContext()->GetCatalog()->GetTable(table_id);
    auto *meta = table_info->table_.get();
    auto first_page_id = meta->GetFirstPageId();
    page_ = static_cast<TablePage *>(bpm->FetchPage(first_page_id));
    if (result_set.size() == 0) raw_values = plan_->RawValues();
}

bool InsertExecutor::Next(Tuple *tuple, RID *rid) {
     if (raw_values.size() == 0) {
         if (result_set.size() == 0) return false;
         table_info->table_->InsertTuple(result_set.front(), rid, exec_ctx_->GetTransaction()); 
         result_set.erase(result_set.begin());
         auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
         if(indexs_info.size() != 0){
            IndexInsert(indexs_info,tuple,rid);
         }
         return true;
     }
     std::vector<Value> entry;
     entry = raw_values.front();
     raw_values.erase(raw_values.begin());
     tuple = new Tuple(entry,&table_info->schema_);
     table_info->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
     auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    // tuple->GetRid() = *rid;
     if(indexs_info.size() != 0){
        IndexInsert(indexs_info,tuple,rid);
     }
     return true; 
}

void InsertExecutor::IndexInsert(std::vector<bustub::IndexInfo *> indexs_info, Tuple *tuple, RID *rid){
    for(auto index : indexs_info){
        auto key_attrs = index->index_.get()->GetMetadata()->GetKeyAttrs();
        index->index_.get()->InsertEntry(tuple->KeyFromTuple(table_info->schema_,
                                                             index->key_schema_,
                                                             key_attrs),
                                         *rid,
                                         exec_ctx_->GetTransaction());
    }
}

}  // namespace bustub
