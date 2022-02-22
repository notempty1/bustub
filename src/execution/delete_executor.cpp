//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
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

void DeleteExecutor::Init() {
    auto *bpm = exec_ctx_->GetBufferPoolManager();
    auto table_id = plan_->TableOid();
    table_info = GetExecutorContext()->GetCatalog()->GetTable(table_id);
    auto *meta = table_info->table_.get();
    auto first_page_id = meta->GetFirstPageId();
    page_ = static_cast<TablePage *>(bpm->FetchPage(first_page_id));
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    if(result_set.size() == 0) return false;
    *tuple = result_set.front();
    table_info->table_.get()->MarkDelete(result_set.front().GetRid(), exec_ctx_->GetTransaction());
    auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
     if(indexs_info.size() != 0){    
         IndexDelete(indexs_info,tuple);
     }
    result_set.erase(result_set.begin());
    return true;
 }

void DeleteExecutor::IndexDelete(std::vector<bustub::IndexInfo *> indexs_info, Tuple *tuple){
    for(auto index : indexs_info){
        auto key_attrs = index->index_.get()->GetMetadata()->GetKeyAttrs();
        index->index_.get()->DeleteEntry(tuple->KeyFromTuple(table_info->schema_,
                                                             index->key_schema_,
                                                             key_attrs),
                                         tuple->GetRid(),
                                         exec_ctx_->GetTransaction());
    }
}
}  // namespace bustub
