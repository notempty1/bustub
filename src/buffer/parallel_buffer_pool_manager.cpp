//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances)
{
  for(uint32_t i = 0; i < num_instances; i++){
    auto *bpi = new BufferPoolManagerInstance(pool_size, num_instances_, i, disk_manager);
    bpi_map[i] = bpi;
    bpi_index.push_back(i);
   // printf("%d bpi init success\n",i);
  }

  // Allocate and create individual BufferPoolManagerInstances
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return 0;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return nullptr;
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  auto index = page_id % num_instances_;
  std::list<int>::iterator it = find(bpi_index.begin(), bpi_index.end(), index);
  bpi_index.erase(it);
  bpi_index.push_front(index);
  auto *bpi = bpi_map[index];
  while(!bpi->TryLock());
  bpi->GetLock();
  auto *res = bpi->FetchPage(page_id);
  if(res == nullptr) printf("nullptr\n");
  bpi->UnLock();
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return res;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  auto index = page_id % num_instances_;
  std::list<int>::iterator it = find(bpi_index.begin(), bpi_index.end(), index);
  bpi_index.erase(it);
  bpi_index.push_front(index);
  auto *bpi = bpi_map[index];
  while(!bpi->TryLock());
  bpi->GetLock();
  bool res = bpi->UnpinPage(page_id, is_dirty);
  bpi->UnLock();
  // Unpin page_id from responsible BufferPoolManagerInstance
  return res;
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return false;
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  size_t cnt = 0;
  while(cnt != bpi_index.size()){
    auto i = bpi_index.front();
    auto *bpi = bpi_map[i];
    bpi_index.pop_front();
    bpi_index.push_back(i);  
    cnt++;
    if(bpi->GetFreeListSize() != 0){
      while(!bpi->TryLock());
      bpi->GetLock();
      auto *tmp = bpi->NewPage(page_id);
      // printf("%d bpi new page\n",i);
      bpi->UnLock();      
      return tmp;
      }else{
        continue;
      }
    }
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  return NULL;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return false;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
}

}  // namespace bustub
