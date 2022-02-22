//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    // int len = lru_map.size();
    // if (len != 0){
    //     *frame_id = lru_map.begin()->second;
    //     lru_map.erase(lru_map.begin()->first);
    // }
    if(st == lru_map.end()) st = lru_map.begin();
    *frame_id = st->second;
    auto tmp = st;
    st ++;
    lru_map.erase(tmp->first);
    return false; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    if(lru_map.find(frame_id) != lru_map.end()){
        if (lru_map.find(frame_id) == st){
            st ++;
        }
        lru_map.erase(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    int len = lru_map.size();
    if(lru_map.find(frame_id) == lru_map.end()){     
    //    int loc = lru_map.end()->second;   
        lru_map[len+1] = frame_id;
        //printf("lru_map = %d, ele = %d\n", len, frame_id);
    }
}

size_t LRUReplacer::Size() { return lru_map.size(); }

}  // namespace bustub
