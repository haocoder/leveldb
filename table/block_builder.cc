// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
// Block 中存储的是连续的一系列键值对，每隔若干个 Key 设置一个基准 Key，对于每
// 个key划分为两个部分，一个是 sharedKey，一个是 unsharedKey,前者表示相对基准
// Key 的共同前缀内容，后者表示相对基准 Key 的不同后缀部分，这样存储的时候，就可
// 以不存储sharedkey，从而节约空间。基准 Key 的特点就是 它的 sharedKey 部分是
// 空串。基准 Key 的位置，也就是它在块中的偏移量我们称之为「重启点」Restart Point，
// 以每个restart point开始的key-value序列构成一个restart array，也即这restart array
// 中的entry(key-value)共享相同的前缀，在Block的尾部，记录了所有restart point的偏移，
// 第一个restart point的偏移是0，也即block的第一个key。同时还可以使用二分查找搜索特定的key。
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]

//  | shared_bytes | unshared_bytes | value_length | key_delta | value |

// shared_bytes == 0 for restart points. (因为它是每个restart array的第一个key,前面没有其他key,所以没有共享部分）
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "include/comparator.h"
#include "include/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (int i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  // 添加一个新的key,采用共享前缀key的方式压缩存储
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    // 计算key和前一个key(last_key_piece)的相同前缀长度
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    // 前一个restart array以满，开始一个新的restart array
    restarts_.push_back(buffer_.size());        // 保存新的restart point偏移
    counter_ = 0;
  }
  // 如果block中还没有key,即buffer_是空的，那么当前要添加的key是第一个，则它的shared_key为0，
  // unshared key是它本身，即这个key是restart point(基准key)
  // 另外需要注意，一个restart array中可能所有的key都没有相同的前缀，或者只有部分有
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);      // 存储delta部分，即非共享部分
  buffer_.append(value.data(), value.size());           // 存储key-value的value部分

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}
