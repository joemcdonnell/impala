// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <type_traits>

#include "common/compiler-util.h"
#include "gutil/legacy_atomics/atomicops.h"
#include "gutil/macros.h"

namespace impala {

namespace internal {

/// Atomic integer. This class template should not be used directly; instead use the
/// typedefs below. 'T' can be either 32-bit or 64-bit signed integer. Each operation
/// is performed atomically and has a specified memory-ordering semantic:
///
/// Acquire: these operations ensure no later memory access by the same thread can be
/// reordered ahead of the operation. (C++11: memory_order_relaxed)
///
/// Release: these operations ensure that no previous memory access by the same thread
/// can be reordered after the operation (C++11: memory_order_release).
///
/// Barrier: these operations have both Acquire and Release semantics (C++11:
/// memory_order_acq_rel).
///
/// NoBarrier: these operations do not guarantee any ordering (C++11:
/// memory_order_relaxed). The compiler/CPU is free to reorder memory accesses (as seen
/// by other threads) just like any normal variable.
///
template<typename T>
class LegacyAtomicInt {
 public:
  LegacyAtomicInt(T initial = 0) : value_(initial) {
    static_assert(sizeof(T) == sizeof(legacy::base::subtle::Atomic32) ||
        sizeof(T) == sizeof(legacy::base::subtle::Atomic64),
            "Only AtomicInt32 and AtomicInt64 are implemented");
  }

  /// Atomic load with "acquire" memory-ordering semantic.
  ALWAYS_INLINE T Load() const {
    return legacy::base::subtle::Acquire_Load(&value_);
  }

  /// Atomic store with "release" memory-ordering semantic.
  ALWAYS_INLINE void Store(T x) {
    legacy::base::subtle::Release_Store(&value_, x);
  }

  /// Atomic add with "barrier" memory-ordering semantic. Returns the new value.
  ALWAYS_INLINE T Add(T x) {
    return legacy::base::subtle::Barrier_AtomicIncrement(&value_, x);
  }

  /// Atomically compare 'old_val' to 'value_' and set 'value_' to 'new_val' and return
  /// true if they compared equal, otherwise return false (and do no updates), with
  /// "barrier" memory-ordering semantic. That is, atomically performs:
  ///  if (value_ == old_val) {
  ///     value_ = new_val;
  ///     return true;
  ///  }
  ///  return false;
  ALWAYS_INLINE bool CompareAndSwap(T old_val, T new_val) {
    return legacy::base::subtle::Barrier_CompareAndSwap(&value_, old_val, new_val) == old_val;
  }

  /// Store 'new_val' and return the previous value. Implies a Release memory barrier
  /// (i.e. the same as Store()).
  ALWAYS_INLINE T Swap(T new_val) {
    return legacy::base::subtle::Release_AtomicExchange(&value_, new_val);
  }

 private:
  T value_;

  DISALLOW_COPY_AND_ASSIGN(LegacyAtomicInt);
};

} // namespace internal

/// Atomic pointer. Operations have the same semantics as AtomicInt.
template<typename T>
class LegacyAtomicPtr {
 public:
  LegacyAtomicPtr(T* initial = nullptr) : ptr_(reinterpret_cast<intptr_t>(initial)) {}

  /// Atomic load with "acquire" memory-ordering semantic.
  ALWAYS_INLINE T* Load() const { return reinterpret_cast<T*>(ptr_.Load()); }

  /// Atomic store with "release" memory-ordering semantic.
  ALWAYS_INLINE void Store(T* val) { ptr_.Store(reinterpret_cast<intptr_t>(val)); }

  /// Store 'new_val' and return the previous value. Implies a Release memory barrier
  /// (i.e. the same as Store()).
  ALWAYS_INLINE T* Swap(T* val) {
    return reinterpret_cast<T*>(ptr_.Swap(reinterpret_cast<intptr_t>(val)));
  }
 private:
  internal::LegacyAtomicInt<intptr_t> ptr_;
};

}

