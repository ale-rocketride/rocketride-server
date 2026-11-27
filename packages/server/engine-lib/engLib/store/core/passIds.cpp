// =============================================================================
// MIT License
// Copyright (c) 2026 Aparavi Software AG
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
// =============================================================================

#include <engLib/eng.h>

#include <cstdint>
#include <cstdio>
#include <random>

namespace engine::store {

std::string withPassTraceId(std::string trace, const std::string &traceId) {
    if (traceId.empty()) return trace;
    std::string member = "\"tid\":\"" + traceId + "\"";
    // No trace (trace level none): a null value, serialized "null"
    if (trace.empty() || trace == "null") return "{" + member + "}";
    if (trace.front() != '{') return trace;
    bool empty = trace.find_first_not_of(" \t\r\n", 1) == trace.size() - 1;
    trace.insert(1, empty ? member : member + ",");
    return trace;
}

std::string PassIds::begin() noexcept {
    std::lock_guard<std::mutex> guard(m_lock);
    if (m_level++ == 0) m_id = newId();
    return m_id;
}

std::string PassIds::end() noexcept {
    std::lock_guard<std::mutex> guard(m_lock);
    std::string id = m_id;
    endLevelLocked();
    return id;
}

void PassIds::unwind() noexcept {
    std::lock_guard<std::mutex> guard(m_lock);
    endLevelLocked();
}

std::string PassIds::current() const noexcept {
    std::lock_guard<std::mutex> guard(m_lock);
    return m_id;
}

int PassIds::level() const noexcept {
    std::lock_guard<std::mutex> guard(m_lock);
    return m_level;
}

void PassIds::endLevelLocked() noexcept {
    if (m_level > 0 && --m_level == 0) m_id.clear();
}

/// 128 random bits as 32 lowercase hex chars (never all zero). Not
/// cryptographic (mt19937_64): an identifier, never a secret. Each thread's
/// generator is seeded with 256 bits: run processes and threads across a fleet
/// must not share a seed, or they would repeat each other's ids.
std::string PassIds::newId() noexcept {
    thread_local std::mt19937_64 random = [] {
        std::random_device device;
        std::seed_seq seed{device(), device(), device(), device(),
                           device(), device(), device(), device()};
        return std::mt19937_64(seed);
    }();
    uint64_t high = random(), low = random();
    if (!high && !low) low = 1;
    char text[33];
    std::snprintf(text, sizeof(text), "%016llx%016llx",
                  static_cast<unsigned long long>(high),
                  static_cast<unsigned long long>(low));
    return text;
}

}  // namespace engine::store
