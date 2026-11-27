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

#pragma once

#include <mutex>
#include <string>

namespace engine::store {

/**
 * @brief The serialized trace with the pass's trace id added as "tid".
 *
 * The id is inserted into the serialized text as the first member of the
 * object, so the trace (which at FULL holds the object's data) is not
 * serialized a second time.
 *
 *   - empty traceId: the trace, unchanged
 *   - "" or "null" (no trace at this trace level): {"tid":"<id>"}
 *   - an object: the same object with "tid" first
 *   - any other value (not an object): the trace, unchanged
 */
std::string withPassTraceId(std::string trace, const std::string &traceId);

/**
 * @brief The levels of one pipe's nested enter/leave calls and the trace id
 * of the pass they belong to.
 *
 * A pass begins with the outermost begin() and ends when its level ends
 * (end(), or unwind() when an exception skipped the leave). Each pass gets a
 * new id; nested levels keep the outer one. Reentrant sub-invocations enter
 * and leave from other threads, so the level and the id change together under
 * one lock.
 *
 * Off is the caller's: the Debugger calls it only with --otel_pass_ids on a
 * monitored run, and otherwise uses an empty id.
 */
class PassIds {
public:
    PassIds() noexcept = default;
    PassIds(const PassIds &) = delete;
    PassIds &operator=(const PassIds &) = delete;


    /// One level more; at the outermost level a new pass with a new id.
    /// Returns the id of the pass this level belongs to.
    std::string begin() noexcept;

    /// One level less (the matching leave). Returns the id of the pass the
    /// ended level belonged to; the pass ends with its outermost level.
    std::string end() noexcept;

    /// One level less when the leave will not come (an exception unwound the
    /// call): the pass still ends with its outermost level.
    void unwind() noexcept;

    /// The current pass's id; empty between passes
    std::string current() const noexcept;

    /// The current nesting level (0 between passes)
    int level() const noexcept;

    /// A new W3C trace id: 32 lowercase hex chars, never all zero
    static std::string newId() noexcept;

private:
    // One level less (never below zero); at zero the pass is over. Under m_lock.
    void endLevelLocked() noexcept;

    int m_level = 0;
    std::string m_id;
    mutable std::mutex m_lock;
};

}  // namespace engine::store
