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

#include "../store.h"

//-----------------------------------------------------------------------------
// Unit tests for the pass trace ids the debugger sends with its ENTER/LEAVE
// events (engine::store::PassIds and engine::store::withPassTraceId).
//
// A pass is one run of the pipeline over an object on a pipe: it begins with
// the outermost level (begin) and ends when that level ends (end, or unwind
// when an exception skipped the leave). Nested levels keep the outer pass's
// id; the next pass gets a new one.
//-----------------------------------------------------------------------------
namespace {

// A pass id is a W3C trace id: 32 lowercase hex chars
bool isTraceId(const std::string &id) {
    return id.size() == 32 &&
           id.find_first_not_of("0123456789abcdef") == std::string::npos &&
           id != std::string(32, '0');
}

// Simulates an exception between enter and leave: the scope's destructor
// unwinds the level, as Debugger::Level does when leave() was never called
struct UnwindOnExit {
    PassIds &ids;
    ~UnwindOnExit() { ids.unwind(); }
};

}  // namespace

TEST_CASE("store::withPassTraceId") {
    const std::string id = "0123456789abcdef0123456789abcdef";
    const std::string tidMember = "\"tid\":\"" + id + "\"";

    SECTION("empty trace becomes an object with only the tid") {
        auto text = withPassTraceId("", id);
        REQUIRE(text == "{" + tidMember + "}");
        auto value = json::parse(text);
        REQUIRE_NO_ERROR(value);
        REQUIRE(value->isObject());
        REQUIRE((*value)["tid"].asString() == id);
    }

    SECTION("null trace becomes an object with only the tid") {
        auto text = withPassTraceId("null", id);
        REQUIRE(text == "{" + tidMember + "}");
        REQUIRE_NO_ERROR(json::parse(text));
    }

    SECTION("empty object gets the tid as its only member") {
        auto text = withPassTraceId("{}", id);
        REQUIRE(text == "{" + tidMember + "}");
        REQUIRE_NO_ERROR(json::parse(text));
    }

    SECTION("object keeps its members, tid first") {
        auto text = withPassTraceId(R"({"lane":"text","result":"continue"})",
                                    id);
        REQUIRE(text ==
                "{" + tidMember + R"(,"lane":"text","result":"continue"})");
        auto value = json::parse(text);
        REQUIRE_NO_ERROR(value);
        REQUIRE((*value)["tid"].asString() == id);
        REQUIRE((*value)["lane"].asString() == "text");
        REQUIRE((*value)["result"].asString() == "continue");
    }

    SECTION("serialized object with whitespace stays valid") {
        auto text = withPassTraceId("{ }", id);
        REQUIRE(text.rfind("{" + tidMember, 0) == 0);
        REQUIRE_NO_ERROR(json::parse(text));

        text = withPassTraceId("{\n  \"lane\": \"text\"\n}", id);
        REQUIRE(text.rfind("{" + tidMember + ",", 0) == 0);
        auto value = json::parse(text);
        REQUIRE_NO_ERROR(value);
        REQUIRE((*value)["lane"].asString() == "text");
    }

    SECTION("a value that is not an object is returned unchanged") {
        REQUIRE(withPassTraceId("[1,2]", id) == "[1,2]");
        REQUIRE(withPassTraceId("\"text\"", id) == "\"text\"");
        REQUIRE(withPassTraceId("42", id) == "42");
    }

    SECTION("no pass id: the trace is unchanged") {
        REQUIRE(withPassTraceId("", "") == "");
        REQUIRE(withPassTraceId("null", "") == "null");
        REQUIRE(withPassTraceId(R"({"lane":"text"})", "") ==
                R"({"lane":"text"})");
    }
}

TEST_CASE("store::PassIds") {
    SECTION("two passes get different ids") {
        PassIds ids;
        REQUIRE(ids.current().empty());

        auto first = ids.begin();
        REQUIRE(isTraceId(first));
        REQUIRE(ids.current() == first);
        REQUIRE(ids.end() == first);
        REQUIRE(ids.level() == 0);
        REQUIRE(ids.current().empty());

        auto second = ids.begin();
        REQUIRE(isTraceId(second));
        REQUIRE(second != first);
        REQUIRE(ids.end() == second);
        REQUIRE(ids.current().empty());
    }

    SECTION("nested enter/leave keeps the outer pass's id") {
        PassIds ids;
        auto outer = ids.begin();
        REQUIRE(ids.begin() == outer);
        REQUIRE(ids.begin() == outer);
        REQUIRE(ids.level() == 3);
        REQUIRE(ids.end() == outer);
        REQUIRE(ids.end() == outer);
        REQUIRE(ids.current() == outer);
        REQUIRE(ids.level() == 1);
        REQUIRE(ids.end() == outer);
        REQUIRE(ids.level() == 0);
        REQUIRE(ids.current().empty());
    }

    SECTION("an exception between enter and leave ends the pass") {
        PassIds ids;
        std::string first;
        try {
            first = ids.begin();
            UnwindOnExit scope{ids};
            throw std::runtime_error("node failed");
        } catch (const std::runtime_error &) {
        }
        REQUIRE(isTraceId(first));
        REQUIRE(ids.level() == 0);
        REQUIRE(ids.current().empty());

        auto next = ids.begin();
        REQUIRE(isTraceId(next));
        REQUIRE(next != first);
        REQUIRE(ids.end() == next);
    }

    SECTION("an exception in a nested level keeps the outer pass") {
        PassIds ids;
        auto outer = ids.begin();
        try {
            ids.begin();
            UnwindOnExit scope{ids};
            throw std::runtime_error("node failed");
        } catch (const std::runtime_error &) {
        }
        REQUIRE(ids.level() == 1);
        REQUIRE(ids.current() == outer);
        REQUIRE(ids.end() == outer);
        REQUIRE(ids.level() == 0);
    }

    SECTION("a level never goes below zero") {
        PassIds ids;
        ids.unwind();
        REQUIRE(ids.end().empty());
        REQUIRE(ids.level() == 0);
        auto id = ids.begin();
        REQUIRE(isTraceId(id));
        REQUIRE(ids.level() == 1);
        REQUIRE(ids.end() == id);
    }

    SECTION("off (no id, as the Debugger passes it): the trace text is unchanged") {
        REQUIRE(withPassTraceId(R"({"lane":"text"})", std::string{}) ==
                R"({"lane":"text"})");
        REQUIRE(withPassTraceId("", std::string{}).empty());
    }
}
