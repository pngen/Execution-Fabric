#include "execution_fabric/state.hpp"
#include "execution_fabric/sha256.hpp"
#include "execution_fabric/digest.hpp"
#include "execution_fabric/state_machine.hpp"
#include "test_util.hpp"

using namespace execution_fabric;

int main() {
    CHECK(ExecutionStateMachine::from(ExecutionState::CREATED, ExecutionState::READY));
    CHECK(ExecutionStateMachine::from(ExecutionState::READY, ExecutionState::DISPATCHED));
    CHECK(ExecutionStateMachine::from(ExecutionState::DISPATCHED, ExecutionState::RUNNING));
    CHECK(ExecutionStateMachine::from(ExecutionState::RUNNING, ExecutionState::COMPLETED));
    CHECK(ExecutionStateMachine::from(ExecutionState::COMPLETED, ExecutionState::COMMITTED));
    CHECK(ExecutionStateMachine::from(ExecutionState::RUNNING, ExecutionState::AMBIGUOUS));
    CHECK(ExecutionStateMachine::from(ExecutionState::AMBIGUOUS, ExecutionState::DISPATCHED));
    CHECK(!ExecutionStateMachine::from(ExecutionState::CREATED, ExecutionState::RUNNING));
    CHECK(!ExecutionStateMachine::from(ExecutionState::COMMITTED, ExecutionState::RUNNING));
    CHECK(!ExecutionStateMachine::from(ExecutionState::FAILED, ExecutionState::READY));
    CHECK(ExecutionStateMachine::is_terminal(ExecutionState::COMMITTED));
    CHECK(AttemptStateMachine::from(AttemptState::RUNNING, AttemptState::LOST));
    CHECK(AttemptStateMachine::from(AttemptState::RUNNING, AttemptState::PREEMPTED));
    CHECK(!AttemptStateMachine::from(AttemptState::COMPLETED, AttemptState::RUNNING));
    const char* n = to_string(ExecutionState::AMBIGUOUS);
    CHECK(n != nullptr);
    return TEST_MAIN_RETURN();
}