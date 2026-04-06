#!/bin/bash

if [ -n "${BASH_VERSION:-}" ]; then
    _quant_stack_source="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION:-}" ]; then
    _quant_stack_source="${(%):-%N}"
else
    _quant_stack_source="$0"
fi

STACK_DIR="$(cd "$(dirname "$_quant_stack_source")" && pwd)"
unset _quant_stack_source

export QUANT_STACK_ROOT="$STACK_DIR"
export FACTOR_LAB_ROOT="${FACTOR_LAB_ROOT:-$STACK_DIR/factor-lab}"
export QUANT_CN_ROOT="${QUANT_CN_ROOT:-$STACK_DIR/quant-research-cn}"
export QUANT_US_ROOT="${QUANT_US_ROOT:-$STACK_DIR/quant-research-v1}"
export PYTHON_BIN="${PYTHON_BIN:-python3}"
export FACTOR_LAB_AGENT_BACKEND="${FACTOR_LAB_AGENT_BACKEND:-codex}"
export FACTOR_LAB_CODEX_MODEL="${FACTOR_LAB_CODEX_MODEL:-gpt-5.4}"
export FACTOR_LAB_CODEX_REASONING_EFFORT="${FACTOR_LAB_CODEX_REASONING_EFFORT:-xhigh}"
