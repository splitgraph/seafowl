#!/bin/bash -e

# We have to run Clippy twice here: first, with --fix so that it can fix
# some issues automatically, then with -D warnings so that it can error out
# on detected issues. Running it at the same time isn't possible:
#
# https://users.rust-lang.org/t/pre-commit-clippy-fix/66584

cargo clippy --all-targets --workspace --fix --allow-dirty --allow-staged --allow-no-vcs
cargo clippy --all-targets --workspace -- -D warnings
