#!/usr/bin/env fish

set NUM_CLIENTS 16

for i in (seq 0 (math $NUM_CLIENTS - 1))
    ./generator/generator --deletes 1000000 | ./target/release/lsm-tree-client --cli &
end

wait