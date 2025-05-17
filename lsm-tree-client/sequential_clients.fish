#!/usr/bin/env fish

set NUM_CLIENTS 200

for i in (seq 0 (math $NUM_CLIENTS - 1))
    ./generator/generator --puts 500000 --external-puts --seed $i;
    echo "l 0.dat" | ./target/release/lsm-tree-client --cli
end

wait