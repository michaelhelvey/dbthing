default:
    rm -rf ./data
    mkdir -p data/databases
    zig build
    ./zig-out/bin/db --data-dir=./data

test:
    zig build test
