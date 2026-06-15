set -e

cargo publish --package elefant-client-macros --allow-dirty
cargo publish --package elefant-client --allow-dirty
cargo publish --package elefant-tools --allow-dirty
cargo publish --package elefant-sync --allow-dirty