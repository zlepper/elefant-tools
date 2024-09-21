{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
    # nativeBuildInputs is usually what you want -- tools you need to run
    packages = [
        pkgs.postgresql_16
        pkgs.hyperfine
    ];

    shellHook = ''
        export ELEFANT_SYNC_PATH=./target/release/elefant-sync
    '';
}
