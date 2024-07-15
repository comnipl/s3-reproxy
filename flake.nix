{
  description = "Sutera developing environment of Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        rust-beta = pkgs.rust-bin.beta.latest.default;
        rust-nightly = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default);
      in {
        devShells = {
          default = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.libiconv
              rust
            ];
            shellHook = ''
              exec $SHELL
            '';
          };

          beta = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.libiconv
              rust-beta
            ];
            shellHook = ''
              exec $SHELL
            '';
          };

          nightly = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.libiconv
              rust-nightly
            ];
            shellHook = ''
              exec $SHELL
            '';
          };
        };
      }
    );
}
