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
      in {
        devShells = {
          default = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.libiconv
              pkgs.darwin.apple_sdk.frameworks.Security
              pkgs.minio-client
              pkgs.python312Packages.boto3
              rust
            ];
            shellHook = ''
              exec $SHELL
            '';
          };

        };
      }
    );
}
