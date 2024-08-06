{
  description = "developing environment of Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { 
	  inherit system;
	};
      in {
        devShells = {
          default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              libiconv
              darwin.apple_sdk.frameworks.Security
              minio-client
              python312Packages.boto3
	      rustup
            ];
            shellHook = ''
              exec $SHELL
            '';
          };
        };
      }
    );
}
