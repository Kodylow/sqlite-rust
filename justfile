# Set the local rustup toolchain to 1.70.0 for this project
rust: 
  rustup install 1.70 && rustup default 1.70 && rustc --version && cargo --version

