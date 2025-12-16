{
  outputs = { self, nixpkgs }: {
    devShells.x86_64-linux.default = let
      pkgs = import nixpkgs { system = "x86_64-linux"; };
    in pkgs.mkShell {
      # 这里列出开发所需的包
      buildInputs = [ (pkgs.scala_2_12.override {jre = pkgs.jdk11;}) pkgs.sbt pkgs.jdk11 pkgs.protobuf_25];
      
      shellHook = ''
        echo "Scala 开发环境已就绪！"
      '';
    };
  };
}
