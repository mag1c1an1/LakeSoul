meta-cleanup:
     PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_cleanup.sql lakesoul_test

spark-test-1:
    -mvn -q -B test -pl lakesoul-spark \
        -am -Pcross-build -Pparallel-test --file pom.xml \
        -Dtest='!ANNCase,UpdateScalaSuite,AlterTableByNameSuite,ReadSuite,UpdateSQLSuite,ParquetNativeFilterSuite,DeleteScalaSuite,DeleteSQLSuite,ParquetV2FilterSuite,ParquetScanSuite,UpsertSuiteBase' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        -Dlog4j.configurationFile=file:./lakesoul-spark/src/test/resources/log4j2-test.properties
    mvn -q surefire-report:report-only -pl lakesoul-spark -am

spark-test-2:
    -mvn -q -B test -pl lakesoul-spark \
        -am -Pcross-build -Pparallel-test --file pom.xml \
        -Dtest='!ANNCase,!UpdateScalaSuite,!AlterTableByNameSuite,!ReadSuite,!UpdateSQLSuite,!ParquetNativeFilterSuite,!DeleteScalaSuite,!DeleteSQLSuite,!ParquetV2FilterSuite,!ParquetScanSuite,!UpsertSuiteBase,!RBACOperationSuite,!DeltaJoinSuite' \
        -Dsurefire.failIfNoSpecifiedTests=false
    mvn -q surefire-report:report-only -pl lakesoul-spark -am

flink-test-1:
    -MAVEN_OPTS="-Xmx5g" mvn -q -B test -pl lakesoul-flink \
        -am -Pcross-build --file pom.xml \
        -Dtest='!LakeSoulRBACTest' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        -Dlog4j.configurationFile=file:"$(pwd)/lakesoul-flink/src/test/resources/log4j2-test.properties" \
        -Dlog4j2.statusLoggerLevel=OFF
    mvn -q surefire-report:report-only -pl lakesoul-flink -am

spark-test-gluten:
    -MAVEN_OPTS="-Xmx4g -Dio.netty.tryReflectionSetAccessible=true" \
        mvn -q -B test -pl lakesoul-spark-gluten -am -Pgluten -Pcross-build --file pom.xml -Dtest='LakeSoulGlutenCompatSuite,UpdateGlutenTestSuite,UpsertGlutenTestSuite,DeleteSQLGlutenTestSuite,MergeIntoSQLGlutenTestSuite,' -Dsurefire.failIfNoSpecifiedTests=false
    mvn -q surefire-report:report-only -pl lakesoul-spark-gluten -am -Pgluten

cross:
    cross build --target x86_64-unknown-linux-gnu --release --package lakesoul-io-c -F hdfs
    cross build --target x86_64-unknown-linux-gnu --release  --package lakesoul-metadata-c --all-features
    cp rust/target/x86_64-unknown-linux-gnu/release/lib*.so rust/target/release

copy-to-java ext="dylib":
  cargo build -p lakesoul-io-c -p lakesoul-metadata-c
  mkdir -p lakesoul-common/target/classes/
  cp rust/target/debug/liblakesoul_io_c.{{ext}} lakesoul-common/target/classes/
  cp rust/target/debug/liblakesoul_metadata_c.{{ext}} lakesoul-common/target/classes/


dist:
    #!/usr/bin/env bash
    set -euo pipefail

    ROOT="$(pwd)"
    export CARGO_TARGET_DIR="$ROOT/rust/target"
    mkdir -p "$ROOT/dist"

    # Maturin and the standalone cargo-zigbuild binary generate different
    # linker-wrapper paths, which invalidates Cargo fingerprints. Intercept
    # Maturin's Cargo invocation and build the FFI libraries in the exact same
    # Zig/glibc 2.17 environment instead.
    REAL_CARGO="$(command -v cargo)"
    CARGO_WRAPPER_DIR="$(mktemp -d)"
    trap 'rm -rf "$CARGO_WRAPPER_DIR"' EXIT

    cat >"$CARGO_WRAPPER_DIR/cargo" <<'CARGO_WRAPPER'
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ "${1:-}" == "rustc" ]]; then
        "$LAKESOUL_REAL_CARGO" "$@"

        if [[ ! -e "$LAKESOUL_FFI_BUILD_MARKER" ]]; then
            echo "Building LakeSoul C FFI libraries with Maturin's Zig toolchain..." >&2
            "$LAKESOUL_REAL_CARGO" build \
                --profile release \
                --target x86_64-unknown-linux-gnu \
                --manifest-path "$LAKESOUL_ROOT/Cargo.toml" \
                --package lakesoul-io-c \
                --package lakesoul-metadata-c \
                --features lakesoul-io-c/hdfs \
                1>&2
            touch "$LAKESOUL_FFI_BUILD_MARKER"
        fi
        exit 0
    fi

    exec "$LAKESOUL_REAL_CARGO" "$@"
    CARGO_WRAPPER
    chmod +x "$CARGO_WRAPPER_DIR/cargo"

    export LAKESOUL_REAL_CARGO="$REAL_CARGO"
    export LAKESOUL_ROOT="$ROOT"
    export LAKESOUL_FFI_BUILD_MARKER="$CARGO_WRAPPER_DIR/ffi-built"
    export PATH="$CARGO_WRAPPER_DIR:$PATH"

    cd "$ROOT/python"
    uvx --from 'maturin[zig]==1.14.1' maturin build \
        --release \
        --zig \
        --target x86_64-unknown-linux-gnu \
        --auditwheel repair \
        --compatibility manylinux2014 \
        --out "$ROOT/dist"

    cp \
        "$CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release/liblakesoul_io_c.so" \
        "$CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release/liblakesoul_metadata_c.so" \
        "$ROOT/dist/"
