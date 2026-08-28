Run tests for a specific module. Usage: /test-module <module-name>

The argument $ARGUMENTS should be a module name like `ursa-storage-core` or `ursa-storage-lakehouse`.

Run the tests:
```bash
mvn -B -ntp test -pl $ARGUMENTS
```

If the module is `ursa-storage-lakehouse`, add the test group:
```bash
mvn -B -ntp test -pl ursa-storage-lakehouse -Dgroups=lakehouse
```

If the module is `ursa-storage-test`, ask which test group to run:
- untagged — framing and lakehouse ingestion tests
- `docker` — S3 and GCS backend integration tests

Then run:
```bash
mvn -B -ntp test -pl ursa-storage-test -Dgroups=<selected-group>
```

After tests complete, if there are failures, read the surefire report:
```bash
find $ARGUMENTS/target/surefire-reports -name "*.txt" -exec grep -l "FAILURE\|ERROR" {} \;
```
Then read the relevant report files to diagnose failures.
