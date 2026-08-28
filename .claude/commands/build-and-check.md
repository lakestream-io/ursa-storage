Run the full pre-PR quality gate. Execute each step sequentially and stop on first failure:

1. Check license headers:
```bash
mvn -B -ntp license:check
```

2. Check code style:
```bash
mvn -B -ntp checkstyle:check
```

3. Build all modules (skip tests):
```bash
mvn -B -ntp clean install -DskipTests
```

4. Run SpotBugs static analysis:
```bash
mvn -B -ntp spotbugs:check
```

Report the results of each step. If any step fails, show the relevant error output and suggest fixes.
