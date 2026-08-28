Auto-fix license headers and identify checkstyle violations.

1. Fix missing license headers:
```bash
mvn -B -ntp license:format
```

2. Run checkstyle and capture violations:
```bash
mvn -B -ntp checkstyle:check 2>&1 || true
```

3. If checkstyle violations found, parse the output and fix each violation:
   - **Missing package-info.java**: Create the file with license header and package declaration
   - **Star imports**: Replace with specific imports
   - **Import ordering**: Move static imports first, sort alphabetically
   - **Line too long**: Break lines at 120 characters
   - **Non-static Preconditions import**: Change to `import static com.google.common.base.Preconditions.checkArgument`

4. Re-run checkstyle to verify fixes:
```bash
mvn -B -ntp checkstyle:check
```
