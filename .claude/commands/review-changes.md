Review current changes against architecture guardrails.

1. Show what's changed:
```bash
git diff --stat
git diff --name-only
```

2. For each changed file, check:
   - **License header**: Present and matches `resources/license.template`
   - **Package-info.java**: Exists for any new packages
   - **Import style**: Static imports first, no star imports, Preconditions static
   - **Test naming**: Uses `*Test.java` not `*Tests.java`
   - **No System.out/System.err**: Uses SLF4J instead
   - **Line length**: Max 120 characters
   - **No TODO(username)**: Just `TODO`

3. Check module boundaries:
   - Lakehouse: Iceberg and Delta packages don't cross-reference
   - Lakehouse: New code goes in `v2/` packages, not root legacy packages
   - Vendor code (`io.delta.kernel`, `org.apache.iceberg`): Not modified unless intentional
   - Dependencies: Use BOM versions, don't hardcode

4. Run quality gates:
```bash
mvn -B -ntp license:check && mvn -B -ntp checkstyle:check
```

5. Report findings with specific file:line references and suggested fixes.
