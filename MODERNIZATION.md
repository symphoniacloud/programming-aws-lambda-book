# Modernization Summary for Programming AWS Lambda Examples

## Completed Work (November 2025)

### Test Harness
- Created `test-harness/` with deploy, test, and teardown scripts
- Smoke tests for chapters 2, 3, 4, 5-api, 5-data-pipeline, 5-event-sources, 7
- Integration tests for chapter 6 (Maven failsafe)
- GitHub Actions workflow (requires IAM permissions - see below)

### Java 21 Runtime
- All `pom.xml` files updated to Java 21
- All `template.yaml` files updated to `java21` runtime
- Applied `var` keyword for cleaner code in chapters 3, 5-api, 6, 7

### Security Updates
- Jackson 2.17.2 (fixed CVEs from 2.10.1)
- Log4j 2.23.1

### AWS SDK v2 Migration
All chapters migrated from AWS SDK v1 to v2:
- DynamoDB (chapters 4, 5-api, 7)
- S3, SNS (chapters 5-data-pipeline, 6)
- CloudFormation, CloudWatch Logs (chapter 6 integration tests)
- X-Ray SDK v2 instrumentor (chapter 7)

### Testing Modernization
- JUnit 5 (Jupiter 5.10.3)
- Mockito 5.12.0
- system-stubs-jupiter 2.1.6 (replaced system-rules for Java 9+ compatibility)
- S3Event tests build events programmatically

---

## Current Dependency Versions

```xml
<properties>
    <!-- Java -->
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>

    <!-- AWS SDK v2 -->
    <aws.sdk.version>2.29.6</aws.sdk.version>

    <!-- Lambda Libraries -->
    <aws.lambda.java.core.version>1.2.3</aws.lambda.java.core.version>
    <aws.lambda.java.events.version>3.14.0</aws.lambda.java.events.version>

    <!-- Logging -->
    <log4j.version>2.23.1</log4j.version>

    <!-- JSON -->
    <jackson.version>2.17.2</jackson.version>

    <!-- Testing -->
    <junit.version>5.10.3</junit.version>
    <mockito.version>5.12.0</mockito.version>
    <system-stubs.version>2.1.6</system-stubs.version>

    <!-- X-Ray SDK v2 -->
    <xray.version>2.18.1</xray.version>
</properties>
```

---

## Future Improvements

### Lambda SnapStart
Add to template.yaml for fast cold starts:
```yaml
Globals:
  Function:
    SnapStart:
      ApplyOn: PublishedVersions
```
Requires `AutoPublishAlias` on each function.

### ARM64 Architecture
20% better price/performance:
```yaml
Architectures:
  - arm64
```

### AWS Lambda Powertools
Replace manual logging/metrics/tracing (chapter 7):
```xml
<dependency>
    <groupId>software.amazon.lambda</groupId>
    <artifactId>powertools-tracing</artifactId>
    <version>2.0.0</version>
</dependency>
```

### SAM Template Improvements
- Use `Globals` section for DRY configuration
- Add structured logging: `LoggingConfig: { LogFormat: JSON }`

---

## Test Harness Usage

### Local Testing
```bash
# Deploy all
./test-harness/deploy/deploy-all.sh

# Test all
./test-harness/test/test-all.sh

# Cleanup
./test-harness/teardown/teardown-all.sh
```

### GitHub Actions
Requires IAM permissions on the GitHub Actions role:
- `iam:CreateRole`, `iam:DeleteRole`, `iam:GetRole`, `iam:PassRole`
- `iam:AttachRolePolicy`, `iam:DetachRolePolicy`
- `iam:PutRolePolicy`, `iam:DeleteRolePolicy`
- `iam:TagRole`, `iam:UntagRole`

Scope to: `arn:aws:iam::ACCOUNT:role/lb-test-*`
