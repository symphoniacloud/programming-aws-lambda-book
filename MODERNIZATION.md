# Modernization Plan for Programming AWS Lambda Examples

## Completed Work

### Test Harness (Done)
- Created `test-harness/` with deploy, test, and teardown scripts
- Smoke tests for chapters 2, 3, 4, 5-api, 5-event-sources, 7
- Integration tests for chapter 6 (Maven failsafe)
- GitHub Actions workflow (needs IAM permissions to run)
- All 9 chapters deploy successfully with `java8.al2` runtime

### Immediate Fixes Applied
- Updated runtime from `java8` (deprecated) to `java8.al2`
- Shortened stack prefixes for S3 bucket name limits

### Phase 1 Complete (November 2025)
- **Java 21 Runtime**: All pom.xml and template.yaml files updated
- **Security Updates**: Jackson 2.17.2, Log4j 2.23.1
- **AWS SDK v2 Migration**: All chapters migrated to SDK v2
  - DynamoDB (chapters 4, 5-api, 7)
  - S3, SNS (chapters 5-data-pipeline, 6)
  - CloudFormation, CloudWatch Logs (chapter 6 integration tests)
  - X-Ray SDK v2 instrumentor (chapter 7)

### Phase 3 Partial (November 2025)
- **JUnit 5 Migration**: Chapter 6 tests migrated to JUnit Jupiter 5.10.3
- **system-stubs**: Replaced system-rules with system-stubs-jupiter 2.1.6
- **Mockito 5.12.0**: Updated for Java 21 compatibility
- **S3Event tests**: Fixed to build events programmatically (no Jackson deserialization)

### Known Issues (Resolved)
- **Chapter 6 integration tests**: Fixed stack name passing to Maven failsafe plugin
  - Root cause: Maven property `${integration.test.stack.name}` resolves at project load time, before `-D` override
  - Solution: Use shell environment variable `${env.STACK_NAME}` instead of Maven property
  - Test harness exports `STACK_NAME`, failsafe passes it via `environmentVariables`, test reads `System.getenv()`

---

## Phase 1: Critical Updates (Do First)

### 1. Java 21 Runtime
**Files to change:**
- All `pom.xml` files: Update `maven.compiler.source/target` from `1.8` to `21`
- All `template.yaml` files: Update `Runtime` from `java8.al2` to `java21`

### 2. Security Updates
**jackson-databind** (currently 2.10.1 - has CVEs)
- Update to `2.17.2` in all pom.xml files
- Locations: chapter5-api, chapter5-data-pipeline, chapter6, chapter7

**log4j** (currently 2.17.0)
- Update to `2.23.1` in chapter6, chapter7

### 3. AWS SDK v2 Migration (End-of-support Dec 2025)
**Largest effort - affects all chapters using AWS services**

Package changes:
- `com.amazonaws:aws-java-sdk-*` → `software.amazon.awssdk:*`
- BOM: `1.11.600` → `2.29.x`

Code changes required:
- DynamoDB client API changes (chapters 4, 5-api, 7)
- S3 client API changes (chapters 5-data-pipeline, 6, 8)
- SNS client API changes (chapters 5-data-pipeline, 6)
- CloudFormation/Logs client changes (chapter 6 integration tests)

**Note:** `aws-lambda-java-core` and `aws-lambda-java-events` are separate artifacts and remain compatible.

---

## Phase 2: High Value Improvements

### 4. Lambda SnapStart
Add to template.yaml for fast cold starts:
```yaml
Globals:
  Function:
    SnapStart:
      ApplyOn: PublishedVersions
```
Requires `AutoPublishAlias` on each function.

### 5. ARM64 Architecture
20% better price/performance:
```yaml
Architectures:
  - arm64
```

### 6. AWS Lambda Powertools
Replace manual logging/metrics/tracing (chapter 7):
```xml
<dependency>
    <groupId>software.amazon.lambda</groupId>
    <artifactId>powertools-tracing</artifactId>
    <version>2.0.0</version>
</dependency>
```

---

## Phase 3: Testing & Code Quality

### 7. JUnit 5 Migration
- `junit:4.12` → `junit-jupiter:5.10.3`
- `mockito-core:3.0.0` → `mockito-core:5.12.0`
- Affects chapter 6 test code

### 8. Replace system-rules
**Critical for Java 9+ compatibility**
- `system-rules:1.19.0` → `system-stubs-jupiter:2.1.6`
- Used in chapter 6 bulk-events-stage and single-event-stage tests
- Currently skipped in test harness due to reflection errors

### 9. Java 21 Language Features
Modernize code with:
- Records for DTOs (WeatherEvent, etc.)
- Pattern matching for instanceof
- Text blocks for JSON templates

---

## Phase 4: Nice to Have

### 10. SAM Template Improvements
- Use `Globals` section for DRY configuration
- Consider SAM Connectors syntax
- Add structured logging config:
```yaml
LoggingConfig:
  LogFormat: JSON
```

### 11. Build Improvements
- Consider maven-shade-plugin consistently (simpler than assembly)
- Update maven plugins to latest versions

### 12. Additional AWS Features
- Function URLs (alternative to API Gateway for simple cases)
- Response streaming (for long-running responses)

---

## Recommended Dependency Versions

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

    <!-- Powertools -->
    <powertools.version>2.0.0</powertools.version>

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

## Implementation Order

1. **Start with chapter 2** (simplest - no dependencies)
   - Update Java version
   - Update runtime
   - Test with harness

2. **Then chapter 3** (adds aws-lambda-java-core)

3. **Then chapter 4** (adds DynamoDB - first SDK v2 migration)

4. **Then chapter 5-api** (DynamoDB + Jackson + API Gateway)

5. **Continue with remaining chapters**

6. **Chapter 6 last** (most complex - has tests that need JUnit 5 migration)

---

## Test Harness Notes

### Known Issues
- Chapter 6 integration test uses old CloudWatch logs (may pass when it should fail)
- Unit tests skipped due to system-rules incompatibility with Java 9+

### GitHub Actions
Requires IAM permissions on the GitHub Actions role:
- `iam:CreateRole`, `iam:DeleteRole`, `iam:GetRole`, `iam:PassRole`
- `iam:AttachRolePolicy`, `iam:DetachRolePolicy`
- `iam:PutRolePolicy`, `iam:DeleteRolePolicy`
- `iam:TagRole`, `iam:UntagRole`

Scope to: `arn:aws:iam::ACCOUNT:role/lb-test-*`

### Local Testing
```bash
# Deploy all
./test-harness/deploy/deploy-all.sh

# Test all
./test-harness/test/test-all.sh

# Cleanup
./test-harness/teardown/teardown-all.sh
```
