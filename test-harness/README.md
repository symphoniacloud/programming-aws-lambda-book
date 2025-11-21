# Test Harness for Programming AWS Lambda Examples

This test harness deploys and tests all chapter examples to ensure they work correctly with AWS Lambda.

## Prerequisites

- AWS CLI configured with appropriate credentials
- SAM CLI installed
- Maven 3.6.0+
- Java 8 (for current examples)

## Directory Structure

```
test-harness/
├── config.sh              # Shared configuration
├── deploy/
│   ├── deploy-stack.sh    # Deploy single chapter
│   └── deploy-all.sh      # Deploy all chapters
├── test/
│   ├── test-stack.sh      # Test single chapter
│   ├── test-all.sh        # Test all chapters
│   └── tests/             # Per-chapter test scripts
└── teardown/
    ├── teardown-stack.sh  # Delete single stack
    └── teardown-all.sh    # Delete all stacks
```

## Usage

### Deploy a single chapter

```bash
./test-harness/deploy/deploy-stack.sh chapter2
```

### Deploy all chapters

```bash
./test-harness/deploy/deploy-all.sh
```

### Run tests for a single chapter

```bash
./test-harness/test/test-stack.sh chapter5-api
```

### Run all tests

```bash
./test-harness/test/test-all.sh
```

### Cleanup

```bash
# Single stack
./test-harness/teardown/teardown-stack.sh chapter2

# All stacks
./test-harness/teardown/teardown-all.sh
```

## Stack Naming

Stacks are named using the pattern: `lambda-book-test-{run-id}-{chapter}`

- In CI: Uses `GITHUB_RUN_ID`
- Locally: Uses `local`

Example: `lambda-book-test-local-chapter5-api`

## Chapters Tested

| Chapter | Description | Test Type |
|---------|-------------|-----------|
| chapter2 | Hello World | Lambda invoke |
| chapter3 | Environment Variables | Lambda invoke |
| chapter4 | DynamoDB | Lambda invoke |
| chapter5-api | Weather Events API | HTTP POST/GET |
| chapter5-event-sources | API Gateway | HTTP GET |

## GitHub Actions

The workflow in `.github/workflows/integration-test.yml` runs on:
- Push to `main` or `feature/**` branches
- Pull requests to `main`
- Manual trigger (workflow_dispatch)

It deploys all stacks, runs tests, and always tears down (even on failure).

## Adding Tests for New Chapters

1. Create a test script in `test-harness/test/tests/{chapter-name}.sh`
2. Add the chapter to the `CHAPTERS` array in `config.sh`

## Troubleshooting

### Stack deletion fails
Some stacks have S3 buckets that must be emptied before deletion. The teardown script handles this automatically.

### Test times out
CloudFormation deployments can take 5-10 minutes. If tests fail, check that the stack deployed successfully in the AWS Console.

### API Gateway 403 errors
Ensure the API Gateway stage is deployed. SAM deploy should handle this automatically.
