# Pretty CFN Examples

This directory contains examples demonstrating the capabilities of the `pretty-cfn` formatter, organized by template type.

## Directory Structure

```
examples/
├── cfn/                    # CloudFormation templates
├── sam/                    # AWS SAM templates
└── cdk/                    # CDK-generated templates
```

## CloudFormation Examples (`/cfn`)

Standard CloudFormation YAML templates demonstrating various formatting scenarios.

### basic-template
A simple S3 bucket template demonstrating basic formatting of parameters, resources, and outputs.

**Key features:**
- Parameter alignment
- Resource property formatting
- Output formatting

### iam-policies
IAM roles and policies with complex conditions.

**Key features:**
- Proper handling of `aws:SourceIp` and other keys with colons
- Complex IAM policy documents
- Nested conditions

### event-sources
Lambda wired to SQS, Kinesis, self-managed Kafka, and S3 notifications.

**Key features:**
- EventSourceMapping folding across SQS, Kinesis, and self-managed Kafka
- Bucket notifications and paired Lambda permissions
- Ensures samify covers common event trigger shapes

### intrinsic-functions
Demonstrates all CloudFormation intrinsic functions.

**Key features:**
- Preserves `!Ref`, `!GetAtt`, `!Sub`, `!Join`, `!If`, etc.
- Conditions and mappings
- Complex function combinations

### parameters-outputs
Comprehensive parameter types and output configurations.

**Key features:**
- All parameter types and constraints
- Metadata section
- Export values in outputs
- Parameter groups

### sam-refactor-cases
Examples demonstrating CloudFormation to SAM conversion patterns for various AWS services.

**Key features:**
- Side-by-side CloudFormation and SAM examples
- DocumentDB events, Cognito triggers, IoT rules, and API Gateway mappings
- Shows how traditional resources can be expressed using SAM constructs
- Covers Complex event source configurations and permissions

### vpc-network
Complete VPC setup with public and private subnets across two availability zones.

**Key features:**
- Multi-AZ VPC architecture
- NAT gateways for private subnet internet access
- Route tables and associations
- Export values for cross-stack references

## SAM Examples (`/sam`)

AWS Serverless Application Model (SAM) templates with serverless resources.

### hello-world
A simple serverless API with Lambda and DynamoDB.

**Key features:**
- SAM Transform declaration
- AWS::Serverless resource types
- API Gateway events
- DynamoDB Simple Table
- SAM policy templates

## CDK Examples (`/cdk`)

CDK-synthesized CloudFormation templates demonstrating CDK cleaning capabilities.

### appsync-dynamodb
GraphQL API with DynamoDB resolver functions and JS pipeline resolvers.

**Key features:**
- AppSync schema and resolvers
- Multiple assets staged from CDK
- IAM roles and policies for DynamoDB access

### lambda-api
Lambda functions with API Gateway REST API and a Lambda Function URL.

**Key features:**
- Lambda function with IAM role and policies
- API Gateway with multiple methods and resources
- DynamoDB table for state storage
- Custom log retention resources
- Complex permission boundaries
 - Asset-packaged Lambda function
 - Function URL + CORS folding

### s3-cloudfront
Static website hosting with S3 and CloudFront.

**Key features:**
- S3 bucket with website configuration
- CloudFront distribution with Origin Access Control
- WAF WebACL with rate limiting and geo-blocking rules
- Separate logs bucket with lifecycle policies
- Security best practices (encryption, public access blocking)

### sam-refactor-showcase
Composite CDK app meant to exercise samify and cleaning end-to-end.

**Key features:**
- Demonstrates various SAM transformation patterns including:
  - LayerVersion consolidation and sharing
  - Complex DynamoDB grant configurations  
  - DocumentDB event source mappings with authentication
  - IoT topic rules connected to Lambda functions
  - Cognito UserPool trigger configurations using SAM refs
  - HTTP API Stage and mapping folding

### step-functions
Step Functions state machine for order processing workflow.

**Key features:**
- Multiple Lambda functions orchestrated by Step Functions
- Choice states and error handling
- SNS topic for notifications
- CloudWatch Logs integration
- IAM roles with fine-grained permissions

## Running the Examples

### Format a single example

```bash
# CloudFormation example
uv run pretty-cfn format --input examples/cfn/<example-name>/input.yaml

# SAM example
uv run pretty-cfn format --input examples/sam/<example-name>/input.yaml

# CDK example (with cleaning)
uv run pretty-cfn refactor --input examples/cdk/<example-name>/cdk.out/StackName.template.json \
  --target clean-cfn --output examples/cdk/<example-name>/output.yaml
```

### Regenerate all outputs

Use the dedicated Makefile in this directory to refresh every template:

```bash
make -C examples examples
```

Or run the equivalent loops manually:

```bash
# CloudFormation examples
for dir in examples/cfn/*/; do
  if [ -f "$dir/input.yaml" ]; then
    uv run pretty-cfn format --input "$dir/input.yaml" -o "$dir/output.yaml"
    echo "Formatted $(basename $dir)"
  fi
done

# SAM examples
for dir in examples/sam/*/; do
  if [ -f "$dir/input.yaml" ]; then
    uv run pretty-cfn format --input "$dir/input.yaml" -o "$dir/output.yaml"
    echo "Formatted $(basename $dir)"
  fi
done

# CDK examples (with cleaning)
for dir in examples/cdk/*/; do
  if [ -d "$dir/cdk.out" ]; then
    template=$(ls "$dir/cdk.out"/*.template.json 2>/dev/null | head -1)
    if [ -f "$template" ]; then
      uv run pretty-cfn refactor --input "$template" --target clean-cfn \
        -o "$dir/output.yaml"
      echo "Formatted and cleaned $(basename $dir)"
    fi
  fi
done
```

## Verifying Output

You can validate every example at once with:

```bash
make -C examples verify-examples
```

To check that a single formatted output is valid:

```bash
# Quick formatter validation (ensures idempotence and CFN parsing)
uv run pretty-cfn format --input examples/<type>/<example-name>/output.yaml --check

# Validate with a CFN-aware YAML loader (handles !Ref, !GetAtt, etc.)
uv run python - <<'PY'
from pretty_cfn.formatter import CFNLoader
import yaml
path = 'examples/<type>/<example-name>/output.yaml'
with open(path) as f:
    yaml.load(f.read(), Loader=CFNLoader)
print('CFN YAML parse ok:', path)
PY
```

## Features Demonstrated

### Core Formatting
1. **Value Alignment**: All values are aligned to column 40 by default
2. **Section Spacing**: Automatic spacing between major sections
3. **Resource Comments**: Each resource in the Resources section gets a comment header
4. **Intrinsic Function Preservation**: All CloudFormation functions remain intact
5. **Complex Key Handling**: Keys with colons (like `aws:SourceIp`) are handled correctly
6. **Multi-line Strings**: Preserved and properly formatted
7. **List Formatting**: Lists maintain their structure and alignment

### Template Type Support
1. **CloudFormation**: Standard CFN templates with all intrinsic functions
2. **SAM**: Serverless resources with Transform declaration
3. **CDK**: Synthesized templates with metadata-aware cleaning

### CDK Cleaning Features
1. **Hash Removal**: Strips 8-character hex suffixes from logical IDs
2. **Metadata Integration**: Uses CDK's manifest.json for perfect renaming
3. **Semantic Naming**: Applies intelligent patterns to resource names
4. **Reference Updates**: Maintains template integrity after renaming

## Development Tips

- Use uv for linting and formatting:

```bash
uv run ruff examples pretty_cfn tests
uv run black examples pretty_cfn tests
```

- Test new examples for validity:

```bash
# Format and check
uv run pretty-cfn examples/<type>/<new-example>/input.yaml --check

# Validate CloudFormation syntax
aws cloudformation validate-template \
  --template-body file://examples/<type>/<new-example>/output.yaml
```
