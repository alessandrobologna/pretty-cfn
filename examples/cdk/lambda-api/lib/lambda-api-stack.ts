import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as path from 'path';

export class LambdaApiStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // DynamoDB table for storing data
    const counterTable = new dynamodb.Table(this, 'CounterTable', {
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Log group for Lambda function
    const helloFunctionLogGroup = new logs.LogGroup(this, 'HelloFunctionLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const pythonHelloFunctionLogGroup = new logs.LogGroup(this, 'PythonHelloFunctionLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Inline Lambda functions
    const helloFunction = new lambda.Function(this, 'HelloFunction', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          console.log('Event:', JSON.stringify(event));
          return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Hello World!' })
          };
        };
      `),
      timeout: cdk.Duration.seconds(30),
      environment: {
        TABLE_NAME: counterTable.tableName,
      },
      logGroup: helloFunctionLogGroup,
    });

    // Grant permissions to Lambda to access DynamoDB
    counterTable.grantReadWriteData(helloFunction);

    const pythonHelloFunction = new lambda.Function(this, 'PythonHelloFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
import json

def handler(event, context):
    print("Event:", json.dumps(event))
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Hello from Python!"})
    }
      `),
      timeout: cdk.Duration.seconds(30),
      environment: {
        TABLE_NAME: counterTable.tableName,
      },
      logGroup: pythonHelloFunctionLogGroup,
    });

    counterTable.grantReadWriteData(pythonHelloFunction);

    // Asset-based Lambda function with Function URL
    const assetFunction = new lambda.Function(this, 'AssetFunction', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda')),
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
    });

    const assetFunctionUrl = assetFunction.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      cors: {
        allowedOrigins: ['*'],
        allowedMethods: [lambda.HttpMethod.GET],
      },
    });

    // API Gateway REST API
    const api = new apigateway.RestApi(this, 'ApiGateway', {
      restApiName: 'HelloApi',
      deployOptions: {
        stageName: 'prod',
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
      },
    });

    // Lambda integrations
    const lambdaIntegration = new apigateway.LambdaIntegration(helloFunction);
    const pythonIntegration = new apigateway.LambdaIntegration(pythonHelloFunction);

    // Add GET method to root
    api.root.addMethod('GET', lambdaIntegration);

    // Add Python-specific route
    const pythonResource = api.root.addResource('python');
    pythonResource.addMethod('GET', pythonIntegration);

    // Add proxy resource for all other paths
    const proxyResource = api.root.addResource('{proxy+}');
    proxyResource.addMethod('ANY', lambdaIntegration);

    // Output the API URL
    new cdk.CfnOutput(this, 'ApiUrl', {
      value: api.url,
    });

    // Output the Function URL
    new cdk.CfnOutput(this, 'AssetFunctionUrl', {
      value: assetFunctionUrl.url,
    });
  }
}
