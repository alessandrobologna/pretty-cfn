import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iot from 'aws-cdk-lib/aws-iot';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as apigwv2 from 'aws-cdk-lib/aws-apigatewayv2';

export class SamRefactorShowcaseStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const commonLayer = new lambda.LayerVersion(this, 'CommonLayer', {
      code: lambda.Code.fromAsset('layer-code'),
      compatibleRuntimes: [lambda.Runtime.NODEJS_22_X],
      description: 'Shared utilities layer for SAM refactor showcases',
    });

    const ordersTable = new dynamodb.Table(this, 'OrdersTable', {
      partitionKey: { name: 'orderId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PROVISIONED,
      readCapacity: 5,
      writeCapacity: 5,
      pointInTimeRecovery: true,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
    });

    const ordersHandler = new lambda.Function(this, 'OrdersHandler', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(
        `exports.handler = async (event) => {
          console.log('event', JSON.stringify(event));
          return { statusCode: 200 };
        };`,
      ),
      environment: { TABLE_NAME: ordersTable.tableName },
      layers: [commonLayer],
    });
    ordersTable.grantReadWriteData(ordersHandler);

    const docdbHandler = new lambda.Function(this, 'DocDbStreamHandler', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromInline(
        "def handler(event, _context):\n    print(event)\n    return { 'statusCode': 200 }\n",
      ),
    });

    new lambda.CfnEventSourceMapping(this, 'DocDbEventSource', {
      functionName: docdbHandler.functionName,
      eventSourceArn: 'arn:aws:docdb:us-east-1:123456789012:cluster:orders',
      startingPosition: 'LATEST',
      documentDbEventSourceConfig: {
        databaseName: 'orders',
        collectionName: 'events',
        fullDocument: 'UpdateLookup',
      },
      sourceAccessConfigurations: [
        {
          type: 'BASIC_AUTH',
          uri: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:docdb-creds',
        },
      ],
    });

    const deviceRule = new iot.CfnTopicRule(this, 'DeviceRule', {
      topicRulePayload: {
        sql: "SELECT * FROM 'devices/+/events'",
        actions: [
          {
            lambda: {
              functionArn: ordersHandler.functionArn,
            },
          },
        ],
        ruleDisabled: false,
      },
    });

    new lambda.CfnPermission(this, 'IotInvokePermission', {
      action: 'lambda:InvokeFunction',
      functionName: ordersHandler.functionName,
      principal: 'iot.amazonaws.com',
      sourceArn: deviceRule.attrArn,
    });

    new cognito.UserPool(this, 'Users', {
      userPoolName: 'refactor-showcase-users',
      lambdaTriggers: {
        preSignUp: ordersHandler,
        postConfirmation: ordersHandler,
      },
    });

    const httpApi = new apigwv2.CfnApi(this, 'HttpApiShell', {
      name: 'shell-api',
      protocolType: 'HTTP',
      description: 'Placeholder HTTP API for SAM HttpApi folding',
    });

    new apigwv2.CfnStage(this, 'HttpApiStage', {
      apiId: httpApi.ref,
      stageName: '$default',
      autoDeploy: true,
    });
  }
}
