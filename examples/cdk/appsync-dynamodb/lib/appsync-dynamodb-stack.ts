import * as cdk from 'aws-cdk-lib';
import { AppsyncFunction, AuthorizationType, Code, Definition, FunctionRuntime, GraphqlApi, Resolver } from 'aws-cdk-lib/aws-appsync';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import * as path from 'path';

export class AppsyncDynamodbStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create DynamoDB table for tasks
    const tasksTable = new Table(this, 'TasksTable', {
      partitionKey: { name: 'id', type: AttributeType.STRING },
      tableName: 'tasks',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      billingMode: BillingMode.PAY_PER_REQUEST,
    });

    // Create AppSync GraphQL API
    const api = new GraphqlApi(this, 'TaskApi', {
      name: 'task-api',
      definition: Definition.fromFile(path.join(__dirname, '../graphql/schema.graphql')),
      authorizationConfig: {
        defaultAuthorization: {
          authorizationType: AuthorizationType.API_KEY,
        },
      },
      xrayEnabled: true,
    });

    // Connect DynamoDB table to the AppSync API as a data source
    const tasksDataSource = api.addDynamoDbDataSource('TasksDataSource', tasksTable);

    // Create AppSync function for getting a task
    const getTaskFunction = new AppsyncFunction(this, 'GetTaskFunction', {
      name: 'getTask',
      api,
      dataSource: tasksDataSource,
      code: Code.fromAsset(path.join(__dirname, '../resolvers/getTask.js')),
      runtime: FunctionRuntime.JS_1_0_0,
    });

    // Create AppSync function for listing tasks
    const listTasksFunction = new AppsyncFunction(this, 'ListTasksFunction', {
      name: 'listTasks',
      api,
      dataSource: tasksDataSource,
      code: Code.fromAsset(path.join(__dirname, '../resolvers/listTasks.js')),
      runtime: FunctionRuntime.JS_1_0_0,
    });

    // Create resolver for Query.getTask
    new Resolver(this, 'GetTaskResolver', {
      api,
      typeName: 'Query',
      fieldName: 'getTask',
      runtime: FunctionRuntime.JS_1_0_0,
      code: Code.fromAsset(path.join(__dirname, '../resolvers/pipeline.js')),
      pipelineConfig: [getTaskFunction],
    });

    // Create resolver for Query.listTasks
    new Resolver(this, 'ListTasksResolver', {
      api,
      typeName: 'Query',
      fieldName: 'listTasks',
      runtime: FunctionRuntime.JS_1_0_0,
      code: Code.fromAsset(path.join(__dirname, '../resolvers/pipeline.js')),
      pipelineConfig: [listTasksFunction],
    });

    // Output the API URL and key
    new cdk.CfnOutput(this, 'GraphQLAPIURL', {
      value: api.graphqlUrl,
    });

    new cdk.CfnOutput(this, 'GraphQLAPIKey', {
      value: api.apiKey || '',
    });
  }
}
