import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as logs from 'aws-cdk-lib/aws-logs';

export class StepFunctionsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // SNS Topic for notifications
    const orderNotificationTopic = new sns.Topic(this, 'OrderNotificationTopic', {
      displayName: 'Order Processing Notifications',
    });

    // Lambda functions for order processing
    const validateOrderFunction = new lambda.Function(this, 'ValidateOrderFunction', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          return { isValid: true };
        };
      `),
    });

    const processPaymentFunction = new lambda.Function(this, 'ProcessPaymentFunction', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          return { paymentSuccess: true };
        };
      `),
    });

    const shipOrderFunction = new lambda.Function(this, 'ShipOrderFunction', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          return { shipped: true };
        };
      `),
    });

    // Step Functions tasks
    const validateOrder = new tasks.LambdaInvoke(this, 'ValidateOrder', {
      lambdaFunction: validateOrderFunction,
    });

    const processPayment = new tasks.LambdaInvoke(this, 'ProcessPayment', {
      lambdaFunction: processPaymentFunction,
    });

    const shipOrder = new tasks.LambdaInvoke(this, 'ShipOrder', {
      lambdaFunction: shipOrderFunction,
    });

    const notifySuccess = new tasks.SnsPublish(this, 'NotifySuccess', {
      topic: orderNotificationTopic,
      message: sfn.TaskInput.fromText('Order processed successfully'),
    });

    const notifyOrderFailed = new tasks.SnsPublish(this, 'OrderFailed', {
      topic: orderNotificationTopic,
      message: sfn.TaskInput.fromText('Order validation failed'),
    });

    const notifyPaymentFailed = new tasks.SnsPublish(this, 'PaymentFailed', {
      topic: orderNotificationTopic,
      message: sfn.TaskInput.fromText('Payment processing failed'),
    });

    // Choice states
    const orderValidChoice = new sfn.Choice(this, 'OrderValid?')
      .when(
        sfn.Condition.booleanEquals('$.Payload.isValid', true),
        processPayment
      )
      .otherwise(notifyOrderFailed);

    const paymentSuccessChoice = new sfn.Choice(this, 'PaymentSuccessful?')
      .when(
        sfn.Condition.booleanEquals('$.Payload.paymentSuccess', true),
        shipOrder
      )
      .otherwise(notifyPaymentFailed);

    // Build the state machine
    const definition = validateOrder
      .next(orderValidChoice);

    processPayment.next(paymentSuccessChoice);
    shipOrder.next(notifySuccess);

    // Create the state machine
    const logGroup = new logs.LogGroup(this, 'ProcessOrderStateMachineLogGroup', {
      retention: logs.RetentionDays.TWO_YEARS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const stateMachine = new sfn.StateMachine(this, 'ProcessOrderStateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      stateMachineName: 'ProcessOrderStateMachine',
      stateMachineType: sfn.StateMachineType.STANDARD,
      logs: {
        destination: logGroup,
        level: sfn.LogLevel.ALL,
        includeExecutionData: true,
      },
    });

    // Output the state machine ARN
    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: stateMachine.stateMachineArn,
    });
  }
}