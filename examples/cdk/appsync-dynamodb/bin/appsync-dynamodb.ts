#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AppsyncDynamodbStack } from '../lib/appsync-dynamodb-stack';

const app = new cdk.App();
new AppsyncDynamodbStack(app, 'AppsyncDynamodbStack', {});
