#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { StepFunctionsStack } from '../lib/step-functions-stack';

const app = new cdk.App();
new StepFunctionsStack(app, 'StepFunctionsStack');