package com.amazonaws.cdkstack.myorg;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.SingletonFunction;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.CfnBucket;
import software.amazon.awscdk.services.sam.CfnApplication;
import software.amazon.awscdk.services.sam.CfnApplicationProps;
import software.amazon.awscdk.services.sam.CfnFunction;
import software.amazon.awscdk.services.sam.CfnFunction.SAMPolicyTemplateProperty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static java.lang.Runtime.*;
import static software.amazon.awscdk.services.lambda.Runtime.JAVA_11;
import static software.amazon.awscdk.services.lambda.Runtime.PYTHON_2_7;
import static software.amazon.awscdk.services.s3.CfnBucket.*;
import static software.amazon.awscdk.services.sam.CfnFunction.*;

public class CdkDeployStack extends Stack
{

    private static final String SPILL_BUCKET = "spill_bucket";
    private static final String SPILL_PREFIX = "spill_prefix";
    private static final String DISABLE_SPILL_ENCRYPTION = "disable_spill_encryption";

    public CdkDeployStack(final Construct scope, final String id)
    {
        this(scope, id, null);
    }

    public CdkDeployStack(final Construct scope, final String id, final StackProps props)
    {
        super(scope, id, props);

        // todo - paramaterize
        String athenaCatalogName = "athena-cassandra-connector";
        String description = "Enables Amazon Athena to communicate with CassandraDB, making your tables accessible via SQL";
        int lambdaTimeout = 300;
        int lambdaMemory = 1024;
        String handler = "com.amazonaws.athena.connectors.cassandra.CassandraCompositeHandler";
        boolean disableSpillEncryption = false;
        String spillBucketName = "athena-cassandra-connector";
        String spillPrefix = "spill";

        FunctionEnvironmentProperty environmentProperties =
                FunctionEnvironmentProperty.builder()
                                           .variables(new HashMap<String, String>()
                                           {{
                                               put(SPILL_BUCKET, spillBucketName);
                                               put(SPILL_PREFIX, spillPrefix);
                                               put(DISABLE_SPILL_ENCRYPTION, String.valueOf(disableSpillEncryption));
                                           }})
                                           .build();

        PublicAccessBlockConfigurationProperty blockAllS3AccessPolicyProperty =
                PublicAccessBlockConfigurationProperty.builder()
                                                      .blockPublicAcls(true)
                                                      .blockPublicPolicy(true)
                                                      .ignorePublicAcls(true)
                                                      .restrictPublicBuckets(true)
                                                      .build();

        Bucket spillBucket = Bucket.Builder.create(this, "spillBucket")
                                           .bucketName(spillBucketName)
                                           .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                                           .build();

        // core policies for Glue and S3
        List<String> actions = Arrays.asList("glue:GetTableVersions",
                                             "glue:GetPartitions",
                                             "glue:GetTables",
                                             "glue:GetTableVersion",
                                             "glue:GetDatabases",
                                             "glue:GetTable",
                                             "glue:GetPartition",
                                             "glue:GetDatabase",
                                             "glue:GetQueryExecution",
                                             "s3:ListAllMyBuckets");

        List<String> keyspacesActions = Arrays.asList("cassandra:Select");

        List<PolicyStatement> statements = Arrays.asList(
                PolicyStatement.Builder.create().actions(actions).effect(Effect.ALLOW).resources(Arrays.asList("*")).build(),
                PolicyStatement.Builder.create().actions(keyspacesActions).effect(Effect.ALLOW).resources(Arrays.asList("*")).build()
        );

/*        SAMPolicyTemplateProperty samPolicyTemplateProperty =
                SAMPolicyTemplateProperty.builder()
                                         .s3CrudPolicy(BucketSAMPTProperty.builder()
                                                                          .bucketName(spillBucket.getBucketName())
                                                                          .build())
                                         .build();

        CfnPolicy policy = CfnPolicy.Builder.create(this, "iamPolicy")
                                            .policyName("cassandra-connector-default-policy")
                                            .policyDocument(PolicyDocument.Builder.create()
                                                                                  .statements(statements)
                                                                                  .build())
                                            .policyDocument(samPolicyTemplateProperty.getS3CrudPolicy())
                                            .build();*/

        Function serverlessFunction = Function.Builder.create(this, "serverlessFunction")
                                                         .handler(
                                                                    "com.amazonaws.athena.connectors.cassandra.CassandraCompositeHandler")
                                                         .environment(new HashMap<String, String>()
                                                         {{
                                                             put(SPILL_BUCKET, spillBucketName);
                                                             put(SPILL_PREFIX, spillPrefix);
                                                             put(DISABLE_SPILL_ENCRYPTION, String.valueOf(disableSpillEncryption));
                                                         }})
                                                         .code(Code.fromAsset("/aws-athena-query-federation/athena-cassandra/target/athena-cassandra-1.0.jar"))
                                                         .description(description)
                                                         .runtime(JAVA_11)
                                                         .functionName(athenaCatalogName)
                                                         .timeout(Duration.seconds(lambdaTimeout))
                                                         .initialPolicy(statements)
                                                         .memorySize(lambdaMemory)
                                                         .build();

        spillBucket.grantReadWrite(serverlessFunction);

/*
        CfnApplicationProps applicationProps = CfnApplicationProps.builder()
                                                                  .tags(new HashMap<String, String>(){{ }})
                                                                  .parameters(new HashMap<String, String>(){{ }})
                                                                  .location(CfnApplication.ApplicationLocationProperty.builder().build())
                                                                  .build();

        CfnApplication serverlessApplication = CfnApplication.Builder.create(scope, "serverlessApplication")
                                                                     .location(CfnApplication.ApplicationLocationProperty.builder()
                                                                                                                         .applicationId("")
                                                                                                                         .semanticVersion("")
                                                                                                                         .build())
                                                                     .build();*/

    }
}
