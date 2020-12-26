package com.myorg;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.SingletonFunction;
import software.amazon.awscdk.services.sam.CfnApplication;
import software.amazon.awscdk.services.sam.CfnApplicationProps;
import software.amazon.awscdk.services.sam.CfnFunction;

import java.util.HashMap;
import java.util.UUID;

import static java.lang.Runtime.*;
import static software.amazon.awscdk.services.lambda.Runtime.JAVA_11;
import static software.amazon.awscdk.services.lambda.Runtime.PYTHON_2_7;

public class CdkDeployStack extends Stack {

    private static final String SPILL_BUCKET = "spill_bucket";
    private static final String SPILL_PREFIX = "spill_prefix";
    private static final String DISABLE_SPILL_ENCRYPTION = "disable_spill_encryption";


    public CdkDeployStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public CdkDeployStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        SingletonFunction lambdaFunction =
                SingletonFunction.Builder.create(this, "cdk-lambda-cron")
                                         .description("Lambda which prints \"I'm running\"")
                                         .code(Code.fromInline("def main(event, context):\n" + "    print(\"I'm running!\")\n"))
                                         .handler("index.main")
                                         .timeout(Duration.seconds(300))
                                         .runtime(JAVA_11)
                                         .uuid(UUID.randomUUID().toString())
                                         .build();

        CfnApplicationProps applicationProps = CfnApplicationProps.builder()
                                                                  .tags(new HashMap<String, String>(){{ }})
                                                                  .parameters(new HashMap<String, String>(){{ }})
                                                                  .build();

        CfnApplication serverlessApplication = CfnApplication.Builder.create(scope, "serverlessApplication")
                                                                     .location(CfnApplication.ApplicationLocationProperty.builder()
                                                                                                                         .applicationId("")
                                                                                                                         .semanticVersion("")
                                                                                                                         .build())
                                                                     .build();

        // todo - paramaterize
        String athenaCatalogName = "";
        String description = "Enables Amazon Athena to communicate with CassandraDB, making your tables accessible via SQL";
        int lambdaTimeout = 600000;
        int lambdaMemory = 1024;
        String handler = "com.amazonaws.athena.connectors.cassandra.CassandraCompositeHandler";
        boolean disableSpillEncryption = false;
        String spillBucket = "";
        String spillPrefix = "";

        CfnFunction.FunctionEnvironmentProperty environmentProperties =
                CfnFunction.FunctionEnvironmentProperty.builder()
                                                       .variables(new HashMap<String, String>(){{
                                                           put(SPILL_BUCKET, spillBucket);
                                                           put(SPILL_PREFIX, spillPrefix);
                                                           put(DISABLE_SPILL_ENCRYPTION, String.valueOf(disableSpillEncryption));
                                                       }})
                                                       .build();

        CfnFunction serverlessFunction = CfnFunction.Builder.create(scope, "serverlessFunction")
                                                            .handler("")
                                                            .environment(environmentProperties)
                                                            .codeUri("./target/athena-cassandra-1.0.jar")
                                                            .description(description)
                                                            .runtime(JAVA_11.getName())
                                                            .functionName(athenaCatalogName)
                                                            .timeout(lambdaTimeout)
                                                            .memorySize(lambdaMemory)
                                                            .policies("")
                                                            .build();
    }
}
