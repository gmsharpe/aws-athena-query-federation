package com.amazonaws.cdkstack.myorg;

import software.amazon.awscdk.core.App;

public class CdkDeployApp {
    public static void main(final String[] args) {
        App app = new App();

        new CdkDeployStack(app, "CdkDeployStack");

        app.synth();

    }
}
