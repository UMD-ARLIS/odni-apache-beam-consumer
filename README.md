## Dependencies
To setup the virtual environment run:

```bash
make venv
make install-deps
```

## Setting up the AWS MSK IAM AUTH
To include the `aws-msk-iam-auth` library in your classpath for the Apache Beam expansion service, you can follow these steps:

1. **Download the Dependency**:
   Download the `aws-msk-iam-auth` JAR file from Maven Central or any Maven repository.

2. **Specify the Classpath**:
   When starting the expansion service, include the path to the expansion service JAR file (beam-sdks-java-io-expansion-service-2.37.0.jar, included in the repo) and the path to the `aws-msk-iam-auth` JAR file using the `-cp` or `-classpath` option. After this list the class name of the main class that the expansion service uses for execution (org.apache.beam.sdk.expansion.service.ExpansionService), followed by the port number that the expansion service should run on. For example:
   ```bash
   java -cp /path/to/aws-msk-iam-auth.jar:/path/to/beam-sdks-java-io-expansion-service-2.37.0.jar org.apache.beam.sdk.expansion.service.ExpansionService 8088 --javaClassLookupAllowlistFile='*'
   ```
   Replace `/path/to/` with the actual path to the downloaded JAR files.

3. **Configure the Java Classpath Allowlist**:
   Since the Apache Beam expansion service uses a Java Classpath Allowlist, you need to add the package containing the `aws-msk-iam-auth` classes to the allowlist. It seems you've already set `--javaClassLookupAllowlistFile='*'`, which allows all classes to be loaded. If you want to restrict the allowlist to specific packages, you can provide a file containing the package names.

4. **Configure Kafka Client**:
   Once the library is in the classpath of the expansion service, you can configure the Kafka client in your Python code to use IAM authentication. Ensure that the required classes and configurations from `aws-msk-iam-auth` are accessible to the expansion service.

By following these steps, you should be able to use the `aws-msk-iam-auth` library for IAM authentication in your Apache Beam pipeline running through the expansion service. Make sure to test thoroughly to ensure that everything works as expected.

## Expansion Service
A shell script is included to start the expansion service. To execute it run:

sudo bash ./start-expansion-svc.sh <port# to run expansion service on>