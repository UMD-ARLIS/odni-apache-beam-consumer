To setup the virtual environment run:

```bash
make venv
make install-deps
```

To start the expansion service run:

java -jar beam-sdks-java-io-expansion-service-2.37.0.jar 8088 --javaClassLookupAllowlistFile='*' 