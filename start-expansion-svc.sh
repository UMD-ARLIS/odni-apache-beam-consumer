if [ "$#" -lt 1 ] ; then
    echo "No arguments supplied"
    exit 1
fi

if ! [[ $1 =~ ^[0-9]+$ ]] ; then
   echo "error: $1 is not a valid number"
   exit 1
fi

sudo java -cp aws-msk-iam-auth.jar:beam-sdks-java-io-expansion-service-2.37.0.jar org.apache.beam.sdk.expansion.service.ExpansionService $1 --javaClassLookupAllowlistFile='*'